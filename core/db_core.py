import gc
import os
import struct
import zlib
from collections import defaultdict
from contextlib import closing

import lmdb
import msgpack
import numpy as np
from pyroaring import BitMap
from tqdm import tqdm
from lz4 import frame
import config as cf
from core import io_worker as iw


def is_byte_obj(obj):
    if isinstance(obj, bytes) or isinstance(obj, bytearray):
        return True
    return False


def set_default(obj):
    if isinstance(obj, set):
        return sorted(list(obj))
    raise TypeError


def deserialize_key(key, integerkey=False, is_64bit=False):
    if not integerkey:
        return key.decode(cf.ENCODING)
    try:
        if is_64bit:
            return struct.unpack("Q", key)[0]
        else:
            return struct.unpack("I", key)[0]
    except Exception:
        iw.print_status(key)
        raise Exception


def deserialize_value(value, bytes_value=cf.ToBytesType.OBJ, compress_value=False):
    if bytes_value == cf.ToBytesType.INT_NUMPY:
        value = np.frombuffer(value, dtype=np.uint32).tolist()
    elif bytes_value == cf.ToBytesType.INT_BITMAP:
        if not isinstance(value, bytes):
            value = bytes(value)
        value = BitMap.deserialize(value)
    else:  # mode == "msgpack"
        if compress_value:
            try:
                value = frame.decompress(value)
            except RuntimeError:
                pass
        value = msgpack.unpackb(value, strict_map_key=False)
    return value


def deserialize(
    key,
    value,
    integerkey=False,
    is_64bit=False,
    bytes_value=cf.ToBytesType.OBJ,
    compress_value=False,
):
    key = deserialize_key(key, integerkey, is_64bit)
    value = deserialize_value(value, bytes_value, compress_value)
    res_obj = (key, value)
    return res_obj


def serialize_key(key, integerkey=False, is_64bit=False):
    if not integerkey:
        if not isinstance(key, str):
            key = str(key)
        return key.encode(cf.ENCODING)[: cf.LMDB_MAX_KEY]
    if is_64bit:
        return struct.pack("Q", key)
    else:
        return struct.pack("I", key)


def serialize_value(
    value, bytes_value=cf.ToBytesType.OBJ, compress_value=False, sort_values=True
):
    if bytes_value == cf.ToBytesType.INT_NUMPY:
        if sort_values:
            value = sorted(list(value))
        if not isinstance(value, np.ndarray):
            value = np.array(value, dtype=np.uint32)
        value = value.tobytes()
    elif bytes_value == cf.ToBytesType.INT_BITMAP:
        value = BitMap(value).serialize()
    else:  # mode == "msgpack"
        value = msgpack.packb(value, default=set_default)
        if compress_value:
            value = frame.compress(value)
    return value


def serialize(
    key,
    value,
    integerkey=False,
    is_64bit=False,
    bytes_value=cf.ToBytesType.OBJ,
    compress_value=False,
):
    key = serialize_key(key, integerkey, is_64bit)
    value = serialize_value(value, bytes_value, compress_value)
    res_obj = (key, value)
    return res_obj


def preprocess_data_before_dump(
    data,
    integerkey=False,
    bytes_value=cf.ToBytesType.OBJ,
    compress_value=False,
    sort_key=True,
):
    if isinstance(data, dict):
        data = list(data.items())

    if sort_key and integerkey:
        data.sort(key=lambda x: x[0])

    first_key = data[0][0]
    first_value = data[0][0]

    if not is_byte_obj(first_key) and not is_byte_obj(first_value):
        data = [
            serialize(
                k,
                v,
                integerkey=integerkey,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )
            for k, v in data
            if k is not None
        ]

    if sort_key and not integerkey:
        data.sort(key=lambda x: x[0])

    return data


class DBCore:
    def __init__(self, db_file, max_db, map_size=cf.LMDB_MAP_SIZE):
        self._db_file = db_file
        iw.create_dir(self._db_file)
        self._max_db = max_db
        self._env = lmdb.open(
            self._db_file,
            map_async=True,
            map_size=map_size,
            subdir=False,
            lock=False,
            max_dbs=max_db,
        )
        self._env.set_mapsize(map_size)

    @property
    def env(self):
        return self._env

    def get_map_size(self):
        tmp = self._env.info().get("map_size")
        if not tmp:
            return "Unknown"
        return f"{tmp / cf.SIZE_1GB:.0f}GB"

    def close(self):
        self._env.close()

    def copy_lmdb(self):
        """
        Copy current env to new one (reduce file size)
        :return:
        :rtype:
        """
        iw.print_status(self._env.stat())
        if self._env.stat().get("map_size"):
            iw.print_status("%.2fGB" % (self._env.stat()["map_size"] % cf.SIZE_1GB))
        new_dir = self._db_file + ".copy"
        self._env.copy(path=new_dir, compact=True)
        try:
            if os.path.exists(self._db_file):
                os.remove(self._db_file)
        except Exception as message:
            iw.print_status(message)
        os.rename(new_dir, self._db_file)

    def get_iter_integerkey(
        self,
        db,
        from_i=0,
        to_i=-1,
        get_values=True,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=False,
    ):
        with self._env.begin(db=db, write=False) as txn:
            if to_i == -1:
                to_i = self.get_db_size(db)
            cur = txn.cursor()
            cur.set_range(serialize_key(from_i, integerkey=True))
            for item in cur.iternext(values=get_values):
                if get_values:
                    key, value = item
                else:
                    key = item
                key = deserialize_key(key, integerkey=True)
                if key > to_i:
                    break
                if get_values:
                    value = deserialize_value(
                        value, bytes_value=bytes_value, compress_value=compress_value,
                    )
                    yield key, value
                else:
                    yield key
            cur.next()

    def get_iter_with_prefix(
        self,
        db,
        prefix,
        integerkey=False,
        get_values=True,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=False,
    ):
        with self._env.begin(db=db, write=False) as txn:
            cur = txn.cursor()
            prefix = serialize_key(prefix, integerkey=integerkey)
            cur.set_range(prefix)

            while cur.key().startswith(prefix) is True:
                try:
                    if cur.key() and not cur.key().startswith(prefix):
                        continue
                    key = deserialize_key(cur.key(), integerkey=integerkey)
                    if get_values:
                        value = deserialize_value(
                            cur.value(),
                            bytes_value=bytes_value,
                            compress_value=compress_value,
                        )
                        yield key, value
                    else:
                        yield key
                except Exception as message:
                    iw.print_status(message)
                cur.next()

    def is_available(self, db, key_obj, integerkey=False):
        with self._env.begin(db=db) as txn:
            key_obj = serialize_key(key_obj, integerkey=integerkey)
            if key_obj:
                try:
                    value_obj = txn.get(key_obj)
                    if value_obj:
                        return True
                except Exception as message:
                    iw.print_status(message)
        return False

    def get_memory_size(self, db, key_obj, integerkey=False, is_64bit=False):
        with self._env.begin(db=db, buffers=True) as txn:
            key_obj = serialize_key(key_obj, integerkey=integerkey, is_64bit=is_64bit)
            responds = None
            if key_obj:
                try:
                    value_obj = txn.get(key_obj)
                    if value_obj:
                        return len(value_obj)
                except Exception as message:
                    iw.print_status(message)

            return responds

    def get_value(
        self,
        db,
        key_obj,
        integerkey=False,
        is_64bit=False,
        get_deserialize=True,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=False,
    ):
        with self._env.begin(db=db, buffers=True) as txn:
            if isinstance(key_obj, np.ndarray):
                key_obj = key_obj.tolist()

            if (
                isinstance(key_obj, list)
                or isinstance(key_obj, set)
                or isinstance(key_obj, tuple)
            ):
                key_obj = [serialize_key(k, integerkey=integerkey) for k in key_obj]
                responds = dict()
                for k, v in txn.cursor(db).getmulti(key_obj):
                    if v:
                        if get_deserialize:
                            try:
                                k, v = deserialize(
                                    k,
                                    v,
                                    integerkey=integerkey,
                                    is_64bit=is_64bit,
                                    bytes_value=bytes_value,
                                    compress_value=compress_value,
                                )
                                responds[k] = v
                            except Exception as message:
                                iw.print_status(message)
                        else:
                            k = deserialize_key(
                                k, integerkey=integerkey, is_64bit=is_64bit
                            )
                            responds[k] = v
            else:
                key_obj = serialize_key(
                    key_obj, integerkey=integerkey, is_64bit=is_64bit
                )
                responds = None
                if key_obj:
                    try:
                        value_obj = txn.get(key_obj)
                        if value_obj:
                            if get_deserialize:
                                responds = deserialize_value(
                                    value_obj,
                                    bytes_value=bytes_value,
                                    compress_value=compress_value,
                                )
                            else:
                                responds = value_obj
                    except Exception as message:
                        iw.print_status(message)

        return responds

    def head(
        self,
        db,
        n,
        bytes_value=cf.ToBytesType.OBJ,
        from_i=0,
        integerkey=False,
        compress_value=False,
    ):
        respond = defaultdict()
        for i, (k, v) in enumerate(
            self.get_db_iter(
                db,
                bytes_value=bytes_value,
                from_i=from_i,
                integerkey=integerkey,
                compress_value=compress_value,
            )
        ):
            respond[k] = v
            if i == n - 1:
                break
        return respond

    def get_db_iter(
        self,
        db,
        get_values=True,
        deserialize_obj=True,
        from_i=0,
        to_i=-1,
        integerkey=False,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=False,
    ):
        if to_i == -1:
            to_i = self.get_db_size(db)

        with self._env.begin(db=db) as txn:
            cur = txn.cursor()
            for i, db_obj in enumerate(cur.iternext(values=get_values)):
                if i < from_i:
                    continue
                if i >= to_i:
                    break

                if get_values:
                    key, value = db_obj
                else:
                    key = db_obj
                try:
                    if deserialize_obj:
                        key = deserialize_key(key, integerkey=integerkey)
                        if get_values:
                            value = deserialize_value(
                                value,
                                bytes_value=bytes_value,
                                compress_value=compress_value,
                            )
                    if get_values:
                        return_obj = (key, value)
                        yield return_obj
                    else:
                        yield key
                # Todo: handlers
                except UnicodeDecodeError:
                    iw.print_status(f"UnicodeDecodeError: {i}")
                except Exception:
                    iw.print_status(i)
                    raise Exception

    def get_db_size(self, db):
        with self._env.begin(db=db) as txn:
            return txn.stat()["entries"]

    def delete(self, db, key, integerkey=False, with_prefix=False):
        if not (
            isinstance(key, list) or isinstance(key, set) or isinstance(key, tuple)
        ):
            key = [key]

        if with_prefix:
            true_key = set()
            for k in key:
                for tmp_k in self.get_iter_with_prefix(
                    db, k, integerkey=integerkey, get_values=False
                ):
                    true_key.add(tmp_k)
            if true_key:
                key = list(true_key)

        deleted_items = 0
        with self.env.begin(db=db, write=True, buffers=True) as txn:
            for k in key:
                try:
                    status = txn.delete(serialize_key(k, integerkey))
                    if status:
                        deleted_items += 1
                except Exception as message:
                    iw.print_status(message)
        return deleted_items

    @staticmethod
    def write_bulk(
        env,
        db,
        data,
        sort_key=True,
        integerkey=False,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=False,
        one_sample_write=False,
    ):
        data = preprocess_data_before_dump(
            data,
            bytes_value=bytes_value,
            integerkey=integerkey,
            compress_value=compress_value,
            sort_key=sort_key,
        )
        added_items = 0
        try:
            with env.begin(db=db, write=True, buffers=True) as txn:
                if not one_sample_write:
                    _, added_items = txn.cursor().putmulti(data)
                else:
                    for k, v in data:
                        txn.put(k, v)
                        added_items += 1
        except lmdb.MapFullError:
            curr_limit = env.info()["map_size"]
            new_limit = curr_limit + cf.SIZE_1GB * 5
            env.set_mapsize(new_limit)
            return DBCore.write_bulk(env, db, data, sort_key=False)
        except lmdb.BadValsizeError:
            iw.print_status(lmdb.BadValsizeError)
        except lmdb.BadTxnError:
            if one_sample_write:
                return DBCore.write_bulk(
                    env, db, data, sort_key=False, one_sample_write=True,
                )
        except Exception:
            raise Exception
        return added_items

    @staticmethod
    def write_bulk_with_buffer(
        env,
        db,
        data,
        sort_key=True,
        integerkey=False,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=False,
        show_progress=True,
        step=10000,
        message="DB Write",
    ):
        data = preprocess_data_before_dump(
            data,
            bytes_value=bytes_value,
            integerkey=integerkey,
            compress_value=compress_value,
            sort_key=sort_key,
        )

        def update_desc():
            return f"{message} buffer: {buff_size / cf.LMDB_BUFF_BYTES_SIZE * 100:.0f}%"

        p_bar = None
        buff_size = 0
        i_pre = 0
        if show_progress:
            p_bar = tqdm(total=len(data))

        for i, (k, v) in enumerate(data):
            if show_progress and i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())
            buff_size += len(k) + len(v)

            if buff_size >= cf.LMDB_BUFF_BYTES_SIZE:
                c = DBCore.write_bulk(env, db, data[i_pre:i], sort_key=False)
                if c != len(data[i_pre:i]):
                    iw.print_status(
                        f"WriteError: Missing data. Expected: {len(data[i_pre:i])} - Actual: {c}"
                    )
                i_pre = i
                buff_size = 0

        if buff_size:
            DBCore.write_bulk(env, db, data[i_pre:], sort_key=False)

        if show_progress:
            p_bar.update(len(data) % step)
            p_bar.set_description(desc=update_desc())
            p_bar.close()

    def update_bulk_with_buffer(
        self,
        env,
        db,
        data,
        update_type=cf.DBUpdateType.SET,
        integerkey=False,
        bytes_value=cf.ToBytesType.INT_NUMPY,
        compress_value=False,
        show_progress=True,
        step=10000,
        message="",
        buff_limit=cf.LMDB_BUFF_BYTES_SIZE,
    ):
        buff = []
        p_bar = None
        c_skip, c_update, c_new, c_buff = 0, 0, 0, 0

        def update_desc():
            return (
                f"{message}"
                f"|Skip:{c_skip:,}"
                f"|New:{c_new:,}"
                f"|Update:{c_update:,}"
                f"|Buff:{c_buff / buff_limit * 100:.0f}%"
            )

        if show_progress:
            p_bar = tqdm(total=len(data), desc=update_desc())

        for i, (k, v) in enumerate(data.items()):
            if show_progress and i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(update_desc())

            db_obj = self.get_value(
                db,
                k,
                integerkey=integerkey,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )
            if update_type == cf.DBUpdateType.SET:
                if db_obj:
                    db_obj = set(db_obj)
                    v = set(v)
                    if db_obj and len(v) <= len(db_obj) and db_obj.issuperset(v):
                        c_skip += 1
                        continue
                    if db_obj:
                        v.update(db_obj)
                        c_update += 1
                    else:
                        c_new += 1
                else:
                    c_new += 1
            else:
                if db_obj:
                    v += db_obj
                    c_update += 1
                else:
                    c_new += 1

            k, v = serialize(
                k,
                v,
                integerkey=integerkey,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )

            c_buff += len(k) + len(v)
            buff.append((k, v))

            if c_buff >= buff_limit:
                DBCore.write_bulk(env, db, buff)
                buff = []
                c_buff = 0

        if buff:
            DBCore.write_bulk(env, db, buff)
        if show_progress:
            p_bar.set_description(desc=update_desc())
            p_bar.close()

    def modify_db_compress_value(
        self,
        c_db,
        c_integerkey=False,
        c_bytes_value=cf.ToBytesType.OBJ,
        c_compress_value=False,
        n_integerkey=False,
        n_bytes_value=cf.ToBytesType.OBJ,
        n_compress_value=False,
        step=1000,
    ):
        buff = []
        buff_size = 0

        def update_desc():
            return f"buff:{buff_size / cf.LMDB_BUFF_BYTES_SIZE * 100:.0f}%"

        p_bar = tqdm(total=self.get_db_size(c_db))
        for i, (k, v) in enumerate(
            self.get_db_iter(
                c_db,
                integerkey=c_integerkey,
                bytes_value=c_bytes_value,
                compress_value=c_compress_value,
            )
        ):
            k, v = serialize(
                k,
                v,
                integerkey=n_integerkey,
                bytes_value=n_bytes_value,
                compress_value=n_compress_value,
            )
            buff_size += len(k) + len(v)
            buff.append((k, v))
            if buff_size >= cf.LMDB_BUFF_BYTES_SIZE:
                self.write_bulk(self.env, c_db, buff)
                buff = []
                buff_size = 0
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())
        if buff:
            self.write_bulk(self.env, c_db, buff)

    def drop_db(self, db):
        with self._env.begin(write=True) as in_txn:
            in_txn.drop(db)
            print(in_txn.stat())

    def copy_new_file(
        self, db_names, map_size, buff_size=cf.SIZE_512MB, compress=True, message=False,
    ):
        new_dir = self._db_file + ".copy"
        print(self._env.info())
        iw.print_status("%.2fGB" % (self._env.info()["map_size"] / cf.SIZE_1GB))
        save_drive = 0
        with closing(
            lmdb.open(
                new_dir,
                subdir=False,
                map_async=True,
                lock=False,
                map_size=map_size,
                max_dbs=len(db_names),
            )
        ) as env:
            print(env.info())
            for db_name_src, copy_args in db_names.items():
                db_name_tar = copy_args["name"]

                org_db = self._env.open_db(db_name_src)
                is_integerkey = False
                if copy_args.get("integerkey"):
                    is_integerkey = copy_args["integerkey"]
                tar_db = env.open_db(db_name_tar, integerkey=is_integerkey)

                org_db_n = self.get_db_size(org_db)

                iw.print_status(
                    f"\nCopy: {self._db_file} - {str(db_name_src)} --> {str(db_name_tar)}"
                )

                def update_desc():
                    if compress:
                        return f"Save: {save_drive / cf.SIZE_1GB:.2f}GB|buff:{len_buff/cf.SIZE_1MB}MB"
                    else:
                        return f"buff:{len_buff/cf.SIZE_1MB:.2f}MB"

                with self._env.begin(db=org_db) as txn:
                    cur = txn.cursor()
                    buff = []
                    len_buff = 0
                    if message:
                        p_bar = tqdm(desc=update_desc(), total=org_db_n)
                    for i, (key, value) in enumerate(iter(cur)):
                        if message:
                            p_bar.update()
                        if message and i and i % 100000 == 0:
                            p_bar.set_description(desc=update_desc())
                        if compress:
                            old_size = len(value)
                            value = zlib.compress(value)
                            save_drive += old_size - len(value)
                        buff.append((key, value))

                        len_buff += len(value) + len(key)

                        if len_buff > buff_size:
                            if message:
                                p_bar.set_description(desc=update_desc())
                            DBCore.write_bulk(env, tar_db, buff)
                            buff.clear()
                            len_buff = 0
                            gc.collect()
                    if buff:
                        if message:
                            p_bar.set_description(desc=update_desc())
                        DBCore.write_bulk(env, tar_db, buff)
                        buff.clear()
                        gc.collect()
                    if message:
                        p_bar.close()
            iw.print_status(env.info())
            iw.print_status("%.2fGB" % (env.info()["map_size"] / cf.SIZE_1GB))
