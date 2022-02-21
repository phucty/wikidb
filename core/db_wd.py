import bz2
import gc
import gzip
import os.path
import queue
import string
import sys
from collections import defaultdict
from contextlib import closing
from multiprocessing.pool import Pool

import ujson
from tqdm import tqdm
import marisa_trie
import core.io_worker as iw
import config as cf
from core.db_core import DBCore, serialize, serialize_value, serialize_key
import csv


def parse_sql_values(line):
    values = line[line.find("` VALUES ") + 9 :]
    latest_row = []
    reader = csv.reader(
        [values],
        delimiter=",",
        doublequote=False,
        escapechar="\\",
        quotechar="'",
        strict=True,
    )
    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == "NULL":
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    yield latest_row
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            yield latest_row


def convert_num(text):
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def get_wd_int(wd_id):
    result = None
    if wd_id and len(wd_id) and wd_id[0].lower() in ["p", "q"] and " " not in wd_id:
        result = convert_num(wd_id[1:])
    return result


def is_wd_item(wd_id):
    if get_wd_int(wd_id) is None:
        return False
    else:
        return True


class DumpReaderWikidata(object):
    def __init__(self, dir_dump):
        self.dir_dump = dir_dump

    def __iter__(self):
        if ".bz2" in self.dir_dump:
            reader = bz2.BZ2File(self.dir_dump)
        elif ".gz" in self.dir_dump:
            reader = gzip.open(self.dir_dump, "rt")
        else:
            reader = open(self.dir_dump)

        if reader:
            for line in reader:
                yield line
            reader.close()


class DBWikidata(DBCore):
    def __init__(self, db_file=cf.DIR_WIKIDATA_ITEMS_JSON):
        super().__init__(db_file=db_file, max_db=11, map_size=cf.SIZE_1GB * 100)
        self.db_redirect = self._env.open_db(b"db_redirect", integerkey=True)
        self.db_redirect_of = self._env.open_db(b"db_redirect_of", integerkey=True)
        self.db_label = self._env.open_db(b"db_label", integerkey=True)
        self.db_labels = self._env.open_db(b"db_labels", integerkey=True)
        self.db_descriptions = self._env.open_db(b"db_descriptions", integerkey=True)
        self.db_aliases = self._env.open_db(b"db_aliases", integerkey=True)
        self.db_claims = self._env.open_db(b"db_claims", integerkey=True)
        self.db_sitelinks = self._env.open_db(b"db_sitelinks", integerkey=True)
        self.db_claim_ent_inv = self._env.open_db(b"db_claim_ent_inv")
        if os.path.exists(cf.DIR_WIKIDATA_ITEMS_TRIE):
            self.db_qid_trie = marisa_trie.Trie()
            self.db_qid_trie.load(cf.DIR_WIKIDATA_ITEMS_TRIE)
        else:
            # Build wiki database
            # Will take 1-2 days
            self.db_qid_trie = None
            self.build()

    def get_redirect_of(self, wd_id):
        return self._get_db_item(
            self.db_redirect_of,
            wd_id,
            bytes_value=cf.ToBytesType.INT_NUMPY,
            integerkey=True,
            decode=True,
        )

    def get_redirect(self, wd_id):
        return self._get_db_item(
            self.db_redirect, wd_id, compress_value=False, integerkey=True, decode=True
        )

    def keys(self):
        for k in self.db_qid_trie:
            yield k

    def items(self):
        for k in self.keys():
            v = self.get_item(k)
            yield k, v

    def size(self):
        return self.get_db_size(self.db_label)

    def _get_db_item(
        self,
        db,
        wd_id,
        compress_value=False,
        integerkey=True,
        decode=True,
        bytes_value=cf.ToBytesType.OBJ,
    ):
        if integerkey and not isinstance(wd_id, int):
            wd_id = self.get_lid(wd_id)
            if wd_id is None:
                return None
        results = self.get_value(
            db,
            wd_id,
            integerkey=integerkey,
            compress_value=compress_value,
            bytes_value=bytes_value,
        )
        if not decode or not results:
            return results

        if decode and isinstance(results, int):
            return self.db_qid_trie.restore_key(results)
        if decode and type(results) in [list]:
            return [self.db_qid_trie.restore_key(r) for r in results]

        def decode_ref_nodes(c_refs):
            decode_ref_nodes = []
            for c_ref_nodes in c_refs:
                decode_ref_node = {}
                for ref_type, ref_values in c_ref_nodes.items():
                    decode_ref_type = {}
                    for (ref_prop, ref_value_objs) in ref_values.items():
                        decode_ref_prop = self.get_qid(ref_prop)
                        decode_ref_values = []
                        for ref_value_obj in ref_value_objs:
                            if ref_type == "wikibase-entityid":
                                ref_value_obj = self.get_qid(ref_value_obj)
                            decode_ref_values.append(ref_value_obj)
                        decode_ref_type[decode_ref_prop] = decode_ref_values
                    decode_ref_node[ref_type] = decode_ref_type
                decode_ref_nodes.append(decode_ref_node)
            return decode_ref_nodes

        decode_results = {}
        for c_type, c_statements in results.items():
            decode_c_type = {}
            for c_prop, c_values in c_statements.items():
                decode_c_prop = self.get_qid(c_prop)
                decode_c_values = []
                for c_value in c_values:
                    decode_c_value = c_value["value"]
                    if c_type == "wikibase-entityid":
                        decode_c_value = self.get_qid(decode_c_value)
                    elif c_type == "quantity":
                        if decode_c_value[1] == -1:
                            decode_c_value = (decode_c_value[0], 1)
                        else:
                            decode_c_value = (
                                decode_c_value[0],
                                self.get_qid(decode_c_value[1]),
                            )
                    c_refs = c_value.get("references")
                    if c_refs:
                        c_refs = decode_ref_nodes(c_refs)
                        decode_c_values.append(
                            {"value": decode_c_value, "references": c_refs,}
                        )
                    else:
                        decode_c_values.append({"value": decode_c_value})
                decode_c_type[decode_c_prop] = decode_c_values

            decode_results[c_type] = decode_c_type
        return decode_results

    def get_label(self, wd_id):
        return self._get_db_item(
            self.db_label, wd_id, compress_value=False, integerkey=True, decode=False
        )

    def get_labels(self, wd_id):
        return self._get_db_item(
            self.db_labels, wd_id, compress_value=True, integerkey=True, decode=False
        )

    def get_descriptions(self, wd_id):
        return self._get_db_item(
            self.db_descriptions,
            wd_id,
            compress_value=True,
            integerkey=True,
            decode=False,
        )

    def get_aliases(self, wd_id):
        return self._get_db_item(
            self.db_aliases, wd_id, compress_value=True, integerkey=True, decode=False,
        )

    def get_sitelinks(self, wd_id):
        return self._get_db_item(
            self.db_sitelinks,
            wd_id,
            compress_value=True,
            integerkey=True,
            decode=False,
        )

    def get_claims(self, wd_id):
        return self._get_db_item(
            self.db_claims, wd_id, compress_value=True, integerkey=True, decode=True
        )

    def get_item(self, wd_id):
        if not isinstance(wd_id, int):
            wd_id = self.get_lid(wd_id)
            if wd_id is None:
                return None
        result = dict()

        def update_dict(attr, func):
            tmp = func(wd_id)
            if tmp is not None:
                result[attr] = tmp

        update_dict("label", self.get_label)
        update_dict("labels", self.get_labels)
        update_dict("descriptions", self.get_descriptions)
        update_dict("aliases", self.get_aliases)
        update_dict("sitelinks", self.get_sitelinks)
        update_dict("claims", self.get_claims)
        return result

    def _get_ptype_pid(self, ptype, pid, wd_id):
        claims = self.get_claims(wd_id)
        if claims and claims.get(ptype) and claims[ptype].get(pid):
            obj_qid = claims[ptype][pid]
            results = [i["value"] for i in obj_qid]
            return results
        return None

    def get_instance_of(self, wd_id):
        return self._get_ptype_pid(ptype="wikibase-entityid", pid="P31", wd_id=wd_id)

    def get_subclass_of(self, wd_id):
        return self._get_ptype_pid(ptype="wikibase-entityid", pid="P279", wd_id=wd_id)

    def get_all_types(self, wd_id):
        # wdt:P31/wdt:P279*
        results = set()
        p_items = self.get_instance_of(wd_id)
        if p_items:
            process_queue = queue.Queue()
            for p_item in p_items:
                process_queue.put(p_item)
            while process_queue.qsize():
                process_wd = process_queue.get()
                results.add(process_wd)
                p_items = self.get_subclass_of(process_wd)
                if p_items:
                    for item in p_items:
                        if item not in results:
                            process_queue.put(item)
        return list(results)

    def get_lid(self, wd_id, default=None):
        results = self.db_qid_trie.get(wd_id)
        if results is None:
            return default
        return results

    def get_qid(self, lid):
        if isinstance(lid, int):
            results = self.db_qid_trie.restore_key(lid)
            if results is not None:
                return results
        return lid

    def build(self):
        # 1. Build trie and redirect\
        self.build_trie_and_redirects()

        # 2. Build json dump
        self.build_from_json_dump(n_process=6)

    def build_trie_and_redirects(self, step=100000):
        if not os.path.exists(cf.DIR_DUMP_WIKIDATA_PAGE):
            raise Exception(f"Please download file {cf.DIR_DUMP_WIKIDATA_PAGE}")
        if not os.path.exists(cf.DIR_DUMP_WIKIDATA_REDIRECT):
            raise Exception(f"Please download file {cf.DIR_DUMP_WIKIDATA_REDIRECT}")

        wd_id_qid = {}
        with gzip.open(
            cf.DIR_DUMP_WIKIDATA_PAGE, "rt", encoding="utf-8", newline="\n"
        ) as f:
            p_bar = tqdm(desc="Wikidata pages")
            i = 0
            for line in f:
                if not line.startswith("INSERT INTO"):
                    continue
                for v in parse_sql_values(line):
                    if is_wd_item(v[2]):
                        i += 1
                        if i and i % step == 0:
                            p_bar.update(step)
                        wd_id_qid[v[0]] = v[2]
                # if len(wd_id_qid) > 1000000:
                #     break
            p_bar.close()

        self.db_qid_trie = marisa_trie.Trie(wd_id_qid.values())
        self.db_qid_trie.save(cf.DIR_WIKIDATA_ITEMS_TRIE)
        iw.print_status(f"Trie Saved: {len(self.db_qid_trie):,}")

        buff_obj = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIDATA_REDIRECT, "rt", encoding="utf-8", newline="\n",
        ) as f:
            i = 0
            p_bar = tqdm(desc="Wikidata redirects")
            for line in f:
                if not line.startswith("INSERT INTO"):
                    continue
                for v in parse_sql_values(line):
                    qid = wd_id_qid.get(v[0])
                    if not qid:
                        continue
                    lid = self.get_lid(qid)
                    if lid is None:
                        continue
                    if is_wd_item(v[2]):
                        redirect = self.get_lid(v[2])
                        if redirect is None:
                            continue
                        buff_obj[lid] = redirect
                        i += 1
                        if i and i % step == 0:
                            p_bar.update(step)
            p_bar.close()

        if buff_obj:
            self.write_bulk(self._env, self.db_redirect, buff_obj, integerkey=True)
            buff_obj_inv = defaultdict(set)
            for k, v in buff_obj.items():
                buff_obj_inv[v].add(k)
            self.write_bulk(
                self._env,
                self.db_redirect_of,
                buff_obj_inv,
                integerkey=True,
                bytes_value=cf.ToBytesType.INT_NUMPY,
            )

    def build_from_json_dump(self, json_dump=cf.DIR_DUMP_WD, n_process=1, step=1000):
        iter_items = DumpReaderWikidata(json_dump)
        attr_db = {
            "label": self.db_label,
            "labels": self.db_labels,
            "descriptions": self.db_descriptions,
            "aliases": self.db_aliases,
            "claims": self.db_claims,
            "sitelinks": self.db_sitelinks,
        }

        buff = {attr: [] for attr in attr_db.keys()}
        buff_size = 0
        count = 0

        def update_desc():
            return f"Wikidata Parsing|items:{count:,}|{buff_size / cf.LMDB_BUFF_BYTES_SIZE * 100:.0f}%"

        def save_buff(buff):
            for attr in buff.keys():
                if not buff[attr]:
                    continue
                buff[attr].sort(key=lambda x: x[0])
                buff[attr] = [
                    (serialize_key(k, integerkey=True), v) for k, v in buff[attr]
                ]
                self.write_bulk(self.env, attr_db[attr], buff[attr], sort_key=False)
                buff[attr] = []
                gc.collect()
            buff = {attr: [] for attr in attr_db.keys()}
            return buff

        def encode_ref_nodes(c_refs):
            encode_ref_nodes = []
            for c_ref_nodes in c_refs:
                encode_ref_node = {}
                for ref_type, ref_values in c_ref_nodes.items():
                    encode_ref_type = {}
                    for (ref_prop, ref_value_objs) in ref_values.items():
                        encode_ref_prop = self.get_lid(ref_prop, ref_prop)
                        encode_ref_values = []
                        for ref_value_obj in ref_value_objs:
                            if ref_type == "wikibase-entityid":
                                ref_value_obj = self.get_lid(
                                    ref_value_obj, ref_value_obj
                                )
                            encode_ref_values.append(ref_value_obj)
                        encode_ref_type[encode_ref_prop] = encode_ref_values
                    encode_ref_node[ref_type] = encode_ref_type
                encode_ref_nodes.append(encode_ref_node)
            return encode_ref_nodes

        p_bar = tqdm(desc=update_desc(), total=90000000)
        # if n_process == 1:
        #     for i, iter_item in enumerate(iter_items):
        #         wd_respond = parse_json_dump(iter_item)
        with closing(Pool(n_process)) as pool:
            for i, wd_respond in enumerate(
                pool.imap_unordered(parse_json_dump, iter_items)
            ):
                # if count > 10000:
                #     break
                if i and i % step == 0:
                    p_bar.set_description(desc=update_desc())
                    p_bar.update(step)
                if not wd_respond:
                    continue
                wd_id, wd_obj = wd_respond
                lid = self.get_lid(wd_id)
                if lid is None:
                    continue

                if wd_obj.get("claims") and wd_obj["claims"].get("wikibase-entityid"):
                    if wd_obj["claims"]["wikibase-entityid"].get("P31"):
                        instance_ofs = {
                            i["value"]
                            for i in wd_obj["claims"]["wikibase-entityid"]["P31"]
                        }
                        if cf.WIKIDATA_IDENTIFIERS.intersection(instance_ofs):
                            continue
                    if wd_obj["claims"]["wikibase-entityid"].get("P279"):
                        subclass_ofs = {
                            i["value"]
                            for i in wd_obj["claims"]["wikibase-entityid"]["P279"]
                        }
                        if cf.WIKIDATA_IDENTIFIERS.intersection(subclass_ofs):
                            continue
                count += 1

                for attr, value in wd_obj.items():
                    if not value:
                        continue

                    if attr == "claims":
                        encode_attr = {}
                        for c_type, c_statements in value.items():
                            encode_c_type = {}
                            for c_prop, c_values in c_statements.items():
                                encode_c_prop = self.get_lid(c_prop, c_prop)
                                encode_c_values = []
                                for c_value in c_values:
                                    decode_c_value = c_value["value"]
                                    if c_type == "wikibase-entityid":
                                        decode_c_value = self.get_lid(
                                            decode_c_value, decode_c_value
                                        )
                                    elif c_type == "quantity":
                                        if decode_c_value[1] != "1":
                                            decode_c_value = (
                                                decode_c_value[0],
                                                self.get_lid(
                                                    decode_c_value[1], decode_c_value[1]
                                                ),
                                            )
                                        else:
                                            decode_c_value = (decode_c_value[0], -1)
                                    c_refs = c_value.get("references")
                                    if c_refs:
                                        c_refs = encode_ref_nodes(c_refs)
                                        encode_c_values.append(
                                            {
                                                "value": decode_c_value,
                                                "references": c_refs,
                                            }
                                        )
                                    else:
                                        encode_c_values.append(
                                            {"value": decode_c_value}
                                        )
                                encode_c_type[encode_c_prop] = encode_c_values

                            encode_attr[c_type] = encode_c_type
                        value = encode_attr
                    if attr == "label":
                        compress_value = False
                    else:
                        compress_value = True
                    value = serialize_value(value, compress_value=compress_value)
                    buff_size += len(value)
                    buff[attr].append([lid, value])

                # Save buffer data
                if buff_size > cf.LMDB_BUFF_BYTES_SIZE:
                    p_bar.set_description(desc=update_desc())
                    buff = save_buff(buff)
                    buff_size = 0

            if buff_size:
                p_bar.set_description(desc=update_desc())
                buff = save_buff(buff)
                buff_size = 0


def parse_json_dump(json_line):
    if isinstance(json_line, bytes) or isinstance(json_line, bytearray):
        line = json_line.rstrip().decode(cf.ENCODING)
    else:
        line = json_line.rstrip()
    if line in ("[", "]"):
        return None

    if line[-1] == ",":
        line = line[:-1]
    try:
        obj = ujson.loads(line)
    except ValueError:
        return None
    if obj["type"] != "item" and is_wd_item(obj["id"]) is False:
        return None

    wd_id = obj["id"]
    wd_obj = {}

    def update_dict(attribute, attr_value):
        if attribute == "aliases":
            wd_obj[attribute] = {
                lang: {v.get(attr_value) for v in value}
                for lang, value in obj.get(attribute, {}).items()
            }

        else:
            wd_obj[attribute] = {
                lang: value.get(attr_value)
                for lang, value in obj.get(attribute, {}).items()
            }

    update_dict(attribute="labels", attr_value="value")
    update_dict(attribute="descriptions", attr_value="value")
    update_dict(attribute="sitelinks", attr_value="title")
    update_dict(attribute="aliases", attr_value="value")

    # Get english label:
    wd_obj["label"] = wd_obj.get("labels", {}).get("en", wd_id)

    # Statements
    if obj.get("claims"):
        for prop, claims in obj["claims"].items():
            if wd_obj.get("claims") is None:
                wd_obj["claims"] = defaultdict()

            # if wd_obj.get("claims_provenance") is None:
            #     wd_obj["claims_provenance"] = defaultdict()

            for claim in claims:
                if (
                    claim.get("mainsnak") is None
                    or claim["mainsnak"].get("datavalue") is None
                ):
                    continue
                claim_type = claim["mainsnak"]["datavalue"]["type"]
                claim_value = claim["mainsnak"]["datavalue"]["value"]

                claim_references = claim.get("references")
                if claim_references:
                    nodes = []
                    for reference_node in claim_references:
                        if not reference_node.get("snaks"):
                            continue
                        node = {}
                        for ref_prop, ref_claims in reference_node["snaks"].items():
                            for ref_claim in ref_claims:
                                if ref_claim.get("datavalue") is None:
                                    continue
                                ref_type = ref_claim["datavalue"]["type"]
                                ref_value = ref_claim["datavalue"]["value"]
                                if node.get(ref_type) is None:
                                    node[ref_type] = defaultdict(list)

                                if ref_type == "wikibase-entityid":
                                    ref_value = ref_value["id"]
                                elif ref_type == "time":
                                    ref_value = ref_value["time"]
                                    ref_value = ref_value.replace("T00:00:00Z", "")
                                    if ref_value[0] == "+":
                                        ref_value = ref_value[1:]
                                elif ref_type == "quantity":
                                    ref_unit = ref_value["unit"]
                                    ref_unit = ref_unit.replace(cf.WD, "")
                                    ref_value = ref_value["amount"]
                                    if ref_value[0] == "+":
                                        ref_value = ref_value[1:]
                                    ref_value = (ref_value, ref_unit)
                                elif ref_type == "monolingualtext":
                                    ref_value = ref_value["text"]

                                node[ref_type][ref_prop].append(ref_value)
                        nodes.append(node)
                    claim_references = nodes
                else:
                    claim_references = []

                if wd_obj["claims"].get(claim_type) is None:
                    wd_obj["claims"][claim_type] = defaultdict(list)

                # if wd_obj["claims_provenance"].get(claim_type) is None:
                #     wd_obj["claims_provenance"][claim_type] = defaultdict(list)

                if claim_type == "wikibase-entityid":
                    claim_value = claim_value["id"]
                elif claim_type == "time":
                    claim_value = claim_value["time"]
                    claim_value = claim_value.replace("T00:00:00Z", "")
                    if claim_value[0] == "+":
                        claim_value = claim_value[1:]
                elif claim_type == "quantity":
                    claim_unit = claim_value["unit"]
                    claim_unit = claim_unit.replace(cf.WD, "")
                    claim_value = claim_value["amount"]
                    if claim_value[0] == "+":
                        claim_value = claim_value[1:]
                    claim_value = (claim_value, claim_unit)
                elif claim_type == "monolingualtext":
                    claim_value = claim_value["text"]

                wd_obj["claims"][claim_type][prop].append(
                    {"value": claim_value, "references": claim_references}
                )
                # wd_obj["claims"][claim_type][prop].append(claim_value)
                # wd_obj["claims_provenance"][claim_type][prop].append(
                #     {"value": claim_value, "provenance": claim_references}
                # )

    return wd_id, wd_obj
