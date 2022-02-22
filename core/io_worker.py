import logging
import math
import os
import shutil
import pickle
import zlib


def get_size_obj(num, suffix="B"):
    if num == 0:
        return "0"
    magnitude = int(math.floor(math.log(num, 1024)))
    val = num / math.pow(1024, magnitude)
    if magnitude > 7:
        return "{:3.1f}{}{}".format(val, "Yi", suffix)
    return "{:3.1f}{}{}".format(
        val, ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"][magnitude], suffix
    )


def print_status(message, is_screen=True, is_log=True) -> object:
    if isinstance(message, int):
        message = f"{message:,}"

    if is_screen:
        print(message)
    if is_log:
        logging.info(message)


def delete_folder(folder_dir):
    if os.path.exists(folder_dir):
        shutil.rmtree(folder_dir, ignore_errors=False)
    return True


def delete_file(file_dir):
    if os.path.exists(file_dir):
        os.remove(file_dir)
    return True


def create_dir(file_dir):
    folder_dir = os.path.dirname(file_dir)
    if not os.path.exists(folder_dir):
        os.makedirs(folder_dir)


def save_obj_pkl(file_name, save_object, is_compress=False, is_message=True):
    create_dir(file_name)
    save_file = file_name
    if ".pkl" not in file_name:
        save_file = file_name + ".pkl"
    if is_compress and ".zlib" not in file_name:
        save_file += ".zlib"

    temp_file = save_file + ".temp"

    # Write temp
    with open(temp_file, "wb") as fp:
        if is_compress:
            save_data = zlib.compress(
                pickle.dumps(save_object, pickle.HIGHEST_PROTOCOL)
            )
            fp.write(save_data)
        else:
            pickle.dump(save_object, fp, pickle.HIGHEST_PROTOCOL)

    try:
        if os.path.exists(save_file):
            os.remove(save_file)
    except Exception as message:
        print_status(message)

    os.rename(temp_file, save_file)
    if is_message:
        print_status("Saved: - %d - %s" % (len(save_object), save_file), is_log=False)
    return save_file


def load_obj_pkl(file_name, is_message=False):
    load_obj = None
    if not os.path.exists(file_name) and ".pkl" not in file_name:
        file_name = file_name + ".pkl"

    if not os.path.exists(file_name) and ".zlib" not in file_name:
        file_name = file_name + ".zlib"
    with open(file_name, "rb") as fp:
        if ".zlib" in file_name:
            load_obj = pickle.loads(zlib.decompress(fp.read()))
        else:
            load_obj = pickle.load(fp)
    if is_message and load_obj:
        print_status("%d loaded items - %s" % (len(load_obj), file_name))
    return load_obj


def get_size_of_file(num, suffix="B"):
    """Get human friendly file size
    https://gist.github.com/cbwar/d2dfbc19b140bd599daccbe0fe925597#gistcomment-2845059

    Args:
        num (int): Bytes value
        suffix (str, optional): Unit. Defaults to 'B'.

    Returns:
        str: file size0
    """
    if num == 0:
        return "0"
    magnitude = int(math.floor(math.log(num, 1024)))
    val = num / math.pow(1024, magnitude)
    if magnitude > 7:
        return "{:3.1f}{}{}".format(val, "Yi", suffix)
    return "{:3.1f}{}{}".format(
        val, ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"][magnitude], suffix
    )
