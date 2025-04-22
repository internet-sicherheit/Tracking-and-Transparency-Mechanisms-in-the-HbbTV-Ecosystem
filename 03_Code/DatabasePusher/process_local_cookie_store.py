import base64
import os
import sqlite3
from push_ops import stream_to_BQ
import logging
import zipfile
import tarfile

# Each log line includes the date and time, the log level, the current function and the message
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(funcName)-30s %(message)s')
# The log file is the same as the module name plus the suffix ".log"
# i.e.: calculate.py -> calculate.py.log
fh = logging.FileHandler("%s.log" % (os.path.join("logs", os.path.basename(__file__))))
sh = logging.StreamHandler()
fh.setLevel(logging.DEBUG)  # set the log level for the log file
fh.setFormatter(formatter)
sh.setFormatter(formatter)
sh.setLevel(logging.INFO)  # set the log level for the console
logger = logging.getLogger(__name__)
logger.addHandler(fh)
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)
logger.propagate = False


def unzip_databases(storage_files, storage_path):
    """
    Unzips the cookie store and local storages that we exfiltrated from the rooted TV.
    :param storage_files:
    :param storage_path:
    """
    for file in storage_files:
        tar_file = os.path.join(storage_path, file)

        if not os.path.isdir(tar_file) and tarfile.is_tarfile(tar_file):
            tar = tarfile.open(tar_file)
            tar.extractall(path=tar_file.replace(".tar", ""))
            tar.close()


def write_local_cookie_store(measurement_id):
    """
    Wrapper that identifies the cookie store and local storages files, and starts the import to the database.
    """
    store_log = os.path.join(os.path.join(os.getcwd(), "00_Data", "tv_export"))
    storage_files = os.listdir(store_log)
    logger.info(f"Found {len(storage_files)} cookie jars or local storages")

    unzip_databases(storage_files, store_log)

    storage_files = os.listdir(store_log)
    for file_name in storage_files:
        file = os.path.join(store_log, file_name)
        # Get measurement ID
        # measurement_id = file_name.split('_')[0]

        # Skip compressed stores
        if file.endswith(".tar"):
            continue

        elif os.path.isdir(file):
            for store in os.listdir(file):
                store_file = os.path.join(store_log, file, store)
                if store == "Cookies":
                    write_cookie_db(store_file, measurement_id)
                elif os.path.isdir(store_file) and store == "Local Storage":
                    write_local_storage(os.path.join(store_file), measurement_id)


def write_cookie_db(file, measurement_id):
    """
    Writes the cookie store (exfiltrated form the TV) to the database.
    :param file: Path to the cookie store.
    :param measurement_id: The ID of the measurement.
    """
    all_cookies = []

    # DB connection
    logger.info("Writing cookie DB...")
    con = sqlite3.connect(file)
    cur = con.cursor()
    result = cur.execute("SELECT * FROM cookies;")
    db_cookies = result.fetchall()

    # Iterate over all cookies and convert them into a Big Query friendly structure
    for dd in db_cookies:
        creation_utc, host_key, name, value, path, expires_utc, secure, httponly, last_access_utc, has_expires, persistent, priority, encrypted_value, firstpartyonly = dd
        cookie = dict()
        cookie['scan_profile'] = measurement_id
        cookie['creation_utc'] = creation_utc
        cookie['host_key'] = host_key
        cookie['name'] = name
        cookie['value'] = value
        cookie['path'] = path
        cookie['expires_utc'] = expires_utc
        cookie['secure'] = secure
        cookie['httponly'] = httponly
        cookie['last_access_utc'] = last_access_utc
        cookie['has_expires'] = has_expires
        cookie['persistent'] = persistent
        cookie['encrypted_value'] = base64.b64encode(encrypted_value).decode()
        cookie['firstpartyonly'] = firstpartyonly
        cookie['storage_type'] = "cookie jar"

        all_cookies.append(cookie)

    con.close()

    # Write Data to Big Query
    stream_to_BQ('hbbtv-research.hbbtv.tv_cookie_store', all_cookies)


def write_local_storage(files, measurement_id):
    """
    Writes the local storages  (exfiltrated form the TV) to the database.
    :param files: Path to the directory that holds the local storages.
    :param measurement_id: The ID of the measurement.
    """
    all_cookies = []
    logger.info("Writing local storages...")

    # Iterate over the local storages of the sites/channels
    for storage_file in os.listdir(files):
        if storage_file.endswith("localstorage"):
            # Get site name
            host_key = storage_file.split("_0")[0]

            # Get cookies from local storage
            con = sqlite3.connect(os.path.join(files, storage_file))
            cur = con.cursor()
            result = cur.execute("SELECT * FROM ItemTable;")
            ls_cookies = result.fetchall()
            for ls_cookie in ls_cookies:
                key, value = ls_cookie
                cookie = dict()
                cookie['scan_profile'] = measurement_id
                cookie['host_key'] = host_key
                cookie['name'] = key
                cookie['encrypted_value'] = base64.b64encode(value).decode()
                cookie['storage_type'] = "local storage"
                all_cookies.append(cookie)
            con.close()

    # Write Data to Big Query
    stream_to_BQ('hbbtv-research.hbbtv.tv_cookie_store', all_cookies)
