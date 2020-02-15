import argparse
import pyodbc
import sys
import logging
import zipfile
import re
from sys import exit
from pathlib import Path
from datetime import datetime, timedelta
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor

_VERSION="0.2.0"

def getLogger(dsuffix=None, tofile=False):
    """
    Prepares logging facility
    :param dsuffix: a distinguishing suffix
    :return: logger object to be used in the rest of subs
    """
    log_formatter_stream = logging.Formatter(fmt="{asctime} {message}", style="{")
    log_formatter_file = logging.Formatter(fmt="{asctime} [{threadName}] [{levelname}] {message}", style="{")
    log_handler_stream = logging.StreamHandler()
    log_handler_stream.setLevel(logging.INFO)
    log_handler_stream.setFormatter(log_formatter_stream)
    log_logger = logging.getLogger(Path(sys.argv[0]).name)
    log_logger.addHandler(log_handler_stream)
    if tofile:
        if dsuffix is not None: dsuffix = dsuffix.strip()
        if dsuffix is not None and len(dsuffix) > 0:
            log_handler_file = logging.FileHandler(Path(sys.argv[0]).
                                                   with_name(Path(sys.argv[0]).stem + "_" + dsuffix).
                                                   with_suffix(".log").as_posix(), mode="w")
        else:
            log_handler_file = logging.FileHandler(Path(sys.argv[0]).with_suffix(".log").as_posix(), mode="w")
        log_handler_file.setLevel(logging.DEBUG)
        log_handler_file.setFormatter(log_formatter_file)
        log_logger.addHandler(log_handler_file)
    log_logger.setLevel(logging.DEBUG)
    return log_logger


class DBConnect:
    """
    The class keeps connection vars and returns a new connection on demand
    """

    def __init__(self, server, prog_name, log):
        """
        Stores vars
        :param server: SQL server name
        :param prog_name: the name of the program that connects to the SQL server
        :param log: logger obj
        """
        self.server = server
        self.prog_name = prog_name
        self.log = log
        self.known_drivers = (
            "SQL Server",
            "SQL Native Client",
            "SQL Server Native Client 10.0",
            "SQL Server Native Client 11.0",
            "ODBC Driver 11 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "ODBC Driver 13.1 for SQL Server",
            "ODBC Driver 17 for SQL Server")
        self.known_drivers = {x: y for x, y in zip(self.known_drivers, range(len(self.known_drivers)))}
        self.get_driver()

    def get_driver(self):
        driver_score = -1
        self.driver = None
        for d in pyodbc.drivers():
            try:
                if self.known_drivers[d] > driver_score:
                    self.log.debug("Found better driver \"{}\"".format(d))
                    self.driver = d
                    driver_score = self.known_drivers[d]
                else:
                    self.log.debug("Driver \"{}\" is not the best one".format(d))
            except KeyError:
                self.log.debug("Driver \"{}\" is unknown".format(d))

    def getcon(self):
        try:
            con = pyodbc.connect(driver=self.driver,
                                 server=self.server, trusted_connection="yes",
                                 app=self.prog_name, autocommit=True)
        except Exception:
            self.log.exception("Connect to the DB server failed")
            return None
        else:
            return con


class DBSet:
    """
    The class keeps a databases list and returns it on call
    """

    def __init__(self, dbc, log):
        """
        Fetches the DB list into a member set
        :param dbc: DBConnect obj
        :param log: logger obj
        """
        self.log = log
        con = dbc.getcon()
        self.dbset = set()
        if con is not None:
            try:
                with con.cursor() as cur:
                    cur.execute("SELECT name FROM sys.databases;")
                    for row in cur.fetchall():
                        self.log.debug("Found database \"{}\"".format(row[0].lower()))
                        self.dbset.add(row[0].lower())
            except Exception:
                self.log.exception("DB operation failed")
            con.close()

    def exists(self, dbname):
        """
        Returns True if a db name exists
        :param dbname:
        :return: True of False
        """
        if dbname.lower() in self.dbset:
            return True
        else:
            return False

    def getall(self):
        """
        Returns a var whick keeps the DB set
        :return: a set with DB names
        """
        return self.dbset


def backupDB(dbc, name, names, path, prefix, log):
    """
    Backups a db to a directory, checks a name against existent ones before backing up
    :param dbc: DBConnect obj
    :param name: DB name
    :param names: DBSet obj
    :param path: Path to the backup directory
    :param prefix: Backup file prefix
    :param log: logger obj
    :return: backup name or None
    """
    if names.exists(name):
        con = dbc.getcon()
        if con is not None:
            bak_name = "_".join((prefix, name, datetime.today().strftime("%Y%m%d_%H%M%S"))) + ".bak"
            full_bak_name = Path(path).joinpath(bak_name).as_posix()
            log.info("Backing up DB \"{}\" to \"{}\"".format(name, full_bak_name))
            is_compressed = False
            try:
                with con.cursor() as cur:
                    try:
                        cur.execute("BACKUP DATABASE ? TO DISK = ? WITH COPY_ONLY, COMPRESSION;", (name, full_bak_name.replace("/", "\\")))
                    except pyodbc.ProgrammingError:
                        log.debug("Could not use compression. Will try without one")
                        cur.execute("BACKUP DATABASE ? TO DISK = ? WITH COPY_ONLY;", (name, full_bak_name.replace("/", "\\")))
                    else:
                        is_compressed = True
                    while cur.nextset():
                        pass
            except Exception:
                log.exception("Backup operation failed")
                log.info("Failed backing up DB \"{}\"".format(name))
                return None
            else:
                log.info("Successfully backed up DB \"{}\"".format(name))
                con.close()
                return bak_name, is_compressed
        else:
            con.close()
            return None
    else:
        log.error("DB \"{}\" not found".format(name))
        return None


def compressBak(name, path, log):
    """
    Compresses a bak-file into a ZIP archive and removes the bak-file after compression
    :param name: bak-file name
    :param path: Path to the backup directory
    :param log: logger obj
    :return: ZIP-file name or None
    """
    tmp_ext = ".tmp"
    zip_ext = ".zip"
    full_bak_name = Path(path).joinpath(name).as_posix()
    full_zip_tmp_name = Path(path).joinpath(Path(name).stem + tmp_ext).as_posix()
    zip_name = Path(name).stem + zip_ext
    full_zip_name = Path(path).joinpath(zip_name).as_posix()
    log.info("Compressing bak-file \"{}\"".format(name))
    log.debug("Making ZIP-compressed temp file \"{}\" from \"{}\"".format(full_zip_tmp_name, full_bak_name))
    try:
        with ZipFile(full_zip_tmp_name, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(full_bak_name, name)
        log.debug("Renaming temp file \"{}\" to \"{}\"".format(full_zip_tmp_name, full_zip_name))
        Path(full_zip_tmp_name).rename(full_zip_name)
        log.debug("Removing original bak-file \"{}\"".format(full_bak_name))
        Path(full_bak_name).unlink()
    except Exception:
        log.exception("Compress operation failed")
        log.info("Failed compressing bak-file \"{}\"".format(name))
        return None
    else:
        log.info("Successfully compressed bak-file \"{}\" to zip-file \"{}\"".format(name, zip_name))
        return zip_name
    finally:
        if Path(full_zip_tmp_name).exists():
            log.debug("Removing temp file \"{}\"".format(full_zip_tmp_name))
            try:
                Path(full_zip_tmp_name).unlink()
            except Exception:
                log.exception("Could not remove \"{}\"".format(full_zip_tmp_name))


def backupAndCompress(dbc, name, names, path, prefix, log):
    """
    Combines backup and compress operation. To be used as a thread's target
    :param dbc: DBConnect obj
    :param name: DB name
    :param names: DBSet obj
    :param path: Path to the backup directory
    :param prefix: Backup file prefix
    :param log: logger obj
    :return: zip-file name or None
    """
    bak_info = backupDB(dbc, name, names, path, prefix, log)
    zip_name = None
    if bak_info is not None:
        if bak_info[1]:
            zip_name = bak_info[0]
        else:
            zip_name = compressBak(bak_info[0], path, log)
    return zip_name


def removeOld(path, name, prefix, rttime, simulate, log):
    """
    Removes old files
    :param path: Path to the backup directory
    :param name: Base file name
    :param prefix: Distinguishing prefix
    :param rttime: Retention time (timedelta)
    :param simulate: Simulate file removal - just log a message without real removing
    :param log: logger obj
    :return: a tuple: (Total items number, Removed items number)
    """
    item_glob_pattern = "{prefix}_{name}_????????_??????.{suffix}"  # a pattern of backup files
    try:
        name_parts = dict(zip(("prefix", "name", "timestamp", "suffix"),
                              re.search("({})_(\S*)_(\d{{8}}_\d{{6}})\.(\S+)".format(prefix), name).groups()))
    except Exception:
        log.exception("Could not find valid name parts in \"{}\"".format(name))
        return 0, 0
    try:
        name_time = datetime.strptime(name_parts["timestamp"], "%Y%m%d_%H%M%S")
    except Exception:
        log.exception("Could not convert \"{}\" to time".format(name_parts["timestamp"]))
        return 0, 0
    log.debug("Performing clean up for \"{}\"".format(item_glob_pattern.format(**name_parts)))
    count = {"total": 0, "removed": 0}
    for item in Path(path).glob(item_glob_pattern.format(**name_parts)):
        if item.name == name:
            log.debug("Skipping this session's file \"{}\"".format(item.as_posix()))
            continue
        count["total"] += 1
        try:
            item_parts = dict(zip(("prefix", "name", "timestamp", "suffix"),
                              re.search("({})_(\S*)_(\d{{8}}_\d{{6}})\.(\S+)".format(prefix),
                                        item.as_posix()).groups()))
            item_time = datetime.strptime(item_parts["timestamp"], "%Y%m%d_%H%M%S")
        except Exception:
            log.debug("Skipping irrelevant file \"{}\"".format(item.as_posix()))
            continue
        if name_time - item_time > rttime:
            log.debug("Removing \"{}\"".format(item.as_posix()))
            try:
                if not simulate:
                    item.unlink()
                else:
                    log.debug("Simulating removal of old file \"{}\"".format(item.as_posix()))
            except Exception:
                log.exception("Could not remove \"{}\"".format(item.as_posix()))
            else:
                count["removed"] += 1
        else:
            log.debug("Skipping old file \"{}\", time delta is {} - too recent".format(item.as_posix(),
                                                                                   name_time - item_time))
    return count


def getTimeDelta(timestr, log):
    """
    Returns a timedelta obj extracted from the input str
    :param timestr: time string, WeeksDaysHoursMinutes, IwJdKhLm
    :param log: logger obj
    :return: a timedelta obj
    """
    rv = None
    try:
        time_parts = tuple(int(x[0:-1]) for x in re.search("^(\d+w)?\s*(\d+d)?\s*(\d+h)?\s*(\d+m)?$", timestr.strip())
                           .groups(default="0t"))
        rv = timedelta(**dict(zip(("weeks", "days", "hours", "minutes"), time_parts)))
    except Exception:
        log.exception("Could not extract a time delta from \"{}\"".format(timestr))
    return rv


if __name__ == "__main__":
    defaults = {
        "server": r"localhost\MSSQLSERVER",
        "prefix": "auto_backup_db",
        "workers": (1, 4)  # Default and allowed maximum
    }
    prog_name = Path(sys.argv[0]).name
    cmd = argparse.ArgumentParser(description="Backups as many DBs as given in command line, "
                                              "and compressed them to zip-files")
    cmd.add_argument("-s", metavar=r"host\instance", help="SQL server name ({})".format(defaults["server"]),
                     default=defaults["server"])
    cmd.add_argument("-d", metavar="list", help="Comma separated list of DB names", required=True)
    cmd.add_argument("-p", metavar="path", help="Absolute path to the directory to store backups", required=True)
    cmd.add_argument("-x", metavar="text", help="Backup file name prefix ({})".format(defaults["prefix"]),
                     default=defaults["prefix"])
    cmd.add_argument("-w", metavar="num", help="Max. workers running in parallel ({})".format(defaults["workers"]),
                     default=defaults["workers"][0], type=int)
    cmd.add_argument("-r", metavar="timestr", help="Retention time of old files, w(eeks)d(days)h(ours)m(inutes). "
                                                   "If given, the script performs clean up")
    cmd.add_argument("-m", help="Simulate file removal - just write a log record", action="store_true", default=False)
    cmd.add_argument("-l", metavar="text", help="Distinguishing log file name suffix")
    cmd.add_argument("--logtofile", help="Enable log to file ({})".format(False), action="store_true", default=False)
    cmd.add_argument("-v", action="version", version=_VERSION)
    cmdargs = cmd.parse_args()
    log = getLogger(cmdargs.l, cmdargs.logtofile)
    if not Path(cmdargs.p).is_absolute():
        log.critical("The path to the backup directory is not absolute")
        exit(1)
    if cmdargs.r is not None:
        cmdargs.r = getTimeDelta(cmdargs.r, log)
        if cmdargs.r is None:
            log.critical("Retention time string wasn't parsed. Probably, the string is not correct. "
                         "Clean up will not be performed")
            exit(1)
        else:
            log.debug("Retention time is set to {}".format(cmdargs.r))
    cmdargs.d = re.split("\s*,\s*", cmdargs.d.strip())
    dbc = DBConnect(cmdargs.s, prog_name, log)
    names = DBSet(dbc, log)
    log.debug("Backing up databases using no more than {} worker(s)".format(min(cmdargs.w, defaults["workers"][1])))
    with ThreadPoolExecutor(max_workers=min(cmdargs.w, defaults["workers"][1])) as executor:
        successes = list(name for name in executor.map(backupAndCompress,
                                                       (dbc,) * len(cmdargs.d),
                                                       (dbname for dbname in cmdargs.d),
                                                       (names,) * len(cmdargs.d),
                                                       (cmdargs.p,) * len(cmdargs.d),
                                                       (cmdargs.x,) * len(cmdargs.d),
                                                       (log,) * len(cmdargs.d)) if name is not None)
    if len(cmdargs.d) == len(successes):
        chosen_log = log.info
    else:
        chosen_log = log.warning
    chosen_log("Back up complete. {} successfull out of {}".format(len(successes), len(cmdargs.d)))
    if cmdargs.r is not None:
        log.info("Cleaning up from old files")
        for item in successes:
            removeOld(cmdargs.p, item, cmdargs.x, cmdargs.r, cmdargs.m, log)
