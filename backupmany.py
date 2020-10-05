import argparse
import pyodbc
import sys
import logging
import zipfile
import re
import yaml
from sys import exit
from pathlib import Path
from datetime import datetime, timedelta
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock

_VERSION = "to_be_filled_by_CI"

def getLogger(dsuffix=None, tofile=False, tostdout=False):
    """
    Prepares logging facility
    :param dsuffix: a distinguishing suffix
    :return: logger object to be used in the rest of subs
    """
    log_formatter_stream = logging.Formatter(fmt="{asctime} {message}", style="{")
    log_formatter_file = logging.Formatter(fmt="{asctime} [{threadName}] [{levelname}] {message}", style="{")
    if tostdout:
        log_handler_stream = logging.StreamHandler(sys.stdout)
    else:
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
        self.dbc = dbc
        self.log = log
        self.dbset = dict()
        self.e_updated = Event()
        self.l_update = Lock()
        self.update()

    def update(self):
        self.l_update.acquire()
        self.e_updated.clear()
        self.log.debug("Update the list of known DBs")
        try:
            con = self.dbc.getcon()
            if con is not None:
                try:
                    with con.cursor() as cur:
                        cur.execute("SELECT name, recovery_model_desc, user_access_desc FROM sys.databases;")
                        for row in cur.fetchall():
                            self.log.debug("Found database \"{}\"".format(row[0].lower()))
                            self.dbset[row[0].lower()] = {"recovery_model": row[1], "user_access": row[2]}
                except Exception:
                    self.log.exception("DB operation failed")
                con.close()
        finally:
            self.e_updated.set()
            self.l_update.release()

    def exists(self, dbname):
        """
        Returns True if a db name exists
        :param dbname:
        :return: True of False
        """
        self.e_updated.wait()
        if dbname.lower() in self.dbset:
            return True
        else:
            return False

    def get_recovery_model(self, dbname):
        """
        Returns database recovery model
        :param dbname:
        :return: database's recovery mode (text)
        """
        self.e_updated.wait()
        return self.dbset[dbname.lower()]["recovery_model"]

    def get_user_access_mode(self, dbname):
        """
        Returns database user access mode
        :param dbname:
        :return: database's user access mode (text)
        """
        self.e_updated.wait()
        return self.dbset[dbname.lower()]["user_access"]

    def getall(self):
        """
        Returns a var that keeps the DB set
        :return: a set with DB names
        """
        self.e_updated.wait()
        return self.dbset


def prepareConfig(config):
    """
    Prepares config so it can be later used with no issues:
    lowercases db keys so there will be no issues during search, creates default if not exist
    :param config: config dict
    :return: None
    """
    if "suffix" in config:
        if not isinstance(config["suffix"], dict):
            raise RuntimeError("Config suffix section should be dict")
        if "default" not in config["suffix"]:
            config["suffix"]["default"] = "SELECT 'noversion';"
        if "databases" not in config["suffix"]:
            config["suffix"]["databases"] = dict()
        elif not isinstance(config["suffix"], dict):
            raise RuntimeError("Config suffix section should be dict")
        add_config = dict()
        for key, value in config["suffix"]["databases"].items():
            key_lower = key.lower()
            if key_lower not in config["suffix"]["databases"]:
                add_config[key_lower] = value
        config["suffix"]["databases"].update(add_config)


def backupDB(dbc, name, names, path, prefix, log, config=None, shrink=False):
    """
    Backups a db to a directory, checks a name against existent ones before backing up
    :param dbc: DBConnect obj
    :param name: DB name
    :param names: DBSet obj
    :param path: Path to the backup directory
    :param prefix: Backup file prefix
    :param log: logger obj
    :param config: config dict
    :param shrink: shrink the DB before backing up
    :return: backup name or None
    """
    if names.exists(name):
        con = dbc.getcon()
        if con is not None:
            try:
                suffix = None
                suffix_query = None
                if config:
                    if "suffix" in config:
                        log.debug("Determining suffix query for \"{}\"".format(name))
                        suffix_query = config["suffix"]["databases"].get(name.lower(), config["suffix"]["default"])
                is_compressed = False
                with con.cursor() as cur:
                    if shrink:
                        log.info("Shrinking DB \"{}\"".format(name))
                        if names.get_recovery_model(name) != "SIMPLE":
                            log.debug("Shrinking: swithching DB \"{}\" to SIMPLE".format(name))
                            cur.execute("ALTER DATABASE {} SET RECOVERY SIMPLE;".format(name))
                        log.debug("Shrinking: doing shrink of DB \"{}\"".format(name))
                        cur.execute("DBCC SHRINKDATABASE(?) WITH NO_INFOMSGS;", (name, ))
                        if names.get_recovery_model(name) != "SIMPLE":
                            log.debug("Shrinking: swithching DB \"{}\" back to {}".format(name, names.get_recovery_model(name)))
                            cur.execute("ALTER DATABASE {} SET RECOVERY {};".format(name, names.get_recovery_model(name)))
                    if suffix_query:
                        cur.execute("USE {};".format(name))
                        cur.execute(suffix_query)
                        suffix = (cur.fetchone())[0]
                    else:
                        log.debug("Suffix query for \"{}\" is null - no suffix will be added to the backup file".format(name))
                    bak_name = "_".join((prefix, name, datetime.today().strftime("%Y%m%d_%H%M%S"))) + ("_{}.bak".format(suffix) if suffix else ".bak")
                    full_bak_name = Path(path).joinpath(bak_name).as_posix()
                    log.info("Backing up DB \"{}\" to \"{}\"".format(name, full_bak_name))
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


def backupAndCompress(dbc, name, names, path, prefix, log, config=None, shrink=False):
    """
    Combines backup and compress operation. To be used as a thread's target
    :param dbc: DBConnect obj
    :param name: DB name
    :param names: DBSet obj
    :param path: Path to the backup directory
    :param prefix: Backup file prefix
    :param log: logger obj
    :param config: config dict
    :param shrink: shrink the DB before backing up
    :return: zip-file name or None
    """
    bak_info = backupDB(dbc, name, names, path, prefix, log, config, shrink)
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
    item_glob_pattern = "{prefix}_{name}_????????_??????*.{suffix}"  # a pattern of backup files
    item_re_pattern = "({})_(\S*)_(\d{{8}}_\d{{6}})(?:_\S+)?\.(\S+)"  # a pattern for RE
    try:
        name_parts = dict(zip(("prefix", "name", "timestamp", "suffix"),
                              re.search(item_re_pattern.format(prefix), name).groups()))
    except Exception:
        log.exception("Could not find valid name parts in \"{}\"".format(name))
        return 0, 0
    try:
        name_time = datetime.strptime(name_parts["timestamp"], "%Y%m%d_%H%M%S")
    except Exception:
        log.exception("Could not convert \"{}\" to time".format(name_parts["timestamp"]))
        return 0, 0
    log.debug("Performing cleanup for \"{}\"".format(item_glob_pattern.format(**name_parts)))
    count = {"total": 0, "removed": 0}
    for item in Path(path).glob(item_glob_pattern.format(**name_parts)):
        if item.name == name:
            log.debug("Skipping this session's file \"{}\"".format(item.as_posix()))
            continue
        count["total"] += 1
        try:
            item_parts = dict(zip(("prefix", "name", "timestamp", "suffix"), re.search(item_re_pattern.format(prefix),
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
        "server": r"localhost",
        "prefix": "auto_backup_db",
        "workers": (1, 4)  # Default and allowed maximum
    }
    prog_name = Path(sys.argv[0]).name
    prog_description = """\
Backup as many DBs as given in command line
(and compress them to zip-files if server's compression not available)\
"""
    config_description = """\
YAML config (-c) may look like:
suffix:
    default: SELECT CONCAT('v', Version) FROM Version;
    databases:
        db1: SELECT CONCAT('v', V1) FROM VersionInfo;
        db2: SELECT CONCAT('DBversion', Ver) FROM DBInfo;
        # the null below excludes db3 from suffix processing
        db3: null\
    """
    cmd = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=prog_description,
                                  epilog=config_description)
    cmd.add_argument("-s", metavar=r"host\instance", help="SQL server name ({})".format(defaults["server"]),
                     default=defaults["server"])
    cmd.add_argument("-d", metavar="list", help="Comma separated list of DB names", required=True)
    cmd.add_argument("-p", metavar="path", help="Absolute path to the directory to store backups", required=True)
    cmd.add_argument("-x", metavar="text", help="Backup file name prefix ({})".format(defaults["prefix"]),
                     default=defaults["prefix"])
    cmd.add_argument("-w", metavar="num",
                     help="Number of workers running in parallel (default/max {dw[0]}/{dw[1]})".format(dw=defaults["workers"]),
                     default=defaults["workers"][0], type=int)
    cmd.add_argument("-r", metavar="timestr", help="Retention time of old files, w(eeks)d(days)h(ours)m(inutes). "
                                                   "If given, the script performs cleanup")
    cmd.add_argument("-m", help="Simulate file removal - just write a log record", action="store_true", default=False)
    cmd.add_argument("-l", metavar="text", help="Distinguishing log file name suffix")
    cmd.add_argument("-c", metavar="path", help="YAML config file path")
    cmd.add_argument("--shrink", help="Truncate log and shrink DB before backung up ({})".format(False), action="store_true", default=False)
    cmd.add_argument("--logtofile", help="Enable log to file ({})".format(False), action="store_true", default=False)
    cmd.add_argument("--logtostdout", help="Log to stdout instead of stderr ({})".format(False),
                     action="store_true", default=False)
    cmd.add_argument("-v", action="version", version=_VERSION)
    cmdargs = cmd.parse_args()
    log = getLogger(cmdargs.l, cmdargs.logtofile, cmdargs.logtostdout)
    config = None
    if cmdargs.c:
        try:
            with open(cmdargs.c) as configstream:
                config = yaml.load(configstream, yaml.CLoader) or dict()
            log.debug("Preparing config")
            prepareConfig(config)
        except Exception:
            log.exception("Could not process config file {}".format(cmdargs.c))
            exit(1)
    if not Path(cmdargs.p).is_absolute():
        log.critical("The path to the backup directory is not absolute")
        exit(1)
    if cmdargs.r is not None:
        cmdargs.r = getTimeDelta(cmdargs.r, log)
        if cmdargs.r is None:
            log.critical("Retention time string wasn't parsed. Probably, the string is not correct. "
                         "Cleanup will not be performed")
            exit(1)
        else:
            log.debug("Retention time is set to {}".format(cmdargs.r))
    cmdargs.d = re.split("\s*,\s*", cmdargs.d.strip())
    dbc = DBConnect(cmdargs.s, prog_name, log)
    log.info("Checking DB connection")
    if dbc.getcon() is None:
        log.critical("Could not connect to the DB server \"{}\"".format(cmdargs.s))
        exit(1)
    else:
        names = DBSet(dbc, log)
        log.debug("Backing up databases using no more than {} worker(s)".format(min(cmdargs.w, defaults["workers"][1])))
        with ThreadPoolExecutor(max_workers=min(cmdargs.w, defaults["workers"][1])) as executor:
            successes = list(name for name in executor.map(backupAndCompress,
                                                           (dbc,) * len(cmdargs.d),
                                                           (dbname for dbname in cmdargs.d),
                                                           (names,) * len(cmdargs.d),
                                                           (cmdargs.p,) * len(cmdargs.d),
                                                           (cmdargs.x,) * len(cmdargs.d),
                                                           (log,) * len(cmdargs.d),
                                                           (config,) * len(cmdargs.d),
                                                           (cmdargs.shrink,) * len(cmdargs.d)) if name is not None)
        exit_status = 0
        if len(cmdargs.d) == len(successes):
            chosen_log = log.info
        else:
            chosen_log = log.warning
            exit_status = 1
        chosen_log("Backup complete. {}/{} (successfull/total)".format(len(successes), len(cmdargs.d)))
        if cmdargs.r is not None:
            log.info("Cleaning up from old files")
            for item in successes:
                removeOld(cmdargs.p, item, cmdargs.x, cmdargs.r, cmdargs.m, log)
        exit(exit_status)