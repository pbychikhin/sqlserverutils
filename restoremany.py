
import sys
import os.path
import argparse
from glob import glob
from sys import exit
from copy import deepcopy
from colorama import init as color_init, Fore, Style
from backupmany import getLogger, DBConnect, DBSet
from concurrent.futures import ThreadPoolExecutor
from time import sleep

_VERSION = "to_be_filled_by_CI"

color_init()
_YB = Fore.YELLOW + Style.BRIGHT    # Yellow Bright
_WB = Fore.WHITE + Style.BRIGHT     # White Bright
_SR = Style.RESET_ALL               # Style Reset


def winput(prompt=None):
    """
    Input in yellow
    :param prompt: prompt text
    :return: input's return
    """
    try:
        print(prompt, end="")
        print(_SR + "", end="")
        print(_WB + "", end="")
        rv = input()
        print(_SR + "", end="")
    finally:
        print(_SR + "", end="")
    return rv


def ybstr(str):
    """
    Str in Yellow Bright
    :param str:
    :return: Str in Yellow Bright
    """
    return _YB + str + _SR


class ServerProperties:
    """
    DB server properties
    """
    def __init__(self, dbc, log):
        """
        Init
        :param dbc: DBConnect obj
        :param log: logger obj
        """
        self.dbc = dbc
        self.log = log
        self.properties_to_fetch = {"InstanceDefaultDataPath": True, "InstanceDefaultLogPath": True}
        self.properties = dict()
        self.fetch_properties()

    def fetch_properties(self):
        con = self.dbc.getcon()
        if con is not None:
            log.debug("Fetch server properties")
            try:
                with con.cursor() as cur:
                    for propname, propcritical in self.properties_to_fetch.items():
                        cur.execute("SELECT CAST(SERVERPROPERTY(?) AS VARCHAR(MAX));", (propname,))
                        property = cur.fetchone()
                        if property[0]:
                            self.properties[propname] = property[0]
                        elif propcritical:
                            raise RuntimeError("Critical server property {} not found".format(propname))
            except Exception:
                log.exception("Failed fetching server properties")
                raise
            con.close()

    def __getattr__(self, item):
        return self.properties.get(item)


class MediaSets:
    """
    Look into bak-files and organize them into media sets
    """

    def __init__(self, dbc, log):
        """
        Init
        :param dbc: DBConnect obj
        :param log: logger obj
        """
        self.dbc = dbc
        self.log = log
        self.mediasets = dict()

    def add_file(self, name):
        """
        Add next bak-file for processing
        :return: nothing
        """
        con = self.dbc.getcon()
        if con is not None:
            self.log.debug("Get label from \"{}\"".format(name))
            try:
                with con.cursor() as cur:
                    cur.execute("RESTORE LABELONLY FROM DISK = ?;", (name,))
                    header_columns = tuple(x[0] for x in cur.description)
                    label = {x: y for x, y in zip(header_columns, cur.fetchone())}
            except Exception:
                self.log.exception("Get label failed")
            else:
                MediaSetId = label["MediaSetId"]
                MediaFamilyId = label["MediaFamilyId"]
                if MediaSetId not in self.mediasets:
                    self.log.debug("Add media set \"{}\"".format(MediaSetId))
                    self.mediasets[MediaSetId] = dict()
                if MediaFamilyId not in self.mediasets[MediaSetId]:
                    self.log.debug("Add media family \"{}\" to media set \"{}\"".format(MediaFamilyId, MediaSetId))
                    self.mediasets[MediaSetId][MediaFamilyId] = name
                else:
                    self.log.warning("Media family \"{}\" has already been added to media set \"{}\" as file \"{}\". Skip file \"{}\"".format(
                        MediaFamilyId, MediaSetId, self.mediasets[MediaSetId][MediaFamilyId], name))
            con.close()

    def get_filesets(self):
        """
        Return all the media sets found during add-procedures as file sets
        :return: [[file1, file2, file3], [file4, file5, file6], ...]
        """
        return [tuple(x.values()) for x in self.mediasets.values()]


class BackupFile:
    """
    Backup file definition and manipulation
    """

    def __init__(self, filenameset, dbc, log):
        """
        Init
        :param filenameset: Backup file name set
        :param dbc: DBConnect obj
        :param log: logger obj
        """
        self.filenameset = filenameset
        self.dbc = dbc
        self.log = log
        self.backupsets = tuple()
        self.backupsets_reduced = tuple()   # Only the last backupset of each DB is taken
        self.fetch_backupsets()

    def fetch_backupsets(self):
        con = self.dbc.getcon()
        if con is not None:
            try:
                with con.cursor() as cur:
                    cur.execute("RESTORE HEADERONLY FROM DISK = ?;", (self.filenameset[0],))    # One (and any) backup file is enough to get header
                    backupsets = []
                    backupsets_reduced = {}
                    header_columns = tuple(x[0] for x in cur.description)
                    for row in cur.fetchall():
                        backupset = {x: y for x, y in zip(header_columns, row)}
                        backupsets.append(backupset)
                        dbname = backupset["DatabaseName"]
                        position = backupset["Position"]
                        if backupset["BackupType"] in (1,):
                            if dbname in backupsets_reduced and position > backupsets_reduced[dbname]["Position"]:
                                self.log.debug("Found backupset {} for the DB \"{}\"".format(position, dbname))
                                backupsets_reduced[dbname] = backupset
                            elif dbname not in backupsets_reduced:
                                self.log.debug("Found backupset {} for the DB \"{}\"".format(position, dbname))
                                backupsets_reduced[dbname] = backupset
                        else:
                            log.debug("Could not process unknown backup type {} of backupset {} for the DB \"{}\"".
                                      format(backupset["BackupType"], position, dbname))
            except Exception:
                self.log.exception("Fetching header failed")
            else:
                self.backupsets = tuple(backupsets)
                self.backupsets_reduced = tuple(backupsets_reduced.values())
            con.close()

    def get_backupsets(self):
        """
        Returns non-redundant list of DB backup sets
        :return: list of DB backup sets
        """
        return self.backupsets_reduced

    def get_all_backupsets(self):
        """
        Returns list of all DB backup sets
        :return: list of DB backup sets
        """
        return self.backupsets


class BackupSet:
    """
    Backupset definition and manipulation
    """

    def __init__(self, filenameset, setinfo, dbc, log):
        """
        Init
        :param filenameset: Backup file name set
        :param setinfo: Backupset header info
        :param dbc: DBConnect obj
        :param log: logger obj
        """
        self.filenameset = filenameset
        self.setinfo = setinfo
        self.setfiles = tuple()
        self.dbc = dbc
        self.log = log
        self.fetch_files()

    def fetch_files(self):
        con = self.dbc.getcon()
        if con is not None:
            try:
                with con.cursor() as cur:
                    cur.execute("RESTORE FILELISTONLY FROM DISK = ? WITH FILE = ?;", (self.filenameset[0], self.get_fileno()))  # One (and any) backup file is enough to get filelist
                    files = []
                    files_columns = tuple(x[0] for x in cur.description)
                    for row in cur.fetchall():
                        file = {x: y for x, y in zip(files_columns, row)}
                        files.append(file)
            except Exception:
                self.log.exception("Fetching file list failed")
            else:
                self.setfiles = tuple(files)
            con.close()

    def get_files(self):
        """
        Returns backup set's files
        :return: Backup set's file list
        """
        return self.setfiles

    def get_fileno(self):
        """
        Returns backup set's file number
        :return: Backup set's file number
        """
        return self.setinfo["Position"]

    def get_dbname(self):
        """
        Returns the DB name the backup set was created for
        :return: DB name
        """
        return self.setinfo["DatabaseName"]


class PathMap:
    """
    Utility to memorize and replace paths
    """

    def __init__(self):
        self.pathmap = dict()
        self.pathmap_saved = dict()

    @staticmethod
    def compare_path(p1, p2):
        return os.path.normcase(os.path.normpath(p1)) == os.path.normcase(os.path.normpath(p2))

    def get_path(self, path, cat="default"):
        if cat not in self.pathmap:
            self.pathmap[cat] = dict()
        return self.pathmap[cat].get(os.path.normcase(os.path.normpath(path)), path)

    def set_path(self, path, pathmap, cat="default"):
        if cat not in self.pathmap:
            self.pathmap[cat] = dict()
        self.pathmap[cat][os.path.normcase(os.path.normpath(path))] = pathmap

    def save_state(self):
        self.pathmap_saved = deepcopy(self.pathmap)

    def restore_state(self):
        self.pathmap = deepcopy(self.pathmap_saved)


stop_restore = False
nointerrupt = False

def restoreDB(dbc, filenameset, log, defaultpath, interactive=False, users=None, replace=False, pm=None, sp=None, names=None, only=None, exclude=None):
    """
    Restores all DBs from file
    :param dbc: DBConnect obj
    :param filenameset: set of absolute! backup file names
    :param log: logger obj
    :param defaultpath: fromdb or fromfile, or the absolute path to a directory
    :param interactive: interactively ask for DB files relocation path
    :param users: list of users to fix permissions for
    :param replace: force replacing existing DB
    :param pm: shared PathMap object. Allows saving state between restoreDB invocations
    :param sp: shared ServerProperties object
    :param names: shared DBSet object (names and metadata of databases found in the SQL instance)
    :param only: a list of DB names, which, if defined, should only be restored. The rest, if exists, should be skipped
    :param exclude: a list of DB names, which, if defined, should be skipped while restoring
    :return: a dict - total and restored DBs
    """
    count = {"total": 0, "restored": 0}
    if not nointerrupt:
        log.info("Prepare the restore from {}".format(", ".join(("\"{}\"".format(x) for x in filenameset))))
        log.debug("Wait for awhile so CTRL+C if pressed could be propagated")
        sleep(1)
    if stop_restore:
        log.warn("User has requested to stop")
        return count
    for filename in filenameset:
        if not os.path.isabs(filename):
            raise RuntimeError("Backup file name is not absolute")
    if defaultpath not in ("fromdb", "fromfile") and not os.path.isabs(defaultpath):
        raise RuntimeError("Restore path is not absolute")
    if not pm:
        pm = PathMap()
    if not sp:
        sp = ServerProperties(dbc, log)
    if not names:
        names = DBSet(dbc, log)
    lc_only = tuple()
    lc_exclude = tuple()
    if only:
        lc_only = tuple(x.lower() for x in only)
    if exclude:
        lc_exclude = tuple(x.lower() for x in exclude)
    for item in BackupFile(filenameset, dbc, log).get_backupsets():
        backupset = BackupSet(filenameset, item, dbc, log)
        lc_dbname = backupset.get_dbname().lower()
        if lc_dbname not in lc_exclude and (len(lc_only) == 0 or lc_dbname in lc_only):
            log.info("Restore DB \"{}\"".format(backupset.get_dbname()))
            files = dict()
            is_files_correct = False
            count["total"] += 1
            while not is_files_correct:
                if interactive:
                    pm.save_state()
                for file in backupset.get_files():
                    if defaultpath == "fromdb":
                        log.debug("Get default file path for {} from DB".format(file["LogicalName"]))
                        physfile = os.path.basename(file["PhysicalName"])
                        if file["Type"] == "L":
                            physfile = os.path.join(sp.InstanceDefaultLogPath, physfile)
                        else:
                            physfile = os.path.join(sp.InstanceDefaultDataPath, physfile)
                    elif defaultpath == "fromfile":
                        log.debug("Get default file path for {} from backup".format(file["LogicalName"]))
                        physfile = file["PhysicalName"]
                    else:
                        log.debug("Default file path for {} is defined by user".format(file["LogicalName"]))
                        physfile = os.path.join(defaultpath, os.path.basename(file["PhysicalName"]))
                    if interactive:
                        pm_cat = "log" if file["Type"] == "L" else "data"
                        fhead, ftail = os.path.split(physfile)
                        relocatedfile = os.path.join(pm.get_path(fhead, pm_cat), ftail)
                        newfile = winput("[restore {} file {} as {}]: ".format("log" if file["Type"] == "L" else "data",
                                                                               ybstr(file["LogicalName"]),
                                                                               ybstr(relocatedfile)))
                        if len(newfile) == 0 or newfile.isspace():
                            newfile = relocatedfile
                        pm.set_path(fhead, os.path.dirname(newfile), pm_cat)
                        files[file["LogicalName"]] = newfile
                    else:
                        files[file["LogicalName"]] = physfile
                if interactive:
                    inprompt = ">>>\n"
                    for k, v in files.items():
                        inprompt += ">>> restore {} as {}\n".format(ybstr(k), ybstr(v))
                    inprompt += ">>> is the above correct? (Y/N): "
                    is_files_correct = winput(inprompt)
                    while len(is_files_correct) == 0 or len(is_files_correct) > 1 or is_files_correct not in "yYnN":
                        is_files_correct = winput(">>> is the above correct? Enter Y or N): ")
                    if is_files_correct in "yY":
                        is_files_correct = True
                    else:
                        is_files_correct = False
                        pm.restore_state()
                else:
                    is_files_correct = True
            log.debug("Restore DB files as follows: {}".format(files))
            con = dbc.getcon()
            if con is not None:
                try:
                    with con.cursor() as cur:
                        for user in users:
                            log.debug("Check if user {} exists".format(user))
                            cur.execute("USE master;")
                            cur.execute("SELECT name FROM sys.server_principals WHERE name = ?;", (user,))
                            if len(cur.fetchall()) < 1:
                                raise RuntimeError("No user {} found. Please create one before assigning permissions".
                                                   format(user))
                        with_move = ""
                        fcount = 0
                        while fcount < len(files):
                            if fcount:
                                with_move += ", MOVE ? TO ?"
                            else:
                                with_move += "MOVE ? TO ?"
                            fcount += 1
                        cur.execute("SELECT name FROM sys.databases WHERE name = ?", (backupset.get_dbname(),))
                        if len(cur.fetchall()):
                            log.debug("Set DB \"{}\" to SINGLE_USER".format(backupset.get_dbname()))
                            cur.execute("ALTER DATABASE {} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;".
                                        format(backupset.get_dbname()))
                            try:
                                log.debug("Perform restore of DB \"{}\"".format(backupset.get_dbname()))
                                disk_str = ", ".join(("DISK = ?",) * len(filenameset))
                                cur.execute("RESTORE DATABASE ? FROM {} WITH {}FILE = ?, ".
                                            format(disk_str, "REPLACE, " if replace else "") + with_move + ";",
                                            (backupset.get_dbname(), *filenameset, backupset.get_fileno(), *(sum(files.items(), ()))))
                                while cur.nextset():
                                    pass
                            finally:
                                log.debug("Set DB \"{}\" to {} explicitly".format(backupset.get_dbname(), names.get_user_access_mode(backupset.get_dbname())))
                                cur.execute("ALTER DATABASE {} SET {} WITH ROLLBACK IMMEDIATE;".
                                            format(backupset.get_dbname(), names.get_user_access_mode(backupset.get_dbname())))
                        for user in users:
                            log.debug("Fix user permissions for user {} on DB {}".format(user, backupset.get_dbname()))
                            cur.execute("USE {};".format(backupset.get_dbname()))
                            cur.execute("SELECT name FROM sys.database_principals WHERE name = ?;", (user,))
                            if len(cur.fetchall()) < 1:
                                log.debug("Create user mapping for user {} in DB {}".format(user, backupset.get_dbname()))
                                cur.execute("CREATE USER [{}] FROM LOGIN [{}];".format(user, user))
                            cur.execute("ALTER USER [{}] WITH LOGIN = [{}];".format(user, user))
                            cur.execute("EXEC sp_addrolemember 'db_owner', ?;", (user,))
                except Exception:
                    log.exception("Restore operation failed")
                    log.info("Failed restoring DB \"{}\"".format(backupset.get_dbname()))
                else:
                    log.info("Successfully restored DB \"{}\"".format(backupset.get_dbname()))
                    count["restored"] += 1
                con.close()
        else:
            log.info("Skip DB \"{}\"".format(backupset.get_dbname()))
    return count


def list_from_str(instr=None, sep=","):
    if instr is not None:
        return list(x.strip() for x in instr.split(sep))
    else:
        return list()


if __name__ == "__main__":
    defaults = {
        "server": r"localhost",
        "workers": (1, 4),  # Default and allowed maximum
        "default_data_log": "fromdb"
    }
    prog_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    cmd = argparse.ArgumentParser(description="Restore DB backups (files with *.bak ext) from the dir specified or "
                                              "from a single file")
    cmd.add_argument("-s", metavar=r"host\instance", help="SQL server name ({})".format(defaults["server"]),
                     default=defaults["server"])
    cmdgroup1 = cmd.add_mutually_exclusive_group(required=True)
    cmdgroup1.add_argument("-d", metavar="path", help="DEPRECATED! KEPT FOR COMPATIBILITY! Absolute path to a backup file or directory with backup files. Multiple -d are allowed", action="append")
    cmdgroup1.add_argument("-f", metavar="path", help="Absolute path to a backup file or directory with backup files. Multiple -f are allowed", action="append")
    cmd.add_argument("-p", metavar="<fromdb | fromfile | abs_path>", default=defaults["default_data_log"],
                     help="Default data/log directory path ({})".format(defaults["default_data_log"]))
    cmd.add_argument("-b", help="Batch mode ({})".format(False), action="store_true", default=False)
    cmd.add_argument("-w", metavar="num",
                     help="Number of workers running in parallel for batch mode (default/max {dw[0]}/{dw[1]})".
                     format(dw=defaults["workers"]), default=defaults["workers"][0], type=int)
    cmd.add_argument("-u", metavar="names", help="Comma-separated list of users to fix permissions for")
    cmd.add_argument("-r", help="Replace existing DB ({})".format(False), action="store_true", default=False)
    cmd.add_argument("-l", metavar="text", help="Distinguishing log file name suffix")
    cmd.add_argument("--only", metavar="names", help="Comma-separated list of DB names that should ONLY be restored")
    cmd.add_argument("--exclude", metavar="names", help="Comma-separated list of DB names that should be EXCLUDED from restore")
    cmd.add_argument("--nointerrupt",
                     help="Nointerrupt mode (slightly faster than regular, Ctrl+C is not supposed to be pressed) ({})".format(False),
                     action="store_true", default=False)
    cmd.add_argument("--logtofile", help="Enable log to file ({})".format(False), action="store_true", default=False)
    cmd.add_argument("--logtostdout", help="Log to stdout instead of stderr ({})".format(False),
                     action="store_true", default=False)
    cmd.add_argument("-v", action="version", version=_VERSION)
    cmdargs = cmd.parse_args()
    log = getLogger(cmdargs.l, cmdargs.logtofile, cmdargs.logtostdout)
    bakfiles = []
    if cmdargs.d or cmdargs.f:
        if cmdargs.d:
            log.warning("The -d argument is deprecated! Please use -f for both file and/or directory")
            cmdargs.f = cmdargs.d
        for f in cmdargs.f:
            if not os.path.isabs(f):
                log.critical("The path to the backup file/directory \"{}\" is not absolute".format(f))
                exit(1)
            if os.path.isdir(f):
                bakfiles.extend(glob(os.path.join(f, "*.bak")))
            elif os.path.isfile(f):
                bakfiles.append(f)
            else:
                log.critical("Unsupported backup file/directory \"{}\"".format(f))
                exit(1)
    cmdargs.u = list_from_str(cmdargs.u)
    cmdargs.only = list_from_str(cmdargs.only)
    cmdargs.exclude = list_from_str(cmdargs.exclude)
    dbc = DBConnect(cmdargs.s, prog_name, log)
    log.info("Check DB connection")
    if dbc.getcon() is None:
        log.critical("Could not connect to the DB server \"{}\"".format(cmdargs.s))
        exit(1)
    else:
        cmdargs.w = cmdargs.w if cmdargs.b else 1
        log.debug("Restore databases using no more than {} worker(s)".format(min(cmdargs.w, defaults["workers"][1])))
        interactive = False if cmdargs.b else True
        pm = PathMap()
        sp = ServerProperties(dbc, log)
        names = DBSet(dbc, log)
        mediasets = MediaSets(dbc, log)
        for f in bakfiles:
            mediasets.add_file(f)
        filesets = mediasets.get_filesets()
        log.debug("Got backup filesets {}".format(filesets))
        if cmdargs.nointerrupt:
            nointerrupt = True
        with ThreadPoolExecutor(max_workers=min(cmdargs.w, defaults["workers"][1])) as executor:
            try:
                counts = list(count for count in executor.map(restoreDB,
                                                              (dbc,) * len(filesets),
                                                              filesets,
                                                              (log,) * len(filesets),
                                                              (cmdargs.p,) * len(filesets),
                                                              (interactive,) * len(filesets),
                                                              (cmdargs.u,) * len(filesets),
                                                              (cmdargs.r,) * len(filesets),
                                                              (pm,) * len(filesets),
                                                              (sp,) * len(filesets),
                                                              (names,) * len(filesets),
                                                              (cmdargs.only,) * len(filesets),
                                                              (cmdargs.exclude,) * len(filesets)))
            except KeyboardInterrupt:
                stop_restore = True
                exit(1)
        total = sum(x["total"] for x in counts)
        restored = sum(x["restored"] for x in counts)
        exit_status = 0
        if restored < total:
            chosen_log = log.warning
            exit_status = 1
        else:
            chosen_log = log.info
        chosen_log("Restore complete. {}/{} (successful/total)".format(restored, total))
        if restored < len(filesets) and not cmdargs.only and not cmdargs.exclude and exit_status == 0:
            log.warning("Number of restored DBs is less than total backup filesets. That's suspicious")
            exit_status = 1
        exit(exit_status)
