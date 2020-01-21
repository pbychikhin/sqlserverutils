
import sys
import os.path
import argparse
from glob import glob
from sys import exit
from copy import deepcopy
from colorama import init as color_init, Fore, Style
from backupmany import getLogger, DBConnect
from concurrent.futures import ThreadPoolExecutor

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


class BackupFile:
    """
    Backup file definition and manipulation
    """

    def __init__(self, name, dbc, log):
        """
        Init
        :param name: Backup file name
        :param dbc: DBConnect obj
        :param log: logger obj
        """
        self.name = name
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
                    cur.execute("RESTORE HEADERONLY FROM DISK = ?;", (self.name))
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

    def __init__(self, filename, setinfo, dbc, log):
        """
        Init
        :param filename: Backup file name
        :param setinfo: Backupset header info
        :param dbc: DBConnect obj
        :param log: logger obj
        """
        self.filename = filename
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
                    cur.execute("RESTORE FILELISTONLY FROM DISK = ? WITH FILE = ?;", (self.filename, self.get_fileno()))
                    files = []
                    files_columns = tuple(x[0] for x in cur.description)
                    for row in cur.fetchall():
                        file = {x: y for x, y in zip(files_columns, row)}
                        files.append(file)
            except Exception:
                self.log.exception("Fetching file list failed")
            else:
                self.setfiles = tuple(files)

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

    def get_path(self, path):
        return self.pathmap.get(os.path.normcase(os.path.normpath(path)), path)

    def set_path(self, path, pathmap):
        self.pathmap[os.path.normcase(os.path.normpath(path))] = pathmap

    def save_state(self):
        self.pathmap_saved = deepcopy(self.pathmap)

    def restore_state(self):
        self.pathmap = deepcopy(self.pathmap_saved)


def restoreDB(dbc, backupfile, log, interactive=False, user=None, replace=False, pm=None):
    """
    Restores all DBs from file
    :param dbc: DBConnect obj
    :param backupfile: absolute! backup file name
    :param log: logger obj
    :param interactive: interactively ask for DB files relocation path
    :param user: user to fix permissions for
    :param replace: force replacing existing DB
    :param pm: shared PathMap object. Allows saving state between restoreDB invocations
    :return: a dict - total and restored DBs
    """
    if not os.path.isabs(backupfile):
        raise RuntimeError("Backup file name is not absolute")
    if not pm:
        pm = PathMap()
    count = {"total": 0, "restored": 0}
    for item in BackupFile(backupfile, dbc, log).get_backupsets():
        backupset = BackupSet(backupfile, item, dbc, log)
        log.info("Restoring DB \"{}\"".format(backupset.get_dbname()))
        files = dict()
        is_files_correct = False
        count["total"] += 1
        while not is_files_correct:
            if interactive:
                pm.save_state()
            for file in backupset.get_files():
                if interactive:
                    fhead, ftail = os.path.split(file["PhysicalName"])
                    relocatedfile = os.path.join(pm.get_path(fhead), ftail)
                    newfile = winput("[restore {} as {}]: ".format(ybstr(file["LogicalName"]), ybstr(relocatedfile)))
                    if len(newfile) == 0 or newfile.isspace():
                        newfile = relocatedfile
                    pm.set_path(fhead, os.path.dirname(newfile))
                    files[file["LogicalName"]] = newfile
                else:
                    files[file["LogicalName"]] = file["PhysicalName"]
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
        log.debug("Restoring DB files as follows: {}".format(files))
        con = dbc.getcon()
        if con is not None:
            try:
                with con.cursor() as cur:
                    if user:
                        log.debug("Checking if user {} exists".format(user))
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
                        cur.execute("ALTER DATABASE {} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;".
                                    format(backupset.get_dbname()))
                    cur.execute("RESTORE DATABASE ? FROM DISK = ? WITH {}FILE = ?, ".
                                format("REPLACE, " if replace else "") + with_move + ";",
                                (backupset.get_dbname(), backupfile, backupset.get_fileno(), *(sum(files.items(), ()))))
                    while cur.nextset():
                        pass
                    if user:
                        log.debug("Fixing user permissions for user {} on DB {}".format(user, backupset.get_dbname()))
                        cur.execute("USE {};".format(backupset.get_dbname()))
                        cur.execute("SELECT name FROM sys.database_principals WHERE name = ?;", (user,))
                        if len(cur.fetchall()) < 1:
                            log.debug("Creating user mapping for user {} in DB {}".format(user, backupset.get_dbname()))
                            cur.execute("CREATE USER {} FROM LOGIN {};".format(user, user))
                        cur.execute("ALTER USER {} WITH LOGIN = {};".format(user, user))
                        cur.execute("EXEC sp_addrolemember 'db_owner', ?;", (user,))
            except Exception:
                log.exception("Restore operation failed")
                log.info("Failed restoring DB \"{}\"".format(backupset.get_dbname()))
            else:
                log.info("Successfully restored DB \"{}\"".format(backupset.get_dbname()))
                count["restored"] += 1
    return count


if __name__ == "__main__":
    defaults = {
        "server": r"localhost\MSSQLSERVER",
        "workers": (1, 4)  # Default and allowed maximum
    }
    prog_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    cmd = argparse.ArgumentParser(description="Restores DB backups (files with *.bak ext) from the dir specified or "
                                              "from a single file")
    cmd.add_argument("-s", metavar=r"host\instance", help="SQL server name ({})".format(defaults["server"]),
                     default=defaults["server"])
    cmdgroup1 = cmd.add_mutually_exclusive_group(required=True)
    cmdgroup1.add_argument("-d", metavar="path", help="Absolute path to the directory with backups")
    cmdgroup1.add_argument("-f", metavar="path", help="Absolute path to the backup file")
    cmd.add_argument("-b", help="Batch mode ({})".format(False), action="store_true", default=False)
    cmd.add_argument("-w", metavar="num", help="Max. workers running in parallel for batch mode({})".
                     format(defaults["workers"]), default=defaults["workers"][0], type=int)
    cmd.add_argument("-u", metavar="name", help="Fix user permissions after restore for the specified user")
    cmd.add_argument("-r", help="Replace existing DB ({})".format(False), action="store_true", default=False)
    cmd.add_argument("-l", metavar="text", help="Distinguishing log file name suffix")
    cmdargs = cmd.parse_args()
    log = getLogger(cmdargs.l)
    for path in (cmdargs.d, cmdargs.f):
        if path is not None and not os.path.isabs(path):
            log.critical("The path to the backup file or directory is not absolute")
            exit(1)
    if cmdargs.d is not None:
        bakfiles = glob(os.path.join(cmdargs.d, "*.bak"))
    else:
        bakfiles = (cmdargs.f,)
    dbc = DBConnect(cmdargs.s, prog_name, log)
    cmdargs.w = cmdargs.w if cmdargs.b else 1
    log.debug("Restoring databases using no more than {} worker(s)".format(min(cmdargs.w, defaults["workers"][1])))
    interactive = False if cmdargs.b else True
    pm = PathMap()
    with ThreadPoolExecutor(max_workers=min(cmdargs.w, defaults["workers"][1])) as executor:
        counts = list(count for count in executor.map(restoreDB,
                                                      (dbc,) * len(bakfiles),
                                                      bakfiles,
                                                      (log,) * len(bakfiles),
                                                      (interactive,) * len(bakfiles),
                                                      (cmdargs.u, ) * len(bakfiles),
                                                      (cmdargs.r, ) * len(bakfiles),
                                                      (pm, ) * len(bakfiles)))
    total = sum(x["total"] for x in counts)
    restored = sum(x["restored"] for x in counts)
    if restored < total:
        chosen_log = log.warning
    else:
        chosen_log = log.info
    chosen_log("Restore complete. {}/{} (successful/total)".format(restored, total))
