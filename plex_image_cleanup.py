import glob, os, shutil, sqlite3, sys, time
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from datetime import datetime
from urllib.parse import quote

try:
    import plexapi, requests
    from num2words import num2words
    from pmmutils import args, logging, schedule, util
    from plexapi.exceptions import Unauthorized
    from plexapi.server import PlexServer
    from pmmutils.args import PMMArgs
    from pmmutils.exceptions import Continue, Failed
    from retrying import retry
    from tqdm import tqdm
except (ModuleNotFoundError, ImportError):
    print("Requirements Error: Requirements are not installed")
    sys.exit(0)

if sys.version_info[0] != 3 or sys.version_info[1] < 10:
    print("Version Error: Version: %s.%s.%s incompatible please use Python 3.10+" % (sys.version_info[0], sys.version_info[1], sys.version_info[2]))
    sys.exit(0)

def not_failed(exception):
    return not isinstance(exception, Failed)

modes = {
    "nothing": {
        "ed": "", "ing": "", "space": "",
        "desc": "Does not process any files in the Metadata Directory."
    },
    "clear": {
        "ed": "Cleared", "ing": "Clearing", "space": "Space Recovered",
        "desc": "Clears out the PIC Restore Directory. (CANNOT BE RESTORED)"
    },
    "report": {
        "ed": "Reported", "ing": "Reporting", "space": "Potential Recovery",
        "desc": "File changes will be reported but not done."
    },
    "move": {
        "ed": "Moved", "ing": "Moving", "space": "Potential Recovery",
        "desc": "Files will be moved from the Metadata Directory. (CAN BE RESTORED)"
    },
    "restore": {
        "ed": "Restored", "ing": "Restoring", "space": "",
        "desc": "Restores Renamed Bloat Images."
    },
    "remove": {
        "ed": "Removed", "ing": "Removing", "space": "Space Recovered",
        "desc": "Files will be removed from the Metadata Directory. (CANNOT BE RESTORED)"
    }
}
mode_descriptions = '\n\t'.join([f"{m}: {d}" for m, d in modes.items()])
sc_options = ["mode", "transcode", "empty-trash", "clean-bundles", "optimize-db"]
options = [
    {"arg": "p",  "key": "plex",             "env": "PLEX_PATH",        "type": "str",  "default": None,     "help": "Path to the Plex Install Folder (Contains Folders: Cache, Metadata, Plug-in Support)."},
    {"arg": "m",  "key": "mode",             "env": "MODE",             "type": "str",  "default": "report", "help": f"Run Mode ({', '.join(modes)}). (Default: report)"},
    {"arg": "sc", "key": "schedule",         "env": "SCHEDULE",         "type": "str",  "default": None,     "help": "Schedule to run in continuous mode."},
    {"arg": "u",  "key": "url",              "env": "PLEX_URL",         "type": "str",  "default": None,     "help": "Plex URL of the Server you want to connect to."},
    {"arg": "t",  "key": "token",            "env": "PLEX_TOKEN",       "type": "str",  "default": None,     "help": "Plex Token of the Server you want to connect to."},
    {"arg": "di", "key": "discord",          "env": "DISCORD",          "type": "str",  "default": None,     "help": "Webhook URL to channel for Notifications."},
    {"arg": "ti", "key": "timeout",          "env": "TIMEOUT",          "type": "int",  "default": 600,      "help": "Timeout can be any number greater then 0. (Default: 600)"},
    {"arg": "s",  "key": "sleep",            "env": "SLEEP",            "type": "int",  "default": 60,       "help": "Sleep Timer between Empty Trash, Clean Bundles, and Optimize DB. (Default: 60)"},
    {"arg": "i",  "key": "ignore",           "env": "IGNORE_RUNNING",   "type": "bool", "default": False,    "help": "Ignore Warnings the Plex is currently Running."},
    {"arg": "l",  "key": "local",            "env": "LOCAL_DB",         "type": "bool", "default": False,    "help": "Copy Local DB instead of Downloading from the API (Helps with Large DBs)."},
    {"arg": "e",  "key": "existing",         "env": "USE_EXISTING",     "type": "bool", "default": False,    "help": "Use the existing database if less then 2 hours old."},
    {"arg": "ct", "key": "transcode",        "env": "CLEAN_TRANSCODE",  "type": "bool", "default": False,    "help": "Clean Plex's PhotoTranscoder Directory."},
    {"arg": "et", "key": "empty-trash",      "env": "EMPTY_TRASH",      "type": "bool", "default": False,    "help": "Run Plex's Empty Trash Operation."},
    {"arg": "cb", "key": "clean-bundles",    "env": "CLEAN_BUNDLES",    "type": "bool", "default": False,    "help": "Run Plex's Clean Bundles Operation."},
    {"arg": "od", "key": "optimize-db",      "env": "OPTIMIZE_DB",      "type": "bool", "default": False,    "help": "Run Plex's Optimize DB Operation."},
    {"arg": "tr", "key": "trace",            "env": "TRACE",            "type": "bool", "default": False,    "help": "Run with every request logged."}
]
script_name = "Plex Image Cleanup"
plex_db_name = "com.plexapp.plugins.library.db"
metadata_folders = ["Movies", "TV Shows", "Playlists", "Collections", "Artists", "Albums"]
base_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.join(base_dir, "config")
pmmargs = PMMArgs("meisnate12/Plex-Image-Cleanup", os.path.dirname(os.path.abspath(__file__)), options, use_nightly=False)
secrets = [pmmargs["url"], pmmargs["token"], quote(str(pmmargs["url"])), requests.utils.urlparse(pmmargs["url"]).netloc]
logger = logging.PMMLogger(script_name, "plex_image_cleanup", os.path.join(config_dir, "logs"), discord_url=pmmargs["discord"], log_requests=pmmargs["trace"])
requests.Session.send = util.update_send(requests.Session.send, pmmargs["timeout"])
plexapi.BASE_HEADERS["X-Plex-Client-Identifier"] = pmmargs.uuid

def pic_thread(attrs):
    with ProcessPoolExecutor(max_workers=1) as executor:
        executor.submit(run_plex_image_cleanup, *[attrs])

def run_plex_image_cleanup(attrs):
    logger.header(pmmargs, sub=True, discord_update=True)
    logger.separator()
    do_transcode = attrs["transcode"] if "transcode" in attrs else pmmargs["transcode"]
    do_trash = attrs["empty-trash"] if "empty-trash" in attrs else pmmargs["empty-trash"]
    do_bundles = attrs["clean-bundles"] if "clean-bundles" in attrs else pmmargs["clean-bundles"]
    do_optimize = attrs["optimize-db"] if "optimize-db" in attrs else pmmargs["optimize-db"]
    report = []
    messages = []
    try:
        logger.info("Validating Options")
        try:
            logger.info("Script Started", log=False, discord=True)
        except Failed as e:
            logger.error(e)

        # Check Plex Path
        if not pmmargs["plex"]:
            if not os.path.exists(os.path.join(base_dir, "plex")):
                raise Failed("Args Error: No Plex Path Provided")
            logger.warning(f"No Plex Path Provided. Using default: {os.path.join(base_dir, 'plex')}")
            pmmargs["plex"] = os.path.join(base_dir, "plex")
        pmmargs["plex"] = os.path.abspath(pmmargs["plex"])
        transcoder_dir = os.path.join(pmmargs["plex"], "Cache", "PhotoTranscoder")
        databases_dir = os.path.join(pmmargs["plex"], "Plug-in Support", "Databases")
        meta_dir = os.path.join(pmmargs["plex"], "Metadata")
        restore_dir = os.path.join(pmmargs["plex"], "PIC Restore")
        if not os.path.exists(pmmargs["plex"]):
            raise Failed(f"Folder Error: Plex Databases Folder Path Not Found: {os.path.abspath(pmmargs['plex'])}")
        elif not os.path.exists(transcoder_dir) or not os.path.exists(databases_dir) or not os.path.exists(meta_dir):
            contents = "\n                  ".join(os.listdir(pmmargs["plex"]))
            raise Failed(f'Folder Error: Plex Databases Folder Path: {pmmargs["plex"]}\n'
                         f'              Should contain "Cache", "Metadata", and "Plug-in Support"\n'
                         f'              Contents:\n                  {contents}')

        # Check Mode
        mode = attrs["transcode"].lower() if "transcode" in attrs else pmmargs["mode"].lower()
        if mode not in modes:
            raise Failed(f"Mode Error: {mode} Invalid. Options: \n\t{mode_descriptions}")

        # Delete PhotoTranscoder
        if do_transcode:
            logger.separator(f"Remove PhotoTranscoder Images\nDir: {transcoder_dir}")

            head = logger.info("Scanning for PhotoTranscoder Images", start="transcode_scan")
            transcode_images = [f for f in tqdm(glob.iglob(os.path.join(transcoder_dir, "**", "*.*"), recursive=True), unit=" images", desc=f"| {head}")]
            logger.info(f"Scanning Complete: Found {len(transcode_images)} PhotoTranscoder Images to Remove")
            logger.info(f"Runtime: {logger.runtime()}")
            logger.info()

            head = logger.info("Removing PhotoTranscoder Images", start="transcode")
            logger["size"] = 0
            messages = []
            for f in tqdm(transcode_images, unit=" removed", desc=f"| {head}"):
                file = os.path.join(transcoder_dir, f)
                messages.append(f"REMOVE: {file}")
                logger["size"] += os.path.getsize(file)
                if mode != "report":
                    os.remove(file)
            for message in messages:
                logger.trace(message)

            logger.info(f"Remove Complete: Removed {len(transcode_images)} PhotoTranscoder Images")
            space = util.format_bytes(logger["size"])
            logger.info(f"Space Recovered: {space}")
            logger.info(f"Runtime: {logger.runtime()}")
            report.append([("Remove PhotoTranscoder Images", "")])
            report.append([
                ("Space Recovered", space),
                ("Files Removed", len(transcode_images)),
                ("Scan Time", f"{logger.runtime('transcode_scan')}"),
                ("Remove Time", f"{logger.runtime('transcode')}")
            ])

        # Connection to Plex
        server = None
        def get_server():
            logger.info("Connecting To Plex")
            if not pmmargs["url"]:
                raise Failed("Args Error: No Plex URL Provided")
            if not pmmargs["token"]:
                raise Failed("Args Error: No Plex Token Provided")
            plexapi.server.TIMEOUT = pmmargs["timeout"]
            os.environ["PLEXAPI_PLEXAPI_TIMEOUT"] = str(pmmargs["timeout"])

            @retry(stop_max_attempt_number=5, wait_incrementing_start=60000, wait_incrementing_increment=60000, retry_on_exception=not_failed)
            def plex_connect():
                try:
                    return PlexServer(pmmargs["url"], pmmargs["token"], timeout=pmmargs["timeout"])
                except Unauthorized:
                    raise Failed("Plex Error: Plex token is invalid")
                except Exception as e1:
                    logger.error(e1)
                    raise

            return plex_connect()

        if mode != "nothing":
            logger.info(f"Running in {mode} mode. {modes[mode]['desc']}")

            if os.path.exists(restore_dir):
                match mode:
                    case "restore":
                        logger.separator("Restore Renamed Bloat Images")

                        logger.info("Scanning for Renamed Bloat Images to Restore", start="scanning")
                        restore_images = [f for f in tqdm(glob.iglob(os.path.join(restore_dir, "**", "*.jpg"), recursive=True), unit=" image", desc="| Scanning for Renamed Bloat Images to Restore")]
                        logger.info(f"Scanning Complete: Found {len(restore_images)} Renamed Bloat Images to Restore")
                        logger.info(f"Runtime: {logger.runtime()}")
                        logger.info()

                        logger.info("Restoring Renamed Bloat Images", start="work")
                        for path in tqdm(restore_images, unit=" restored", desc="| Restoring Renamed Bloat Images"):
                            messages.append(f"RENAME: {path}\n  ----> {os.path.join(meta_dir, path.removeprefix(restore_dir)[1:]).removesuffix('.jpg')}\n")
                            util.move_path(path, restore_dir, meta_dir, suffix='.jpg', append=False)
                        for message in messages:
                            logger.trace(message)
                        messages = []
                        logger.info(f"Restore Complete: Restored {len(restore_images)} Renamed Bloat Images")
                        logger.info(f"Runtime: {logger.runtime()}")
                        report.append([("Restore Renamed Bloat Images", "")])
                        report.append([("Scan Time", f"{logger.runtime('scanning')}"), ("Restore Time", f"{logger.runtime('work')}")])
                    case "clear":
                        logger.separator("Removing PIC Restore Folder")

                        logger.info("Scanning PIC Restore for Bloat Images to Remove", start="scanning")
                        del_paths = [os.path.join(r, f) for r, d, fs in tqdm(os.walk(restore_dir), unit=" directories", desc="| Scanning PIC Restore for Bloat Images to Remove") for f in fs]
                        logger.info(f"Scanning Complete: Found {len(del_paths)} Bloat Images in the PIC Folder to Remove")
                        logger.info(f"Runtime: {logger.runtime()}")
                        logger.info()

                        messages = []
                        logger.info("Removing PIC Restore Bloat Images", start="work")
                        logger["size"] = 0
                        for path in tqdm(del_paths, unit=" removed", desc="| Removing PIC Restore Bloat Images"):
                            messages.append(f"REMOVE: {path}")
                            logger["size"] += os.path.getsize(path)
                            os.remove(path)
                        shutil.rmtree(restore_dir)
                        for message in messages:
                            logger.trace(message)
                        logger.info(f"Removing Complete: Removed {len(del_paths)} PIC Restore Bloat Images")
                        space = util.format_bytes(logger["size"])
                        logger.info(f"Space Recovered: {space}")
                        logger.info(f"Runtime: {logger.runtime()}")
                        report.append([("Removing PIC Restore Bloat Images", "")])
                        report.append([
                            ("Space Recovered", space),
                            ("Files Removed", len(del_paths)),
                            ("Scan Time", f"{logger.runtime('scanning')}"),
                            ("Restore Time", f"{logger.runtime('work')}")
                        ])
                    case _:
                        logger.error(f"{mode} mode invalid while the PIC Restore Folder exists.", discord=True, rows=[
                            [("PIC Path", restore_dir)],
                            [("Mode Options", "Mode: restore (Restore the bloat images back into Plex)\nMode: remove (Remove the bloat images)")]
                        ])
                        logger.error(f"PIC Path: {restore_dir}\n"
                                     f"Mode Options:\n"
                                     f"    Mode: restore (Restore the bloat images back into Plex)\n"
                                     f"    Mode: remove (Remove the bloat images)")
                raise Continue

            if mode == ["restore", "clear"]:
                raise Failed(f"Restore Failed: PIC Restore Folder does not exist: {restore_dir}")

            # Check if Running
            if not pmmargs["url"] and not pmmargs["token"]:
                pmmargs["local"] = True
                logger.warning("No Plex URL and Plex Token Given assuming Local Run")
            if pmmargs["local"]:
                db_tmp01 = os.path.join(pmmargs["database"], f"{plex_db_name}-shm")
                db_tmp02 = os.path.join(pmmargs["database"], f"{plex_db_name}-wal")
                if os.path.exists(db_tmp01) or os.path.exists(db_tmp02):
                    temp_db_warning = "At least one of the SQLite temp files is next to the Plex DB; this indicates Plex is still running\n" \
                                      "and copying the DB carries a small risk of data loss as the temp files may not have updated the\n" \
                                      "main DB yet.\n" \
                                      "If you restarted Plex just before running Plex Image Cleanup, and are still getting this error, it\n" \
                                      "can be ignored by using `--ignore` or setting `IGNORE_RUNNING=True` in the .env file."
                    if not pmmargs["ignore"]:
                        raise Failed(temp_db_warning)
                    logger.info(temp_db_warning)
                    logger.info("Warning Ignored")
            else:
                server = get_server()

            # Download DB
            logger.separator("Database")
            dbpath = os.path.join(config_dir, plex_db_name)
            temp_dir = os.path.join(config_dir, "temp")

            is_usable = False
            if pmmargs["existing"]:
                if os.path.exists(dbpath):
                    is_usable, time_ago = util.in_the_last(dbpath, hours=2)
                    if is_usable:
                        logger.info(f"Using existing database (age: {time_ago})")
                    else:
                        logger.info(f"Existing database too old to use (age: {time_ago})")
                else:
                    logger.warning(f"Existing Database not found {'making' if pmmargs['local'] else 'downloading'} a new copy")

            fields = []
            if is_usable:
                report.append([("Plex Database", "Using Existing Database")])
            else:
                report.append([("Plex Database", f"{'Copied' if pmmargs['local'] else 'Downloaded'} New Database")])
                if os.path.exists(dbpath):
                    os.remove(dbpath)
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                os.makedirs(temp_dir)
                if pmmargs["local"]:
                    logger.info(f"Copying database from {os.path.join(databases_dir, plex_db_name)}", start="database")
                    util.copy_with_progress(os.path.join(databases_dir, plex_db_name), dbpath, description=f"Copying database file to: {dbpath}")
                else:
                    logger.info("Downloading Database via the API. Plex will This will take some time... To see progress, log into Plex and\n"
                                "go to Settings | Manage | Console and filter on Database. You can also look at the Plex Dashboard\n"
                                "to see the progress of the Database backup.", start="database")
                    plexapi.utils.download(server.url('/diagnostics/databases'), server._token, savepath=temp_dir, session=server._session, unpack=True, showstatus=True)
                    if backup_file := next((o for o in os.listdir(temp_dir) if str(o).startswith("databaseBackup")), None):
                        shutil.move(os.path.join(temp_dir, backup_file), dbpath)
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                if not os.path.exists(dbpath):
                    raise Failed(f"File Error: Database File Could not {'Copied' if pmmargs['local'] else 'Downloaded'}")
                logger.info(f"Plex Database {'Copy' if pmmargs['local'] else 'Download'} Complete")
                logger.info(f"Database {'Copied' if pmmargs['local'] else 'Downloaded'} to: {dbpath}")
                logger.info(f"Runtime: {logger.runtime()}")
                fields.append(("Copied" if pmmargs["local"] else "Downloaded", f"{logger.runtime('database')}"))

            # Query DB
            urls = []
            with sqlite3.connect(dbpath) as connection:
                logger.info()
                logger.info("Database Opened Querying For In-Use Images", start="query")
                connection.row_factory = sqlite3.Row
                with closing(connection.cursor()) as cursor:
                    for field in ["user_thumb_url", "user_art_url", "user_banner_url"]:
                        cursor.execute(f"SELECT {field} AS url FROM metadata_items WHERE {field} like 'upload://%' OR {field} like 'metadata://%'")
                        urls.extend([requests.utils.urlparse(r["url"]).path[1:] for r in cursor.fetchall() if r and r["url"]])
                logger.info(f"{len(urls)} In-Use Images Found")
                logger.info(f"Runtime: {logger.runtime()}")
                fields.append(("Query", f"{logger.runtime('query')}"))

            report.append(fields)

            # Scan for Bloat Images
            logger.separator(f"{modes[mode]['ing']} Bloat Images")
            logger.info(f"Scanning Metadata Directory For Bloat Images: {meta_dir}", start="scanning")
            bloat_paths = [
                os.path.join(r, f) for r, d, fs in tqdm(os.walk(meta_dir), unit=" directories", desc="| Scanning Metadata for Bloat Images") for f in fs
                if 'Contents' not in r and "." not in f and f not in urls
            ]
            logger.info(f"{len(bloat_paths)} Bloat Images Found")
            logger.info(f"Runtime: {logger.runtime()}")

            # Work on Bloat Images
            if bloat_paths:
                logger.info()
                logger.info(f"{modes[mode]['ing']} Bloat Images", start="work")
                logger["size"] = 0
                messages = []
                for path in tqdm(bloat_paths, unit=f" {modes[mode]['ed'].lower()}", desc=f"| {modes[mode]['ing']} Bloat Images"):
                    logger["size"] += os.path.getsize(path)
                    if mode == "move":
                        messages.append(f"MOVE: {path} --> {os.path.join(restore_dir, path.removeprefix(meta_dir)[1:])}.jpg")
                        util.move_path(path, meta_dir, restore_dir, suffix=".jpg")
                    elif mode == "remove":
                        messages.append(f"REMOVE: {path}")
                        os.remove(path)
                    else:
                        messages.append(f"BLOAT FILE: {path}")
                for message in messages:
                    if mode == "report":
                        logger.debug(message)
                    else:
                        logger.trace(message)
                logger.info(f"Removing Complete: Removed {len(bloat_paths)} Bloat Images")
                space = util.format_bytes(logger["size"])
                logger.info(f"Space Recovered: {space}")
                logger.info(f"Runtime: {logger.runtime()}")
                report.append([("Removing Bloat Images", "")])
                report.append([
                    (modes[mode]["space"], space),
                    (f"Files {modes[mode]['ed']}", len(bloat_paths)),
                    ("Scan Time", f"{logger.runtime('scanning')}"),
                    (f"{mode.capitalize()} Time", f"{logger.runtime('work')}")
                ])
        elif do_trash or do_bundles or do_optimize:
            server = get_server()

        # Plex Operations
        for arg, arg_check, title, op in [
            ("empty-trash", do_trash, "Empty Trash", "emptyTrash"),
            ("clean-bundles", do_bundles, "Clean Bundles", "cleanBundles"),
            ("optimize-db", do_optimize, "Optimize DB", "optimize")
        ]:
            if arg_check:
                if server:
                    logger.separator()
                    logger.info(title)
                    getattr(server.library, op)()
                    for _ in tqdm(range(pmmargs["sleep"]), desc=f"Sleeping for {pmmargs['sleep']} seconds", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]"):
                        time.sleep(1)
                else:
                    logger.error(f"Plex Error: {title} requires a connection to Plex")

    except Continue:
        pass
    except Failed as e:
        logger.separator()
        logger.critical(e, discord=True)
        logger.separator()
    except Exception as e:
        for message in messages:
            logger.debug(message)
        logger.stacktrace()
        logger.critical(e, discord=True)

    logger.error_report()
    logger.separator()
    logger.info(f"{script_name} Finished\nRun Time: {logger.runtime()}", discord=True)
    logger.separator()

if __name__ == "__main__":
    try:
        if pmmargs["schedule"]:
            pmmargs["schedule"] = pmmargs["schedule"].lower()
            valid_sc = []
            schedules = pmmargs["schedule"].split(",")
            print()
            print("Scheduled Runs: ")
            for sc in schedules:
                run_str = ""
                parts = sc.split("|")
                if 1 < len(parts) < 4:
                    opts = None
                    if len(parts) == 2:
                        time_to_run, frequency = parts
                    else:
                        time_to_run, frequency, opts = parts
                    try:
                        datetime.strftime(datetime.strptime(time_to_run, "%H:%M"), "%H:%M")
                    except ValueError:
                        if time_to_run:
                            raise Failed(f'"Schedule Error: Invalid Time: {time_to_run}\nTime must be in the "HH:MM" format between 00:00-23:59"')
                        else:
                            raise Failed(f"Schedule Error: blank time argument")

                    options = {}
                    if opts:
                        for opt in opts.split(";"):
                            try:
                                k, v = opt.split('=')
                            except ValueError:
                                raise Failed(f'Schedule Error: Invalid Options: {opt}\nEach semicolon separated option must be in the "key=value" format')
                            if k not in sc_options:
                                keys = '", "'.join(sc_options[:-1])
                                raise Failed(f'Schedule Error: Invalid Key: {k}\nValid keys: "{keys}", and "{sc_options[-1]}"')
                            if k == "mode":
                                final = v
                                if final not in modes:
                                    raise Failed(f"Mode Error: {v} Invalid. Options: \n\t{mode_descriptions}")
                            else:
                                final = args.parse_bool(v)
                                if final is None:
                                    raise Failed(f'"{k.capitalize()} Error: {v} must be either "True" or "False""')
                            options[k] = final

                    frequency.lower()
                    if frequency == "daily":
                        run_str += "Daily"
                        schedule.every().day.at(time_to_run).do(pic_thread, options)
                    elif frequency.startswith("weekly(") and frequency.endswith(")"):
                        weekday = frequency[7:-1]
                        run_str += f"Weekly on {weekday.capitalize()}s"
                        match weekday:
                            case "sunday":
                                schedule.every().sunday.at(time_to_run).do(pic_thread, options)
                            case "monday":
                                schedule.every().monday.at(time_to_run).do(pic_thread, options)
                            case "tuesday":
                                schedule.every().tuesday.at(time_to_run).do(pic_thread, options)
                            case "wednesday":
                                schedule.every().wednesday.at(time_to_run).do(pic_thread, options)
                            case "thursday":
                                schedule.every().thursday.at(time_to_run).do(pic_thread, options)
                            case "friday":
                                schedule.every().friday.at(time_to_run).do(pic_thread, options)
                            case "saturday":
                                schedule.every().saturday.at(time_to_run).do(pic_thread, options)
                            case _:
                                raise Failed(f"Schedule Error: Invalid Weekly Frequency: {frequency}\nValue must a weekday")
                    elif frequency.startswith("monthly(") and frequency.endswith(")"):
                        try:
                            day = int(frequency[8:-1])
                            run_str += f"Monthly on the {num2words(day, to='ordinal_num')}"
                            if 0 < day < 32:
                                schedule.every().month_on(day).at(time_to_run).do(pic_thread, options)
                            else:
                                raise ValueError
                        except ValueError:
                            raise Failed(f"Schedule Error: Invalid Monthly Frequency: {frequency}\nValue must be between 1-31")

                    run_str += f" at {time_to_run}"
                    if options:
                        run_str += f" (Options: {'; '.join([f'{k}={v}' for k, v in options.items()])})"
                    print(run_str)
                else:
                    raise Failed(f'Schedule Error: Invalid Schedule: {sc}\nEach Schedule must be in either the "time|frequency" or "time|frequency|options" format')

            print()
            while True:
                schedule.run_pending()
                next_run = schedule.next_run()
                time_now = datetime.now()
                time_remaining = next_run - time_now

                td_sec = time_remaining.seconds
                hour_count, rem = divmod(td_sec, 3600)
                minute_count, second_count = divmod(rem, 60)
                remaining_str = ""
                if time_remaining.days > 0:
                    remaining_str += f"{time_remaining.days} day{'s' if hour_count > 1 else ''}"
                if hour_count > 0:
                    remaining_str += f"{', 'if remaining_str else ''}{hour_count} hour{'s' if hour_count > 1 else ''}"
                if minute_count > 0:
                    remaining_str += f"{', 'if remaining_str else ''}{minute_count} minute{'s' if hour_count > 1 else ''}"

                current_time = time_now.strftime("%A %B %d %H:%M")
                next_run_str = next_run.strftime("%A %B %d %H:%M")
                logger.ghost(f"Current Time: {current_time} | {remaining_str} until the next run on {next_run_str}")
                time.sleep(60)
        else:
            pic_thread({})
    except KeyboardInterrupt:
        logger.separator("Exiting Plex Image Cleanup")
