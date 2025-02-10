import glob, os, re, shutil, sqlite3, sys, time, zipfile
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from datetime import datetime
from urllib.parse import quote

if sys.version_info[0] != 3 or sys.version_info[1] < 11:
    print("Version Error: Version: %s.%s.%s incompatible please use Python 3.11+" % (sys.version_info[0], sys.version_info[1], sys.version_info[2]))
    sys.exit(0)

try:
    import plexapi, requests
    from num2words import num2words
    from kometautils import schedule, util, KometaLogger, KometaArgs, Continue, Failed
    from kometautils.args import parse_bool
    from PIL import Image
    from plexapi.exceptions import Unauthorized
    from plexapi.server import PlexServer
    from requests.status_codes import _codes as codes
    from retrying import retry
    from tqdm import tqdm
except (ModuleNotFoundError, ImportError):
    print("Requirements Error: Requirements are not installed")
    sys.exit(0)

def not_failed(exception):
    return not isinstance(exception, Failed)

modes = {
    "nothing": {
        "ed": "", "ing": "", "space": "",
        "desc": "Metadata Directory Files will not even be looked at."
    },
    "clear": {
        "ed": "Cleared", "ing": "Clearing", "space": "Space Recovered",
        "desc": "Clears out the ImageMaid Restore Directory. (CANNOT BE RESTORED)"
    },
    "report": {
        "ed": "Reported", "ing": "Reporting", "space": "Potential Recovery",
        "desc": "Metadata Directory File changes will be reported but not performed."
    },
    "move": {
        "ed": "Moved", "ing": "Moving", "space": "Potential Recovery",
        "desc": "Metadata Directory Files will be moved to the ImageMaid Restore Directory. (CAN BE RESTORED)"
    },
    "restore": {
        "ed": "Restored", "ing": "Restoring", "space": "",
        "desc": "Restores the Metadata Directory Files from the ImageMaid Restore Directory."
    },
    "remove": {
        "ed": "Removed", "ing": "Removing", "space": "Space Recovered",
        "desc": "Metadata Directory Files will be removed. (CANNOT BE RESTORED)"
    }
}
mode_descriptions = '\n\t'.join([f"{m}: {d}" for m, d in modes.items()])
sc_options = ["mode", "photo-transcoder", "empty-trash", "clean-bundles", "optimize-db"]
options = [
    {"arg": "p",  "key": "plex",             "env": "PLEX_PATH",        "type": "str",  "default": None,     "help": "Path to the Plex Config Directory (Contains Directories: Cache, Metadata, Plug-in Support)."},
    {"arg": "m",  "key": "mode",             "env": "MODE",             "type": "str",  "default": "report", "help": f"Global Mode to Run the Script in ({', '.join(modes)}). (Default: report)"},
    {"arg": "sc", "key": "schedule",         "env": "SCHEDULE",         "type": "str",  "default": None,     "help": "Schedule to run in continuous mode."},
    {"arg": "u",  "key": "url",              "env": "PLEX_URL",         "type": "str",  "default": None,     "help": "Plex URL of the Server you want to connect to."},
    {"arg": "t",  "key": "token",            "env": "PLEX_TOKEN",       "type": "str",  "default": None,     "help": "Plex Token of the Server you want to connect to."},
    {"arg": "oo", "key": "overlays-only",    "env": "OVERLAYS_ONLY",    "type": "bool", "default": False,    "help": "Will only remove Kometa Overlay Images and other images will be ignored."},
    {"arg": "di", "key": "discord",          "env": "DISCORD",          "type": "str",  "default": None,     "help": "Webhook URL to channel for Notifications."},
    {"arg": "ti", "key": "timeout",          "env": "TIMEOUT",          "type": "int",  "default": 600,      "help": "Connection Timeout in Seconds that's greater than 0. (Default: 600)"},
    {"arg": "nv", "key": "no-verify-ssl",    "env": "NO_VERIFY_SSL",    "type": "bool", "default": False,    "help": "Turns off Global SSL Verification."},
    {"arg": "s",  "key": "sleep",            "env": "SLEEP",            "type": "int",  "default": 60,       "help": "Sleep Timer between Empty Trash, Clean Bundles, and Optimize DB. (Default: 60)"},
    {"arg": "i",  "key": "ignore",           "env": "IGNORE_RUNNING",   "type": "bool", "default": False,    "help": "Ignore Warnings that Plex is currently Running."},
    {"arg": "l",  "key": "local",            "env": "LOCAL_DB",         "type": "bool", "default": False,    "help": "The script will copy the database file rather than downloading it through the Plex API (Helps with Large DBs)."},
    {"arg": "e",  "key": "existing",         "env": "USE_EXISTING",     "type": "bool", "default": False,    "help": "Use the existing database if less then 2 hours old."},
    {"arg": "pt", "key": "photo-transcoder", "env": "PHOTO_TRANSCODER", "type": "bool", "default": False,    "help": "Global Toggle to Clean Plex's PhotoTranscoder Directory."},
    {"arg": "et", "key": "empty-trash",      "env": "EMPTY_TRASH",      "type": "bool", "default": False,    "help": "Global Toggle to Run Plex's Empty Trash Operation."},
    {"arg": "cb", "key": "clean-bundles",    "env": "CLEAN_BUNDLES",    "type": "bool", "default": False,    "help": "Global Toggle to Run Plex's Clean Bundles Operation."},
    {"arg": "od", "key": "optimize-db",      "env": "OPTIMIZE_DB",      "type": "bool", "default": False,    "help": "Global Toggle to Run Plex's Optimize DB Operation."},
    {"arg": "tr", "key": "trace",            "env": "TRACE",            "type": "bool", "default": False,    "help": "Run with extra trace logs."},
    {"arg": "lr", "key": "log-requests",     "env": "LOG_REQUESTS",     "type": "bool", "default": False,    "help": "Run with every request logged."},
    {"arg": "nv", "key": "no-verify-ssl",    "env": "NO_VERIFY_SSL",    "type": "bool", "default": False,    "help": "Turns off Global SSL Verification."}
]
script_name = "ImageMaid"
plex_db_name = "com.plexapp.plugins.library.db"
base_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.join(base_dir, "config")
args = KometaArgs("Kometa-Team/ImageMaid", base_dir, options, use_nightly=False)
logger = KometaLogger(script_name, "imagemaid", os.path.join(config_dir, "logs"), discord_url=args["discord"], is_trace=args["trace"], log_requests=args["log-requests"])
logger.secret([args["url"], args["discord"], args["token"], quote(str(args["url"])), requests.utils.urlparse(args["url"]).netloc])
requests.Session.send = util.update_send(requests.Session.send, args["timeout"])
plexapi.BASE_HEADERS["X-Plex-Client-Identifier"] = args.uuid

def imagemaid_thread(attrs):
    with ProcessPoolExecutor(max_workers=1) as executor:
        executor.submit(run_imagemaid, *[attrs])

def run_imagemaid(attrs):
    logger.header(args, sub=True, discord_update=True)
    logger.separator("Validating Options", space=False, border=False)
    do_transcode = attrs["photo-transcoder"] if "photo-transcoder" in attrs else args["photo-transcoder"]
    do_trash = attrs["empty-trash"] if "empty-trash" in attrs else args["empty-trash"]
    do_bundles = attrs["clean-bundles"] if "clean-bundles" in attrs else args["clean-bundles"]
    do_optimize = attrs["optimize-db"] if "optimize-db" in attrs else args["optimize-db"]
    local_run = args["local"]
    if "mode" in attrs and attrs["mode"]:
        mode = str(attrs["mode"]).lower()
    elif args["mode"]:
        mode = str(args["mode"]).lower()
    else:
        mode = "report"
    description = f"Running in {mode.capitalize()} Mode"
    extras = []
    if do_trash:
        extras.append("Empty Trash")
    if do_bundles:
        extras.append("Clean Bundles")
    if do_optimize:
        extras.append("Optimize DB")
    if do_transcode:
        extras.append("PhotoTrancoder")
    if extras:
        description += f" with {', '.join(extras[:-1])}{', and ' if len(extras) > 1 else ''}{extras[-1]} set to True"
    logger.info(description)

    try:
        logger.info("Script Started", log=False, discord=True, start="script")
    except Failed as e:
        logger.error(f"Discord URL Error: {e}")
    report = []
    messages = []
    try:
        # Check Mode
        if mode not in modes:
            raise Failed(f"Mode Error: {mode} Invalid. Options: \n\t{mode_descriptions}")
        logger.info(f"{mode.capitalize()}: {modes[mode]['desc']}")
        do_metadata = mode in ["report", "move", "remove"]
        if do_metadata and not local_run and not args["url"] and not args["token"]:
            local_run = True
            logger.warning("No Plex URL and Plex Token Given assuming Local Run")

        # Check Plex Path
        if not args["plex"]:
            if not os.path.exists(os.path.join(base_dir, "plex")):
                raise Failed("Args Error: No Plex Path Provided")
            logger.warning(f"No Plex Path Provided. Using default: {os.path.join(base_dir, 'plex')}")
            args["plex"] = os.path.join(base_dir, "plex")
        args["plex"] = os.path.abspath(args["plex"])
        transcoder_dir = os.path.join(args["plex"], "Cache", "PhotoTranscoder")
        databases_dir = os.path.join(args["plex"], "Plug-in Support", "Databases")
        meta_dir = os.path.join(args["plex"], "Metadata")
        restore_dir = os.path.join(args["plex"], "ImageMaid Restore")

        if not os.path.exists(args["plex"]):
            raise Failed(f"Directory Error: Plex Databases Directory Not Found: {os.path.abspath(args['plex'])}")
        elif local_run and not os.path.exists(databases_dir):
            raise Failed(f"Directory Error: Plug-in Support\\Databases Directory Not Found: {databases_dir}")
        elif mode != "nothing" and not os.path.exists(meta_dir):
            raise Failed(f"Directory Error: Metadata Directory Not Found: {meta_dir}")
        elif do_transcode and not os.path.exists(transcoder_dir):
            logger.error(f"Directory Error: PhotoTranscoder Directory Not Found and will not be cleaned: {transcoder_dir}")
            do_transcode = False

        # Connection to Plex
        server = None
        if do_trash or do_bundles or do_optimize or (do_metadata and not local_run):
            logger.info("Connecting To Plex")
            if not args["url"]:
                raise Failed("Args Error: No Plex URL Provided")
            if not args["token"]:
                raise Failed("Args Error: No Plex Token Provided")
            plexapi.server.TIMEOUT = args["timeout"]
            os.environ["PLEXAPI_PLEXAPI_TIMEOUT"] = str(args["timeout"])

            @retry(stop_max_attempt_number=5, wait_incrementing_start=60000, wait_incrementing_increment=60000, retry_on_exception=not_failed)
            def plex_connect():
                try:
                    session = requests.session()
                    if args["no-verify-ssl"]:
                        session.verify = False
                        if session.verify is False:
                            import urllib3
                            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                    return PlexServer(args["url"], args["token"], timeout=args["timeout"], session=session)
                except Unauthorized:
                    raise Failed("Plex Error: Plex token is invalid")
                except Exception as e1:
                    logger.error(e1)
                    raise
            server = plex_connect()
            logger.info("Successfully Connected to Plex")

        try:
            if do_metadata and os.path.exists(restore_dir):
                logger.error(f"{mode} mode invalid while the ImageMaid Restore Directory exists.", discord=True, rows=[
                    [("ImageMaid Path", restore_dir)],
                    [("Mode Options",
                      "Mode: restore (Restore the bloat images back into Plex)\nMode: remove (Remove the bloat images)")]
                ])
                logger.error(f"ImageMaid Path: {restore_dir}\n"
                             f"Mode Options:\n"
                             f"    Mode: restore (Restore the bloat images back into Plex)\n"
                             f"    Mode: remove (Remove the bloat images)")
            if do_metadata:

                # Check if Running
                if local_run:
                    if any([os.path.exists(os.path.join(databases_dir, f"{plex_db_name}-{t}")) for t in ["shm", "wal"]]):
                        temp_db_warning = "At least one of the SQLite temp files is next to the Plex DB; this indicates Plex is still running\n" \
                                          "and copying the DB carries a small risk of data loss as the temp files may not have updated the\n" \
                                          "main DB yet.\n" \
                                          "If you restarted Plex just before running ImageMaid, and are still getting this error, it\n" \
                                          "can be ignored by using `--ignore` or setting `IGNORE_RUNNING=True` in the .env file."
                        if not args["ignore"]:
                            raise Failed(temp_db_warning)
                        logger.info(temp_db_warning)
                        logger.info("Warning Ignored")

                # Download DB
                logger.separator("Database")
                dbpath = os.path.join(config_dir, plex_db_name)
                temp_dir = os.path.join(config_dir, "temp")

                is_usable = False
                if args["existing"]:
                    if os.path.exists(dbpath):
                        is_usable, time_ago = util.in_the_last(dbpath, hours=2)
                        if is_usable:
                            logger.info(f"Using existing database (age: {time_ago})")
                        else:
                            logger.info(f"Existing database too old to use (age: {time_ago})")
                    else:
                        logger.warning(f"Existing Database not found {'making' if local_run else 'downloading'} a new copy")

                report.append([("Database", "")])
                fields = []
                if is_usable:
                    report.append([("", "Using Existing Database")])
                else:
                    report.append([("", f"{'Copied' if local_run else 'Downloaded'} New Database")])
                    if os.path.exists(dbpath):
                        os.remove(dbpath)
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir)
                    os.makedirs(temp_dir)
                    if local_run:
                        logger.info(f"Copying database from {os.path.join(databases_dir, plex_db_name)}", start="database")
                        util.copy_with_progress(os.path.join(databases_dir, plex_db_name), dbpath, description=f"Copying database file to: {dbpath}")
                    else:
                        logger.info("Downloading Database via the Plex API. First Plex will make a backup of your database.\n"
                                    "To see progress, log into Plex and go to Settings | Manage | Console and filter on Database.\n"
                                    "You can also look at the Plex Dashboard to see the progress of the Database backup.", start="database")
                        logger.info()

                        # fetch the data to be saved
                        headers = {'X-Plex-Token': server._token}
                        response = server._session.get(server.url('/diagnostics/databases'), headers=headers, stream=True)
                        if response.status_code not in (200, 201, 204):
                            message = f"({response.status_code}) {codes.get(response.status_code)[0]}; {response.url} "
                            raise Failed(f"Database Download Failed Try Using Local Copy: {message} " + response.text.replace('\n', ' '))
                        os.makedirs(temp_dir, exist_ok=True)

                        filename = None
                        if response.headers.get('Content-Disposition'):
                            filename = re.findall(r'filename=\"(.+)\"', response.headers.get('Content-Disposition'))
                            filename = filename[0] if filename[0] else None
                        if not filename:
                            raise Failed("DB Filename not found")
                        filename = os.path.basename(filename)
                        fullpath = os.path.join(temp_dir, filename)
                        extension = os.path.splitext(fullpath)[-1]
                        if not extension:
                            contenttype = response.headers.get('content-type')
                            if contenttype and 'image' in contenttype:
                                fullpath += contenttype.split('/')[1]

                        with tqdm(unit='B', unit_scale=True, total=int(response.headers.get('content-length', 0)), desc=f"| {filename}") as bar:
                            with open(fullpath, 'wb') as handle:
                                for chunk in response.iter_content(chunk_size=4024):
                                    handle.write(chunk)
                                    bar.update(len(chunk))

                        # check we want to unzip the contents
                        if fullpath.endswith('zip'):
                            with zipfile.ZipFile(fullpath, 'r') as handle:
                                handle.extractall(temp_dir)

                        if backup_file := next((o for o in os.listdir(temp_dir) if str(o).startswith("databaseBackup")), None):
                            shutil.move(os.path.join(temp_dir, backup_file), dbpath)
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir)
                    if not os.path.exists(dbpath):
                        raise Failed(f"File Error: Database File Could not {'Copied' if local_run else 'Downloaded'}")
                    logger.info(f"Plex Database {'Copy' if local_run else 'Download'} Complete")
                    logger.info(f"Database {'Copied' if local_run else 'Downloaded'} to: {dbpath}")
                    logger.info(f"Runtime: {logger.runtime()}")
                    fields.append(("Copied" if local_run else "Downloaded", f"{logger.runtime('database')}"))

                # Query DB
                urls = []
                with sqlite3.connect(dbpath) as connection:
                    logger.info()
                    logger.info("Database Opened Querying For In-Use Images", start="query")
                    connection.row_factory = sqlite3.Row
                    with closing(connection.cursor()) as cursor:
                        for field in ["user_thumb_url", "user_art_url", "user_banner_url"]:
                            cursor.execute(f"SELECT {field} AS url FROM metadata_items WHERE {field} like 'upload://%' OR {field} like 'metadata://%'")
                            urls.extend([requests.utils.urlparse(r["url"]).path.split("/")[-1] for r in cursor.fetchall() if r and r["url"]])
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
                    bloat_paths_filtered = []
                    for path in tqdm(bloat_paths, unit=f" {modes[mode]['ed'].lower()}", desc=f"| {modes[mode]['ing']} Bloat Images"):
                        overlay_bloat = False
                        if args["overlays-only"]:
                            with Image.open(path) as image:
                                exif_tags = image.getexif()
                                if 0x04bc in exif_tags and exif_tags[0x04bc] == "overlay":
                                    overlay_bloat = True
                        if args["overlays-only"] and overlay_bloat is False:
                            messages.append(f"IGNORED FILE NO EXIF TAG: {path}")
                        else:
                            logger["size"] += os.path.getsize(path)
                            bloat_paths_filtered.append(path)
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
                    logger.info(f"{modes[mode]['ing']} Complete: {modes[mode]['ed']} {len(bloat_paths_filtered)} Bloat Images")
                    space = util.format_bytes(logger["size"])
                    logger.info(f"{modes[mode]['space']}: {space}")
                    logger.info(f"Runtime: {logger.runtime()}")
                    report.append([(f"{modes[mode]['ing']} Bloat Images", "")])
                    report.append([("", f"{space} of {modes[mode]['space']} {modes[mode]['ing']} {len(bloat_paths_filtered)} Files")])
                    report.append([("Scan Time", f"{logger.runtime('scanning')}"), (f"{mode.capitalize()} Time", f"{logger.runtime('work')}")])
            elif mode in ["restore", "clear"]:
                if not os.path.exists(restore_dir):
                    raise Failed(f"Restore Failed: ImageMaid Restore Directory does not exist: {restore_dir}")
                if mode == "restore":
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
                    shutil.rmtree(restore_dir)
                    for message in messages:
                        logger.trace(message)
                    messages = []
                    logger.info(f"Restore Complete: Restored {len(restore_images)} Renamed Bloat Images")
                    logger.info(f"Runtime: {logger.runtime()}")
                    report.append([("Restore Renamed Bloat Images", "")])
                    report.append([("Scan Time", f"{logger.runtime('scanning')}"), ("Restore Time", f"{logger.runtime('work')}")])
                else:
                    logger.separator("Removing ImageMaid Restore Directory")

                    logger.info("Scanning ImageMaid Restore for Bloat Images to Remove", start="scanning")
                    del_paths = [os.path.join(r, f) for r, d, fs in tqdm(os.walk(restore_dir), unit=" directories", desc="| Scanning ImageMaid Restore for Bloat Images to Remove") for f in fs]
                    logger.info(f"Scanning Complete: Found {len(del_paths)} Bloat Images in the ImageMaid Directory to Remove")
                    logger.info(f"Runtime: {logger.runtime()}")
                    logger.info()

                    messages = []
                    logger.info("Removing ImageMaid Restore Bloat Images", start="work")
                    logger["size"] = 0
                    for path in tqdm(del_paths, unit=" removed", desc="| Removing ImageMaid Restore Bloat Images"):
                        messages.append(f"REMOVE: {path}")
                        logger["size"] += os.path.getsize(path)
                        os.remove(path)
                    shutil.rmtree(restore_dir)
                    for message in messages:
                        logger.trace(message)
                    logger.info(f"Removing Complete: Removed {len(del_paths)} ImageMaid Restore Bloat Images")
                    space = util.format_bytes(logger["size"])
                    logger.info(f"Space Recovered: {space}")
                    logger.info(f"Runtime: {logger.runtime()}")
                    report.append([("Removing ImageMaid Restore Bloat Images", "")])
                    report.append([("", f"{space} of Space Recovered Removing {len(del_paths)} Files")])
                    report.append([("Scan Time", f"{logger.runtime('scanning')}"), ("Restore Time", f"{logger.runtime('work')}")])
        except Failed as e:
            logger.error(f"Metadata Error: {e}")

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
                os.remove(file)
            for message in messages:
                logger.trace(message)

            logger.info(f"Remove Complete: Removed {len(transcode_images)} PhotoTranscoder Images")
            space = util.format_bytes(logger["size"])
            logger.info(f"Space Recovered: {space}")
            logger.info(f"Runtime: {logger.runtime()}")
            report.append([("Remove PhotoTranscoder Images", "")])
            report.append([("", f"{space} of Space Recovered Removing {len(transcode_images)} Files")])
            report.append([("Scan Time", f"{logger.runtime('transcode_scan')}"), ("Remove Time", f"{logger.runtime('transcode')}")])

        # Plex Operations
        for arg, arg_check, title, op in [
            ("empty-trash", do_trash, "Empty Trash", "emptyTrash"),
            ("clean-bundles", do_bundles, "Clean Bundles", "cleanBundles"),
            ("optimize-db", do_optimize, "Optimize DB", "optimize")
        ]:
            if arg_check:
                if server:
                    logger.separator()
                    getattr(server.library, op)()
                    logger.info(f"{title} Plex Operation Started")
                    for _ in tqdm(range(args["sleep"]), desc=f"Sleeping for {args['sleep']} seconds", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]"):
                        time.sleep(1)
                else:
                    logger.error(f"Plex Error: {title} requires a connection to Plex")
    except Failed as e:
        logger.separator()
        logger.critical(e, discord=True)
        logger.separator()
    except Exception as e:
        for message in messages:
            logger.debug(message)
        logger.stacktrace()
        logger.critical(e, discord=True)
    except KeyboardInterrupt:
        logger.separator(f"User Canceled Run {script_name}")
        logger.remove_main_handler()
        raise

    logger.error_report()
    logger.switch()
    report.append([(f"{script_name} Finished", "")])
    report.append([("Total Runtime", f"{logger.runtime('script')}")])
    logger.report(f"{script_name} Summary", description=description, rows=report, width=18, discord=True)
    logger.remove_main_handler()

if __name__ == "__main__":
    try:
        if args["schedule"]:
            args["schedule"] = args["schedule"].lower().replace(" ", "")
            valid_sc = []
            schedules = args["schedule"].split(",")
            logger.separator(f"{script_name} Continuous Scheduled")
            logger.info()
            logger.info("Scheduled Runs: ")
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
                                final = parse_bool(v)
                                if final is None:
                                    raise Failed(f'"{k.capitalize()} Error: {v} must be either "True" or "False""')
                            options[k] = final

                    if frequency == "daily":
                        run_str += "Daily"
                        schedule.every().day.at(time_to_run).do(imagemaid_thread, options)
                    elif frequency.startswith("weekly(") and frequency.endswith(")"):
                        weekday = frequency[7:-1]
                        run_str += f"Weekly on {weekday.capitalize()}s"
                        match weekday:
                            case "sunday":
                                schedule.every().sunday.at(time_to_run).do(imagemaid_thread, options)
                            case "monday":
                                schedule.every().monday.at(time_to_run).do(imagemaid_thread, options)
                            case "tuesday":
                                schedule.every().tuesday.at(time_to_run).do(imagemaid_thread, options)
                            case "wednesday":
                                schedule.every().wednesday.at(time_to_run).do(imagemaid_thread, options)
                            case "thursday":
                                schedule.every().thursday.at(time_to_run).do(imagemaid_thread, options)
                            case "friday":
                                schedule.every().friday.at(time_to_run).do(imagemaid_thread, options)
                            case "saturday":
                                schedule.every().saturday.at(time_to_run).do(imagemaid_thread, options)
                            case _:
                                raise Failed(f"Schedule Error: Invalid Weekly Frequency: {frequency}\nValue must a weekday")
                    elif frequency.startswith("monthly(") and frequency.endswith(")"):
                        try:
                            day = int(frequency[8:-1])
                            run_str += f"Monthly on the {num2words(day, to='ordinal_num')}"
                            if 0 < day < 32:
                                schedule.every().month_on(day).at(time_to_run).do(imagemaid_thread, options)
                            else:
                                raise ValueError
                        except ValueError:
                            raise Failed(f"Schedule Error: Invalid Monthly Frequency: {frequency}\nValue must be between 1-31")

                    run_str += f" at {time_to_run}"
                    if options:
                        run_str += f" (Options: {'; '.join([f'{k}={v}' for k, v in options.items()])})"
                    logger.info(f"* {run_str}")
                else:
                    raise Failed(f'Schedule Error: Invalid Schedule: {sc}\nEach Schedule must be in either the "time|frequency" or "time|frequency|options" format')

            logger.info()
            logger.separator()
            logger.info()
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
            imagemaid_thread({})
    except KeyboardInterrupt:
        logger.separator("Exiting ImageMaid")
