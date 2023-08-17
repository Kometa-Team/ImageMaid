import glob, os, shutil, sys, time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from urllib.parse import quote

if sys.version_info[0] != 3 or sys.version_info[1] < 11:
    print("Version Error: Version: %s.%s.%s incompatible please use Python 3.11+" % (sys.version_info[0], sys.version_info[1], sys.version_info[2]))
    sys.exit(0)

try:
    import plexapi, requests
    from num2words import num2words
    from pmmutils import args, logging, schedule, util
    from plexapi.exceptions import Unauthorized
    from plexapi.server import PlexServer
    from pmmutils.args import PMMArgs
    from pmmutils.exceptions import Continue, Failed
    from requests.status_codes import _codes as codes
    from retrying import retry
    from tqdm import tqdm
except (ModuleNotFoundError, ImportError):
    print("Requirements Error: Requirements are not installed")
    sys.exit(0)

def not_failed(exception):
    return not isinstance(exception, Failed)

meta_dirs = {
    "movie": "Movies",
    "show": "TV Shows",
    "season": "TV Shows",
    "episode": "TV Shows",
    "artist": "Artists",
    "album": "Albums",
    "track": "Albums",
    "photo": "Photos",
    "collection": "Collections",
    "playlist": "Playlists"
}
modes = {
    "nothing": {
        "ed": "", "ing": "", "space": "",
        "desc": "Metadata Directory Files will not even be looked at."
    },
    "clear": {
        "ed": "Cleared", "ing": "Clearing", "space": "Space Recovered",
        "desc": "Clears out the PIC Restore Directory. (CANNOT BE RESTORED)"
    },
    "report": {
        "ed": "Reported", "ing": "Reporting", "space": "Potential Recovery",
        "desc": "Metadata Directory File changes will be reported but not performed."
    },
    "move": {
        "ed": "Moved", "ing": "Moving", "space": "Potential Recovery",
        "desc": "Metadata Directory Files will be moved to the PIC Restore Directory. (CAN BE RESTORED)"
    },
    "restore": {
        "ed": "Restored", "ing": "Restoring", "space": "",
        "desc": "Restores the Metadata Directory Files from the PIC Restore Directory."
    },
    "remove": {
        "ed": "Removed", "ing": "Removing", "space": "Space Recovered",
        "desc": "Metadata Directory Files will be removed. (CANNOT BE RESTORED)"
    }
}
mode_descriptions = '\n\t'.join([f"{m}: {d}" for m, d in modes.items()])
sc_options = ["mode", "photo-transcoder", "empty-trash", "clean-bundles", "optimize-db"]
options = [
    {"arg": "u",  "key": "url",              "env": "PLEX_URL",         "type": "str",  "default": None,     "help": "Plex URL of the Server you want to connect to."},
    {"arg": "t",  "key": "token",            "env": "PLEX_TOKEN",       "type": "str",  "default": None,     "help": "Plex Token of the Server you want to connect to."},
    {"arg": "p",  "key": "plex",             "env": "PLEX_PATH",        "type": "str",  "default": None,     "help": "Path to the Plex Config Directory (Contains Directories: Cache, Metadata, Plug-in Support)."},
    {"arg": "m",  "key": "mode",             "env": "MODE",             "type": "str",  "default": "report", "help": f"Global Mode to Run the Script in ({', '.join(modes)}). (Default: report)"},
    {"arg": "sc", "key": "schedule",         "env": "SCHEDULE",         "type": "str",  "default": None,     "help": "Schedule to run in continuous mode."},
    {"arg": "di", "key": "discord",          "env": "DISCORD",          "type": "str",  "default": None,     "help": "Webhook URL to channel for Notifications."},
    {"arg": "ti", "key": "timeout",          "env": "TIMEOUT",          "type": "int",  "default": 600,      "help": "Connection Timeout in Seconds that's greater than 0. (Default: 600)"},
    {"arg": "s",  "key": "sleep",            "env": "SLEEP",            "type": "int",  "default": 60,       "help": "Sleep Timer between Empty Trash, Clean Bundles, and Optimize DB. (Default: 60)"},
    {"arg": "pt", "key": "photo-transcoder", "env": "PHOTO_TRANSCODER", "type": "bool", "default": False,    "help": "Global Toggle to Clean Plex's PhotoTranscoder Directory."},
    {"arg": "et", "key": "empty-trash",      "env": "EMPTY_TRASH",      "type": "bool", "default": False,    "help": "Global Toggle to Run Plex's Empty Trash Operation."},
    {"arg": "cb", "key": "clean-bundles",    "env": "CLEAN_BUNDLES",    "type": "bool", "default": False,    "help": "Global Toggle to Run Plex's Clean Bundles Operation."},
    {"arg": "od", "key": "optimize-db",      "env": "OPTIMIZE_DB",      "type": "bool", "default": False,    "help": "Global Toggle to Run Plex's Optimize DB Operation."},
    {"arg": "tr", "key": "trace",            "env": "TRACE",            "type": "bool", "default": False,    "help": "Run with extra trace logs."},
    {"arg": "lr", "key": "log-requests",     "env": "LOG_REQUESTS",     "type": "bool", "default": False,    "help": "Run with every request logged."}
]
script_name = "Plex Image Cleanup"
plex_db_name = "com.plexapp.plugins.library.db"
base_dir = Path(__file__).parent
config_dir = base_dir / "config"
pmmargs = PMMArgs("meisnate12/Plex-Image-Cleanup", base_dir, options, use_nightly=False)
logger = logging.PMMLogger(script_name, "plex_image_cleanup", config_dir / "logs", discord_url=pmmargs["discord"], is_trace=pmmargs["trace"], log_requests=pmmargs["log-requests"])
logger.secret([pmmargs["url"], pmmargs["discord"], pmmargs["token"], quote(str(pmmargs["url"])), requests.utils.urlparse(pmmargs["url"]).netloc])
requests.Session.send = util.update_send(requests.Session.send, pmmargs["timeout"])
plexapi.BASE_HEADERS["X-Plex-Client-Identifier"] = pmmargs.uuid

def pic_thread(attrs):
    with ProcessPoolExecutor(max_workers=1) as executor:
        executor.submit(run_plex_image_cleanup, *[attrs])

def run_plex_image_cleanup(attrs):
    try:
        logger.header(pmmargs, sub=True, discord_update=True)
        logger.separator("Validating Options", space=False, border=False)
        do_transcode = attrs["photo-transcoder"] if "photo-transcoder" in attrs else pmmargs["photo-transcoder"]
        do_trash = attrs["empty-trash"] if "empty-trash" in attrs else pmmargs["empty-trash"]
        do_bundles = attrs["clean-bundles"] if "clean-bundles" in attrs else pmmargs["clean-bundles"]
        do_optimize = attrs["optimize-db"] if "optimize-db" in attrs else pmmargs["optimize-db"]
        if "mode" in attrs and attrs["mode"]:
            mode = str(attrs["mode"]).lower()
        elif pmmargs["mode"]:
            mode = str(pmmargs["mode"]).lower()
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
    except Exception as e:
        logger.stacktrace()
        logger.critical(e, discord=True)
        raise
    except KeyboardInterrupt:
        logger.separator(f"User Canceled Run {script_name}")
        logger.remove_main_handler()
        raise

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

        # Check Plex Path
        if not pmmargs["plex"]:
            pmmargs["plex"] = base_dir / "plex"
            if not pmmargs["plex"].exists():
                raise Failed("Args Error: No Plex Path Provided")
            logger.warning(f"No Plex Path Provided. Using default: {pmmargs['plex']}")
        pmmargs["plex"] = Path(pmmargs["plex"]).resolve()
        transcoder_dir = pmmargs["plex"] / "Cache" / "PhotoTranscoder"
        meta_dir = pmmargs["plex"] / "Metadata"
        restore_dir = pmmargs["plex"] / "PIC Restore"

        if not pmmargs["plex"].exists():
            raise Failed(f"Directory Error: Plex Databases Directory Not Found: {pmmargs['plex']}")
        elif mode != "nothing" and not meta_dir.exists():
            raise Failed(f"Directory Error: Metadata Directory Not Found: {meta_dir}")
        elif do_transcode and not transcoder_dir.exists():
            logger.error(f"Directory Error: PhotoTranscoder Directory Not Found and will not be cleaned: {transcoder_dir}")
            do_transcode = False

        # Connection to Plex
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
        server = plex_connect()
        logger.info("Successfully Connected to Plex")

        try:
            if do_metadata and restore_dir.exists():
                logger.error(f"{mode} mode invalid while the PIC Restore Directory exists.", discord=True, rows=[
                    [("PIC Path", restore_dir)],
                    [("Mode Options",
                      "Mode: restore (Restore the bloat images back into Plex)\nMode: remove (Remove the bloat images)")]
                ])
                logger.error(f"PIC Path: {restore_dir}\n"
                             f"Mode Options:\n"
                             f"    Mode: restore (Restore the bloat images back into Plex)\n"
                             f"    Mode: remove (Remove the bloat images)")
            elif do_metadata:

                def get_items(sec):
                    for parent in sec.all():
                        yield parent

                        if parent.type in ("show", "artist"):
                            for child in parent:
                                yield child

                                if child.type in ("season", "album"):
                                    for grandchild in child:
                                        yield grandchild

                    if sec.type != "photo":
                        for col in sec.collections():
                            yield col

                    for ply in sec.playlists():
                        yield ply

                def get_resources(media):
                    for method in ["posters", "arts", "themes"]:
                        for res in getattr(media, method, lambda: [])():
                            yield res

                logger.separator(f"{modes[mode]['ing']} Bloat Images")
                logger.info(f"Scanning Plex Items For Bloat Images: {meta_dir}", start="scanning")

                total_images = 0
                total_size = 0
                for section in server.library.sections():
                    section_images = 0
                    logger.info(f"Scanning Library: {section.title}", start=section.title)
                    for item in tqdm(get_items(section), unit=" plex items", desc="| Scanning Plex Items for Bloat Images"):
                        for resource in get_resources(item):
                            if resource.ratingKey.startswith("upload://") and not resource.selected:
                                if item.type == "season":
                                    guid = item.parentGuid
                                elif item.type == "episode":
                                    guid = item.grandparentGuid
                                elif item.type == "track":
                                    guid = item.parentGuid
                                else:
                                    guid = item.guid

                                guid_hash = sha1(guid.encode("utf-8")).hexdigest()
                                resource_path = resource.ratingKey.split("://")[-1]

                                local_path = Path(meta_dirs[item.type]) / guid_hash[0] / f"{guid_hash[1:]}.bundle" / "Uploads" / resource_path
                                source_path = meta_dir / local_path

                                if source_path.exists():
                                    section_images += 1
                                    total_size += source_path.stat().st_size
                                    if mode == "move":
                                        destination_path = restore_dir / local_path.with_suffix(".jpg")
                                        msg = f"MOVE: {source_path} --> {destination_path}"
                                        destination_path.parent.mkdir(parents=True, exist_ok=True)
                                        source_path.rename(destination_path)
                                    elif mode == "remove":
                                        msg = f"REMOVE: {source_path}"
                                        source_path.unlink()
                                    else:
                                        msg = f"BLOAT FILE: {source_path}"
                                else:
                                    msg = f"BLOAT FILE NOT FOUND: {source_path}"

                                if mode == "report":
                                    logger.debug(msg)
                                else:
                                    logger.trace(msg)
                    total_images += section_images
                    logger.info(f"{section_images} Bloat Images Found in {section.title}")
                    logger.info(f"Runtime: {logger.runtime()}")

                logger.info(f"{modes[mode]['ing']} Complete: {modes[mode]['ed']} {total_images} Bloat Images")
                space = util.format_bytes(total_size)
                logger.info(f"{modes[mode]['space']}: {space}")
                logger.info(f"Runtime: {logger.runtime(name='scanning')}")
                report.append([(f"{modes[mode]['ing']} Bloat Images", "")])
                report.append([("", f"{space} of {modes[mode]['space']} {modes[mode]['ing']} {total_images} Files")])
                report.append([(f"{mode.capitalize()} Time", f"{logger.runtime('scanning')}")])
            elif mode in ["restore", "clear"]:
                if not restore_dir.exists():
                    raise Failed(f"Restore Failed: PIC Restore Directory does not exist: {restore_dir}")
                if mode == "restore":
                    logger.separator("Restore Renamed Bloat Images")

                    logger.info("Scanning for Renamed Bloat Images to Restore", start="scanning")
                    restore_images = [f for f in tqdm(glob.iglob(os.path.join(restore_dir, "**", "*.jpg"), recursive=True), unit=" image", desc="| Scanning for Renamed Bloat Images to Restore")]
                    logger.info(f"Scanning Complete: Found {len(restore_images)} Renamed Bloat Images to Restore")
                    logger.info(f"Runtime: {logger.runtime()}")
                    logger.info()

                    logger.info("Restoring Renamed Bloat Images", start="work")
                    for source_path in tqdm(restore_images, unit=" restored", desc="| Restoring Renamed Bloat Images"):
                        destination_path = (meta_dir / str(source_path).removeprefix(restore_dir)[1:]).with_suffix(".jpg")
                        messages.append(f"RENAME: {source_path}\n  ----> {destination_path}\n")
                        destination_path.parent.mkdir(exist_ok=True)
                        source_path.rename(destination_path)
                    shutil.rmtree(restore_dir)
                    for message in messages:
                        logger.trace(message)
                    logger.info(f"Restore Complete: Restored {len(restore_images)} Renamed Bloat Images")
                    logger.info(f"Runtime: {logger.runtime()}")
                    report.append([("Restore Renamed Bloat Images", "")])
                    report.append([("Scan Time", f"{logger.runtime('scanning')}"), ("Restore Time", f"{logger.runtime('work')}")])
                else:
                    logger.separator("Removing PIC Restore Directory")

                    logger.info("Scanning PIC Restore for Bloat Images to Remove", start="scanning")
                    del_paths = [Path(r) / f for r, d, fs in tqdm(os.walk(restore_dir), unit=" directories", desc="| Scanning PIC Restore for Bloat Images to Remove") for f in fs]
                    logger.info(f"Scanning Complete: Found {len(del_paths)} Bloat Images in the PIC Directory to Remove")
                    logger.info(f"Runtime: {logger.runtime()}")
                    logger.info()

                    logger.info("Removing PIC Restore Bloat Images", start="work")
                    logger["size"] = 0
                    for path in tqdm(del_paths, unit=" removed", desc="| Removing PIC Restore Bloat Images"):
                        messages.append(f"REMOVE: {path}")
                        path = Path(path)
                        logger["size"] += path.stat().st_size
                        path.unlink()
                    shutil.rmtree(restore_dir)
                    for message in messages:
                        logger.trace(message)
                    logger.info(f"Removing Complete: Removed {len(del_paths)} PIC Restore Bloat Images")
                    space = util.format_bytes(logger["size"])
                    logger.info(f"Space Recovered: {space}")
                    logger.info(f"Runtime: {logger.runtime()}")
                    report.append([("Removing PIC Restore Bloat Images", "")])
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
                file = transcoder_dir / f
                messages.append(f"REMOVE: {file}")
                logger["size"] += file.stat().st_size
                file.unlink()
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
                    for _ in tqdm(range(pmmargs["sleep"]), desc=f"Sleeping for {pmmargs['sleep']} seconds", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]"):
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
        if pmmargs["schedule"]:
            pmmargs["schedule"] = pmmargs["schedule"].lower().replace(" ", "")
            valid_sc = []
            schedules = pmmargs["schedule"].split(",")
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
                                final = args.parse_bool(v)
                                if final is None:
                                    raise Failed(f'"{k.capitalize()} Error: {v} must be either "True" or "False""')
                            options[k] = final

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
            pic_thread({})
    except KeyboardInterrupt:
        logger.separator("Exiting Plex Image Cleanup")
