from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
import xml.etree.ElementTree as ET
from fastapi import HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
import shutil
import subprocess
import os
import time
import threading
from fastapi import Request
from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse




import logging

# Configure logger
logging.basicConfig(
    level=logging.DEBUG,  # default level
    format="%(asctime)s [%(levelname)s] %(message)s"
)


logger = logging.getLogger("myapp")


load_dotenv()  # loads .env
logger.info("Loaded .env file")


app = FastAPI(root_path="/streamer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# @app.middleware("http")
# async def log_requests(request: Request, call_next):
#     print("REQUEST PATH:", request.url.path)
#     response = await call_next(request)
#     print("RESPONSE STATUS:", response.status_code)
#     return response



# Dummy authentication
USERS = {"crap@lyhnemail.com": "rBeef7D7CHbjb1"}

# Example channel list
CHANNELS = [
    {"name": "DR1", "stream_url": "http://tvheadend:19981/stream/channel1"},
    {"name": "DR2", "stream_url": "http://tvheadend:19981/stream/channel2"},
]



STREAM_DIR = "/tmp/ramdrive/stream"
processes = {}          # channel → Popen
last_access = {}        # channel → timestamp
sessions = {}           # (uuid) -> [timestamp of last killed stream, ua]
flagged_uuids = {}    # (client_id, uuid) => timed out client : kicked due to no segment request


MIN_FREE_BYTES=200000000
MPD=True
TVHURL=os.getenv("TVHURL")
TVHPort=os.getenv("TVHPort")
baseURL = os.getenv("baseURL")


PROFILES = [
    {"name": "hq", "height": 720, "crf": 23, "maxrate": 6000, "preset": "veryfast"},
    #{"name": "mq", "height": 540, "crf": 25, "maxrate": 3500, "preset": "ultrafast"},
    {"name": "lq", "height": 360, "crf": 27, "maxrate": 1800, "preset": "ultrafast"},
]


UseGlobalCleaner=True
InactivityTimeOut=20
FlagTimeOut=180          # seconds to keep client banned/flagged for restarting request for specific UUID


#MPD file names
MPD_InitSegName="init"
MPD_ChunkName="chunk"


cleanup_started = False

# Startup event for any background tasks
@app.on_event("startup")
def start_background_tasks():
  purge_stream_dir()
  global cleanup_started
  if UseGlobalCleaner and not cleanup_started:
    # Start the global cleanup worker thread
    threading.Thread(target=cleanup_worker_global, daemon=True).start()
    cleanup_started = True
  # Start the activity monitor
  threading.Thread(target=monitor_inactivity, daemon=True).start()


def SourceStreamURL(uuid: str):
    return f"{TVHURL.rstrip('/')}:{TVHPort}/stream/channelid/{uuid}?profile=pass"

def ffmpeg_filter_complex(profiles=PROFILES):
    """
    Build the filter_complex string for multiple quality profiles.
    Returns a single string suitable for passing to -filter_complex.
    """
    filter_parts = [f"[0:v]bwdif,scale=-1:{p['height']},fps=25[{p['name']}]" for p in profiles]
    return "; ".join(filter_parts)


def ffmpeg_quality_settings(out_dir, uuid, profile, output):
    args =  []
    #need to add mapping for each quality
    if output == "HLS":
        args += [
            "-map", "0:v:0",
            "-map", "0:a:0",
            "-map", "-0:s",
            # set the profile specific settings
            "-vf", f"bwdif=mode=0:parity=auto,scale=-1:{profile['height']},fps=25",
        ]
    elif output == "DASH":
        args += [
            "-map", f"[{profile['name']}]",
            "-map", "0:a:0",
            "-map", "-0:s",
        ]
    else:
        raise ValueError(f"Unknown output type: {output}")
    
    args += [        
        # set the profile specific settings
        "-crf", f"{profile['crf']}",
        "-preset", f"{profile['preset']}",
        "-maxrate", f"{profile['maxrate']}k",
        "-bufsize", f"{profile['maxrate']*2}k",
        ]
    logger.info(f"FFmpeg quality settings for profile {profile['name']}: {' '.join(args)}")
    return args
def ffmpeg_common_mapping(streamURL, profile, output):
    args =  []
    if output == "HLS":
        pass
    elif output == "DASH":
        args += [
            "-filter_complex", ffmpeg_filter_complex(PROFILES)
        ]
    else:
        raise ValueError(f"Unknown output type: {output}")
    
    return args
def ffmpeg_common_args(streamURL, profile, output):
    args =  []
    
    args += [
        "ffmpeg",
        "-loglevel", "warning",
        "-stats",

        "-fflags", "+discardcorrupt+genpts+igndts+nobuffer",
        "-avoid_negative_ts", "make_zero",
        "-err_detect", "ignore_err",
        "-max_interleave_delta", "0",
        "-probesize", "2M",
        "-analyzeduration", "2M",

        "-i", streamURL,

        "-vsync", "cfr",
        ]

    if output == "HLS":
        pass
    elif output == "DASH":
        args += [
            "-filter_complex", ffmpeg_filter_complex(PROFILES)
        ]
    else:
        raise ValueError(f"Unknown output type: {output}")


    args += [
        # Video
        "-pix_fmt", "yuv420p",
        "-c:v", "libx264",
        "-x264-params", "ref=4",
        "-x264opts", "keyint=48:min-keyint=1:scenecut=0",
        "-tune", "zerolatency",

        "-profile:v", "high",
        "-level", "5.1",

        # Audio
        "-c:a", "aac",
        "-ac", "2",
        "-ar", "48000",
        "-b:a", "128k",

        # GOP
        "-g", "100",
        "-keyint_min", "100",
        ]
    return args

def ffmpeg_output_args(out_dir, uuid, profile, output: str):
    if output == "HLS":
        base = PlaylistURL(uuid)
        if not base.endswith("/"):
            base += "/"

        return [
            "-f", "hls",
            "-hls_time", "2",
            "-hls_list_size", "2000",
            "-hls_flags", "independent_segments+delete_segments+program_date_time",
            "-hls_segment_type", "mpegts",
            "-muxdelay", "0.7",
            "-muxpreload", "0.7",
            "-hls_segment_filename", f"{out_dir}/{profile['name']}_segment-%05d.ts",
            "-hls_base_url", base,
            f"{out_dir}/{profile['name']}.m3u8",
        ]
    elif output == "DASH":
        return [
            "-f", "dash",
            "-window_size", "2000",
            "-extra_window_size", "0",
            "-seg_duration", "2",
            "-use_template", "1",
            "-use_timeline", "1",
            "-remove_at_exit", "1",

            "-init_seg_name", f"{MPD_InitSegName}--$RepresentationID$.m4s",
            "-media_seg_name", f"{MPD_ChunkName}-$RepresentationID$-$Number%03d$.m4s",

            f"{out_dir}/manifest.mpd",
        ]        
    else:
        raise ValueError(f"Unknown output type: {output}")
# Prepare stream directory
def prepare_stream_dir(uuid, output):
    out_dir = f"{STREAM_DIR}/{uuid}"
    os.makedirs(out_dir, exist_ok=True)
    logger.debug(f"Creating directory: {out_dir}")

    if not UseGlobalCleaner:
        threading.Thread(
            target=cleanup_worker,
            args=(uuid,),
            daemon=True
        ).start()

    for f in os.listdir(out_dir):
        os.remove(os.path.join(out_dir, f))

    return out_dir
# Assemble and start ffmpeg command
def start_channel(uuid, output):
    out_dir = prepare_stream_dir(uuid, output)
    streamURL = SourceStreamURL(uuid)

    cmd = ffmpeg_common_args(streamURL, PROFILES[0], output)
    for i, profile in enumerate(PROFILES):
        cmd += ffmpeg_quality_settings(out_dir, uuid, profile, output)
        if output == "HLS":
            # add output format for each profile for HLS because each profile has its own playlist
            cmd += ffmpeg_output_args(out_dir, uuid, profile, output)
        if output == "DASH" and i == len(PROFILES) - 1:
            # only add output format once for DASH because all profiles are in the same manifest
            cmd += [ "-map" , "0:a:0", "-map", "-0:s"]  # no need to repeat for each profile 
            cmd += ffmpeg_output_args(out_dir, uuid, profile, output)  
                     

    logger.info("Starting FFmpeg (%s): %s", output.upper(), " ".join(cmd))
    return subprocess.Popen(cmd)
#Define master playlist for HLS
def write_master_playlist(out_dir, profiles):
    content = "#EXTM3U\n#EXT-X-VERSION:3\n"

    for p in profiles:
        # Calculate approximate bandwidth in bits per second
        bandwidth = int(p["maxrate"] * 1000)  # maxrate is in k, HLS expects bps
        # Approximate width assuming 16:9 aspect ratio
        width = int(p["height"] * 16 / 9)
        content += f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION={width}x{p['height']}\n"
        content += f"{p['name']}.m3u8\n"

    with open(f"{out_dir}/index.m3u8", "w") as f:
        f.write(content)


# for mpeg dash
# ffmpeg -i input.ts \
#   -filter_complex "\
#     [0:v]bwdif,scale=-1:720,fps=25[v720]; \
#     [0:v]bwdif,scale=-1:540,fps=25[v540]; \
#     [0:v]bwdif,scale=-1:360,fps=25[v360]" \
#   -map "[v720]" -map 0:a -c:v:0 libx264 -b:v:0 6000k -maxrate:v:0 6000k -bufsize:v:0 12000k -c:a:0 aac -b:a:0 128k \
#   -map "[v540]" -map 0:a -c:v:1 libx264 -b:v:1 3500k -maxrate:v:1 3500k -bufsize:v:1 7000k -c:a:1 aac -b:a:1 128k \
#   -map "[v360]" -map 0:a -c:v:2 libx264 -b:v:2 1800k -maxrate:v:2 1800k -bufsize:v:2 3600k -c:a:2 aac -b:a:2 128k \
#   -f dash -window_size 2000 -extra_window_size 0 -seg_duration 2 \
#   -use_template 1 -use_timeline 1 -remove_at_exit 1 \
#   -init_seg_name init--$RepresentationID$.m4s \
#   -media_seg_name chunk-$RepresentationID$-$Number%03d$.m4s \
#   /tmp/ramdrive/stream/877117831/manifest.mpd

def get_playlist(uuid: str, output: str, request: Request) -> str:
    out_dir = f"{STREAM_DIR}/{uuid}"

    if output != "HLS" and output != "DASH":
        raise ValueError(f"Unknown output type: {output}")
    
    plu = PlaylistURL(uuid) #playlist url
    if output == "HLS":
        # store the path of the master playlist
        path_main = f"{out_dir}/index.m3u8"
        # get path of first profile m3u8 because we will wait for it to be created
        path_first_profile = f"{out_dir}/{PROFILES[0]['name']}.m3u8"        
        logger.debug(f"Asking for index.m3u8 for UUID = : {uuid}")
    elif output == "DASH":
        # store the path of the master playlist
        path_main = f"{out_dir}/manifest.mpd"
        path_first_profile=path_main        
        logger.debug(f"Asking for manifest.mpd for UUID = : {uuid}")
    else:
        raise ValueError(f"Unknown output type: {output}")

    # check if this uuid is flagged
    if (uuid) in flagged_uuids:
        # uuid was in the flagged_uuids list
        logger.info(f"UUID is flagged: {uuid}")
        flagged_time = flagged_uuids[uuid]
        elapsed = time.time() - flagged_time
        if elapsed < FlagTimeOut:  # still in cooldown
            raise HTTPException(status_code=410, detail="This UUID (Channel) if flagged and in cooldown.")
        else:
            # cooldown expired -> remove from flagged
            del flagged_uuids[uuid]
            logger.info(f"UUID is no longer flagged: {uuid}")

    # Start ffmpeg if not running
    # start by checking if process exists and is running
    if uuid not in processes or processes[uuid].poll() is not None:
        logger.info(f"Creating new process with UUID = : {uuid}")
        processes[uuid] = start_channel(uuid, output)
        # Set the last_access to initialise it. Otherwise it is not present in the inactivity monitor and it will not work
        logger.debug(f"Setting last_access for for UUID = {uuid}")
        last_access[uuid] = time.time()
        # Set the session for client requesting the UUID
        _ua = ua(request)
        sessions[uuid] = { "last_time": time.time(), "ua": _ua,} #track last segment request (init with manifest request)
        logger.info(f"Inital request for index.m3u8 for UUID = : {uuid}")

    # Wait up to 15 seconds for ffmpeg to generate a REAL playlist
    # A real playlist has size > 200 bytes typically
    min_size = 200
    start = time.time()
    for _ in range(300):  # 150 × 0.1 sec = 15 seconds
        if os.path.exists(path_first_profile) and os.path.getsize(path_first_profile) > min_size:
            if output == "HLS":
                # Create master playlist on the fly
                # only for HLS
                write_master_playlist(out_dir, PROFILES)    
            logger.info(f"Playlist ready after {time.time() - start:.1f}s")
            return path_main
        time.sleep(0.1)

    # If still no real m3u8 → fail properly (player will retry)
    logger.info("M3U8 (HLS) not created in due course")
    raise ValueError(f"Playlist not ready in due course: {output}")

# This triggers the start-up of an HLS stream with ffmpeg
@app.get("/stream/{uuid}/index.m3u8")
def serve_manifest_hls(uuid: str, request: Request):
    try:
        path = get_playlist(uuid, "HLS", request)
        # Get file extension
        _, ext = os.path.splitext(path)
        return FileResponse(path, media_type=get_MIME_Type(ext))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))    
    


@app.get("/stream/{uuid}/manifest.mpd")
def serve_manifest_mpd(uuid: str, request: Request):
    try:
        path = get_playlist(uuid, "DASH", request)
        # Get file extension
        _, ext = os.path.splitext(path)
        return FileResponse(path, media_type=get_MIME_Type(ext))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e)) 

    

app.mount(
    "/player/",
    StaticFiles(directory="player/", html=True),
    name="player"
)

app.mount(
    "/shaka/",
    StaticFiles(directory="shaka/", html=True),
    name="shaka"
)


@app.get("/stream/{uuid}/{segment}")
def serve_segment(uuid: str, segment: str, request: Request):
    client_host = request.client.host
    client_port = request.client.port
    user_agent = request.headers.get("user-agent")
    logger.debug(f"{client_host}:{client_port}, User-Agent: {user_agent} requests /stream/{uuid}/{segment}")
    path = f"{STREAM_DIR}/{uuid}/{segment}"

    # Retry loop: try 10 times with 0.1s delay
    MAX_RETRIES = 10
    RETRY_DELAY = 0.1  # seconds
    for attempt in range(MAX_RETRIES):
        if os.path.isfile(path):
            break
        time.sleep(RETRY_DELAY)
    else:
        # After retries, still not found
        logger.warning(f"Requested file not found after {MAX_RETRIES} retries: {path}")
        raise HTTPException(status_code=404, detail="Segment not found")

    # Mark activity time (only for actual segments)
    if is_segment_file(segment):
        last_access[uuid] = time.time()
        client_id = f"{request.client.host}|{request.headers.get('user-agent', 'unknown')}"
        session = sessions.get(uuid)
        if session:
            session["last_time"] = time.time()  # track last segment request
            session["ua"] = client_id     # store who requested it

    # Get file extension and MIME type
    _, ext = os.path.splitext(segment)
    media_type = get_MIME_Type(ext)

    # Return the file
    return FileResponse(path, media_type=media_type)


def get_MIME_Type(ext: str) -> str:
    # Set proper MIME type
    if ext == ".ts":
        return "video/MP2T"
    elif ext == ".m4s":
        return "video/iso.segment"
    elif ext == ".m3u8":
        return "application/vnd.apple.mpegurl"
    elif ext == ".mpd":
        return "application/dash+xml"
    else:
        return "application/octet-stream"

def is_segment_file(filename: str) -> bool:
    """Return True if this is a media segment (TS or fMP4), False if playlist/manifest."""
    return filename.endswith((".ts", ".m4s"))  # HLS TS or DASH/fMP4

def PlaylistURL(uuid):
    return os.path.join(baseURL, uuid)


def monitor_inactivity(timeout=InactivityTimeOut):
    while True:
        logger.debug("Monitor_inactivity looped")
        logger.debug(f"processes keys = {list(processes.keys())}")
        now = time.time()

        #monitor if client is requesting segments
        for uuid, session in list(sessions.items()):
             last_time = session["last_time"]
             _ua = session["ua"]
             if time.time() - last_time > timeout:
                # Remove session entry
                del sessions[uuid]
                # Flag uuid -> do not allow sending playlist again for FlagTimeout [seconds] 
                flagged_uuids[uuid] = time.time()
                logger.debug("Client is repeatedly asking for manifest without segment request. Flagged")
                # kill the process if exists
                proc = processes.get(uuid)
                if proc:
                  try:
                    proc.kill()
                    logger.info(f"Killing FFmpeg for {uuid}")
                  except Exception as e:
                    logger.warning(f"Error killing FFmpeg for {uuid}: {e}")
                  del processes[uuid]


        for uuid, proc in list(processes.items()):
            if uuid in last_access:
                logger.debug(f"Timediff = {now - last_access[uuid] > timeout}")
                if now - last_access[uuid] > timeout:
                    logger.info(f"Stopping FFmpeg for channel {uuid} due to inactivity")

                    # Kill FFmpeg
                    try:
                        proc.kill()
                    except Exception as e:
                        logger.warning(f"Error killing FFmpeg for {uuid}: {e}")

                    # Remove tracking entries
                    del processes[uuid]
                    del last_access[uuid]

                    # ---- CLEAN RAM DRIVE FOR THIS CHANNEL ----
                    channel_dir = f"{STREAM_DIR}/{uuid}"
                    if os.path.exists(channel_dir):
                        try:
                            for f in os.listdir(channel_dir):
                                fp = os.path.join(channel_dir, f)
                                if os.path.isfile(fp):
                                    os.remove(fp)
                            logger.info(f"Cleaned RAM directory for channel {uuid}")
                        except Exception as e:
                            logger.warning(f"Could not clean RAM directory for {uuid}: {e}")

        time.sleep(5)


def is_chromecast(ua: str) -> bool:
    return (
        "crkey" in ua or
        "chromecast" in ua or
        "googlecast" in ua
    )


def ua(request: Request) -> str: # returns the user agent and header from requester
    return (
        f"{request.client.host}|{request.headers.get('user-agent', 'unknown')}"
    )


def cleanup_worker_global(base_folder=STREAM_DIR, min_free_bytes=MIN_FREE_BYTES):
    while True:
        now = time.time()
        files = []

        # Recursively collect all deletable segment files
        for root, dirs, filenames in os.walk(base_folder):
            for f in filenames:
                if f.endswith(".ts") or (f.endswith(".m4s") and f.startswith(f"{MPD_ChunkName}-")):
                    files.append(os.path.join(root, f))

        # Sort by modification time (oldest first)
        files.sort(key=lambda x: os.path.getmtime(x))

        # Delete oldest files if RAMDrive is running out of space
        while shutil.disk_usage(base_folder).free < min_free_bytes and files:
            oldest = files.pop(0)
            try:
                os.remove(oldest)
            except FileNotFoundError:
                pass

        # Wait before next scan
        time.sleep(60)  # run once per minute



def cleanup_worker(uuid, lifetime=720):
    folder = f"{STREAM_DIR}/{uuid}"

    while True:
        now = time.time()

        if os.path.exists(folder):
            for f in os.listdir(folder):
                if is_deletable_segment(f):
                    path = os.path.join(folder, f)
                    try:
                        if now - os.path.getmtime(path) > lifetime:
                            os.remove(path)
                    except FileNotFoundError:
                        pass
        time.sleep(30)

def is_deletable_segment(filename: str) -> bool:
# we need to protect the mpd init.m4s segment from deletion
    if filename.endswith(".ts"):
        return True
    if filename.endswith(".m4s") and filename.startswith(f"{MPD_ChunkName}-"):
        return False # because we use -extra_window_size 0 in MPD ffmpeg command -> deletes automatically
    return False


def purge_stream_dir():
    print(f"[STARTUP] Purging {STREAM_DIR} ...")
    if not os.path.exists(STREAM_DIR):
        logger.info("[STARTUP] Directory missing — creating it.")
        os.makedirs(STREAM_DIR, exist_ok=True)
        return

    for name in os.listdir(STREAM_DIR):
        path = os.path.join(STREAM_DIR, name)
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
        except Exception as e:
            logger.warning(f"[STARTUP] Failed removing {path}: {e}")

    logger.info(f"[STARTUP] Finished purging {STREAM_DIR}")

@app.on_event("startup")
def startup_event():
    purge_stream_dir()


threading.Thread(target=monitor_inactivity, daemon=True).start()
