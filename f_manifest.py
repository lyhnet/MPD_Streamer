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


UseGlobalCleaner=True
InactivityTimeOut=20
FlagTimeOut=180          # seconds to keep client banned/flagged for restarting request for specific UUID


#MPD file names
MPD_InitSegName="init"
MPD_ChunkName="chunk"
MPD_Manifest="manifest.mpd"

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

def start_channel_hls(uuid):
    out_dir = f"{STREAM_DIR}/{uuid}"
    os.makedirs(out_dir, exist_ok=True)
    logger.debug(f"Creating directory: {out_dir}")

    # Start a cleanup thread for this channel
    if not UseGlobalCleaner:
      threading.Thread(target=cleanup_worker, args=(uuid,), daemon=True).start()


    # Clean directory
    for f in os.listdir(out_dir):
        os.remove(os.path.join(out_dir, f))

    base = PlaylistURL(uuid)
    #ensure trailing / for -hls to behave
    if not base.endswith("/"):
       base += "/"

    streamURL= SourceStreamURL(uuid)

    cmd = [
        "ffmpeg",
        "-loglevel", "warning",
        "-stats",
        "-f", "mpegts",
        "-fflags", "+discardcorrupt+genpts+igndts+nobuffer",
        "-avoid_negative_ts", "make_zero",
        "-err_detect", "ignore_err",
        "-max_interleave_delta", "0",
        "-probesize", "1M", #"10M",
        "-analyzeduration", "2M",#"10M",
        "-i", f"{streamURL}",

        # Re-encode section, high quality
        "-sn",
        "-map", "0:v:0",
        "-map", "0:a:0",
        "-map", "-0:s",
        #"-vf", "scale=-1:720",
        #"-vf", "yadif=1,scale=-1:720",
        "-vf", "bwdif=mode=0:parity=auto,scale=-1:720", "-r", "25",
        "-pix_fmt", "yuv420p",
        "-c:v", "libx264",
        "-tune", "zerolatency",
        "-x264opts", "keyint=48:min-keyint=1:scenecut=0",
        "-profile:v", "high",
        "-level", "5.1",
        "-preset", "veryfast",
        "-crf", "23",
        "-maxrate", "6000k",
        "-bufsize", "6000k",

        "-c:a", "aac",
        "-ac", "2",
        "-ar", "48000",
        "-b:a", "128k",
        "-g", "100",
        "-keyint_min", "100",

        "-f", "hls",
        "-hls_time", "2",
        "-hls_list_size", "2000",
        "-hls_flags", "independent_segments+delete_segments+program_date_time",
        "-hls_segment_type", "mpegts",
        "-muxdelay", "0.7",
        "-muxpreload", "0.7",
        "-hls_segment_filename", f"{out_dir}/hq_segment-%05d.ts",
        "-hls_base_url", base,
        f"{out_dir}/copy_index.m3u8",

        # Re-encode section, low quality
        # "-sn",
        # "-map", "0:v:0",
        # "-map", "0:a:0",
        # "-map", "-0:s",

        # "-vf", "yadif=1,scale=-1:720",
        # "-pix_fmt", "yuv420p",
        # "-c:v", "libx264",
        # "-profile:v", "high",
        # "-level", "5.1",
        # "-preset", "ultrafast",
        # "-crf", "23",
        # "-maxrate", "3000k",
        # "-bufsize", "6000k",

        # "-c:a", "aac",
        # "-ac", "2",
        # "-ar", "48000",
        # "-b:a", "128k",
        # "-g", "100",
        # "-keyint_min", "100",

        # "-f", "hls",
        # "-hls_time", "2",
        # "-hls_list_size", "20",
        # "-hls_flags", "independent_segments+delete_segments+program_date_time",
        # "-hls_segment_type", "mpegts",
        # "-hls_segment_filename", f"{out_dir}/lq_segment-%05d.ts",
        # "-hls_base_url", base,
        # f"{out_dir}/reenc_index.m3u8"


    #  "-force_key_frames", "expr:gte(t,n_forced*1)",
    #    "-muxdelay", "0",
    #    "-muxpreload", "0",
            #"-hls_flags", "append_list+omit_endlist",
    # "-hls_base_url", base,
    ]

    print("Starting FFmpeg:", " ".join(cmd))
    return subprocess.Popen(cmd)

def start_channel_mpd(uuid):
    out_dir = f"{STREAM_DIR}/{uuid}"
    os.makedirs(out_dir, exist_ok=True)
    logger.debug(f"Creating directory: {out_dir}")

    # Start a cleanup thread for this channel
    if not UseGlobalCleaner:
      threading.Thread(target=cleanup_worker, args=(uuid,), daemon=True).start()

    # Clean directory before start
    for f in os.listdir(out_dir):
        os.remove(os.path.join(out_dir, f))

    streamURL= SourceStreamURL(uuid)

    # FFmpeg LIVE DASH command
    cmd = [
        "ffmpeg",
        "-loglevel", "warning",
        "-stats",
        "-fflags", "+discardcorrupt+genpts+igndts",
        "-avoid_negative_ts", "make_zero",
        "-err_detect", "ignore_err",
        "-max_interleave_delta", "0",
        "-probesize", "10M",
        "-analyzeduration", "10M",
        "-i", f"{streamURL}",

        # Re-encode section, high quality
        "-sn",
        "-map", "0:v:0",
        "-map", "0:a:0",
        "-map", "-0:s",
        "-vf", "setfield=tff,scale=-1:720",
        #"-vf", "yadif=1,scale=-1:720",
        #"-vf", "bwdif=mode=0:parity=auto,scale=-1:720",
        "-pix_fmt", "yuv420p",
        "-c:v", "libx264",
        "-tune", "zerolatency",
        "-x264opts", "keyint=48:min-keyint=1:scenecut=0",
        "-profile:v", "high",
        "-level", "5.1",
        "-preset", "veryfast",
        "-crf", "21",
        "-maxrate", "6000k",
        "-bufsize", "6000k",
        "-vsync", "cfr",

        # Audio encoding
        "-c:a", "aac",
        "-af", "aresample=async=1:first_pts=0",
        "-ac", "2",
        "-ar", "48000",
        "-b:a", "128k",
        "-g", "100",
        "-keyint_min", "100",

        # DASH live settings
        "-f", "dash",
        "-window_size", "2000", #time shift number of segments.
        "-extra_window_size", "0", #delete older segments
        "-init_seg_name", f"{MPD_InitSegName}--$RepresentationID$.m4s",
        "-media_seg_name", f"{MPD_ChunkName}-$RepresentationID$-$Number%03d$.m4s",
        "-seg_duration", "2",
        "-use_template", "1",
        "-use_timeline", "1",
        "-remove_at_exit", "1",

        f"{out_dir}/{MPD_Manifest}"
    ]

    logger.info(f"Starting FFmpeg: {' '.join(cmd)}")
    return subprocess.Popen(cmd)


@app.get("/stream/{uuid}/index.m3u8")
def serve_manifest_mpd(uuid: str, request: Request):
    out_dir = f"{STREAM_DIR}/{uuid}"
    hls_path = f"{out_dir}/copy_index.m3u8"
    hls_path_main = f"{out_dir}/index.m3u8"
    logger.debug(f"Asking for index.m3u8 for UUID = : {uuid}")
    plu = PlaylistURL(uuid) #playlist url


    
    client_id = ua(request)
    logger.debug(f"client_id = {client_id}")

    # check if this uuid is flagged
    if (uuid) in flagged_uuids:
        # uuid was in the flagged_uuids list
        logger.info(f"UUID is flagged: {uuid}")
        flagged_time = flagged_uuids[uuid]
        elapsed = time.time() - flagged_time
        if elapsed < FlagTimeOut:  # still in cooldown
            raise HTTPException(status_code=410, detail="Stream ended for this client")
        else:
            # cooldown expired -> remove from flagged
            del flagged_uuids[uuid]
            logger.info(f"UUID is no longer flagged: {uuid}")


    # Start ffmpeg if not running
    if uuid not in processes or processes[uuid].poll() is not None:
        logger.info(f"Creating new process with UUID = : {uuid}")
        processes[uuid] = start_channel_hls(uuid)
        # Set the last_access to initialise it. Otherwise it is not present in the inactivity monitor and it will not work
        logger.debug(f"And set last_access for for UUID = {uuid}")
        last_access[uuid] = time.time()
        # Set the session for client requesting the UUID
        _ua = ua(request)
        sessions[uuid] = { "last_time": time.time(), "ua": _ua,} #track last segment request (init with manifest request)
        logger.info(f"Inital request for index.m3u8 for UUID = : {uuid}")

    # Wait up to 15 seconds for ffmpeg to generate a REAL mpd
    # A real mpd has size > 500 bytes typically
    start = time.time()
    for _ in range(300):  # 150 × 0.1 sec = 15 seconds
        if os.path.exists(hls_path) and os.path.getsize(hls_path) > 200:
          # Get file extension
          create_master_playlist(hls_path_main, plu)
          _, ext = os.path.splitext(hls_path_main)
          return FileResponse(hls_path_main, media_type=get_MIME_Type(ext))
        if _ % 10 == 0:
          logger.debug(f"waited {time.time() - start:.1f}s for HLS")
        time.sleep(0.1)

    # If still no real m3u8 → fail properly (player will retry)
    logger.info("M3U8 (HLS) not created in due course")
    raise HTTPException(status_code=503, detail="M3U8 (HLS) not ready")


@app.get("/stream/{uuid}/manifest.mpd")
def serve_manifest_mpd(uuid: str, request: Request):
    out_dir = f"{STREAM_DIR}/{uuid}"
    mpd_path = f"{out_dir}/manifest.mpd"
    logger.debug(f"Asking for manifest.mpd for UUID = : {uuid}")

    client_id = f"{request.client.host}|{request.headers.get('user-agent', 'unknown')}"
    logger.debug(f"client_id = {client_id}")

    # check if this client has recently had this stream killed
    # some times chromecast just asks again after timeout. Need to tell it that resources expired 
    # hopefully this will stop requests for manifest
    if uuid in flagged_uuids:
        # UUID in the flagged_uuids list
        logger.info(f"UUID is flagged: {uuid}")
        flagged_time = flagged_uuids[uuid]
        elapsed = time.time() - flagged_time
        if elapsed < FlagTimeOut:  # still in cooldown
            raise HTTPException(status_code=410, detail="Stream ended for this client")
        else:
            # cooldown expired -> remove from flagged
            del flagged_uuids[uuid]
            logger.info(f"UUID is no longer flagged: {uuid}")


    # Start ffmpeg if not running
    if uuid not in processes or processes[uuid].poll() is not None:
        logger.info(f"Creating new process with UUID = : {uuid}")
        processes[uuid] = start_channel_mpd(uuid)
        # Set the last_access to initialise it. Otherwise it is not present in the inactivity monitor and it will not work
        logger.debug(f"And set last_access for for UUID = {uuid}")
        last_access[uuid] = time.time()
        # Set the session for client requesting the UUID
        _ua = ua(request)
        sessions[uuid] = { "last_time": time.time(), "ua": _ua,} #track last segment request (init with manifest request)
        logger.info(f"Inital request for manifest.mpd for UUID = : {uuid}")

    # Wait up to 15 seconds for ffmpeg to generate a REAL mpd
    # A real mpd has size > 500 bytes typically
    for _ in range(150):  # 150 × 0.1 sec = 15 seconds
        if os.path.exists(mpd_path) and os.path.getsize(mpd_path) > 500:
          # Get file extension
          _, ext = os.path.splitext(mpd_path)
          return FileResponse(mpd_path, media_type=get_MIME_Type(ext))
        time.sleep(0.1)

    # If still no real MPD → fail properly (player will retry)
    logger.info("MPD not created in due course")
    raise HTTPException(status_code=503, detail="MPD not ready")



    

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



import os
from fastapi.responses import FileResponse

def create_master_playlist(pth, plu):
    """
    Create an HLS master playlist (index.m3u8) inside directory `pth`.

    Args:
        pth (str): Directory path where index.m3u8 will be created
        plu (str): Relative path prefix for variant playlists
    """
    

    content = f"""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-INDEPENDENT-SEGMENTS
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
{plu}/copy_index.m3u8
"""
    master_file = pth
    tmp = master_file + ".tmp"
    with open(tmp, "w") as f:
        f.write(content)
    os.replace(tmp, master_file)
    logger.info(f"{master_file} created successfully")






def create_master_playlist_mpd(pth, plu):
#creates the adaptive streaming mpd runtime
# more  Representations can be added if ffmpeg makes several quality levels
# mpeg-dash does not use sup-playlist like HLS that has the Master m3u8 with references to child m3u8's for each quality
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<MPD
    xmlns="urn:mpeg:dash:schema:mpd:2011"
    profiles="urn:mpeg:dash:profile:isoff-live:2011"
    type="dynamic"
    minimumUpdatePeriod="PT2S"
    minBufferTime="PT2S"
    timeShiftBufferDepth="PT60S"
    availabilityStartTime="1970-01-01T00:00:00Z">

  <Period id="1" start="PT0S">

    <AdaptationSet
        mimeType="video/mp4"
        codecs="avc1.640028"
        segmentAlignment="true"
        startWithSAP="1">

      <!-- Copy / original resolution -->
      <Representation
          id="copy"
          bandwidth="5000000"
          width="1920"
          height="1080"
          frameRate="25">
        <BaseURL>{plu}</BaseURL>
        <SegmentTemplate
            initialization="init.mp4"
            media="segment-$Number$.m4s"
            startNumber="1"
            duration="2"/>
      </Representation>
    </AdaptationSet>

  </Period>
</MPD>
"""
    master_file = pth
    with open(master_file, "w") as f:
        f.write(content)
    logger.info(f"{master_file} created successfully")

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
