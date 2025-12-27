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
    level=logging.INFO,  # default level
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
sessions = {}           # (client_id, uuid) -> [timestamp of last killed stream, ua]
flagged_clients = {}    # (client_id, uuid) => timed out client : kicked due to no segment request


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

    streamURL= f"{TVHURL.rstrip('/')}:{TVHPort}/stream/channelid/{uuid}?profile=pass"

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
    if (client_id, uuid) in flagged_clients:
        # client was in the list for this UUID
        logger.info(f"Client is flagged {client_id} for uuid = {uuid}")
        flagged_time = flagged_clients[(client_id, uuid)]
        elapsed = time.time() - flagged_time
        if elapsed < FlagTimeOut:  # still in cooldown
            raise HTTPException(status_code=410, detail="Stream ended for this client")
        else:
            # cooldown expired -> remove from flagged
            del flagged_clients[(client_id, uuid)]
            logger.info(f"client is no longer flagged {client_id} for uuid = {uuid}")


    # Start ffmpeg if not running
    if uuid not in processes or processes[uuid].poll() is not None:
        logger.info(f"Creating new process with UUID = : {uuid}")
        processes[uuid] = start_channel_mpd(uuid)
        # Set the last_access to initialise it. Otherwise it is not present in the inactivity monitor and it will not work
        logger.debug(f"And set last_access for for UUID = {uuid}")
        last_access[uuid] = time.time()
        # Set the session for client requesting the UUID
        ua = request.headers.get("user-agent", "").lower()
        sessions[(client_id, uuid)] = { "last_time": time.time(), "ua": ua,} #track last segment request (init with manifest request)
        logger.info(f"Inital request for manifest.mpd for UUID = : {uuid}")

    # Wait up to 8 seconds for ffmpeg to generate a REAL mpd
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
    "/player2/",
    StaticFiles(directory="player2/", html=True),
    name="player"
)


@app.get("/stream/{uuid}/{segment}")
def serve_segment(uuid: str, segment: str, request: Request):
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
        key = (client_id, uuid)
        session = sessions.get(key)
        if session:
            session["last_time"] = time.time()  # track last segment request

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
        for (client_id, uuid), session in list(sessions.items()):
             last_time = session["last_time"]
             ua = session["ua"]
             if time.time() - last_time > timeout:
                # Remove session entry
                del sessions[(client_id, uuid)]
                # Flag this client+uuid to not send more mpds. prevent immediate restart 
                # but only flag Google Cast for now. 
                if is_chromecast(ua):
                    flagged_clients[(client_id, uuid)] = time.time()
                    logger.debug("Client is repeatedly asking for manifest without segment request. Flagged")
                # Optional: if no other client is active, kill the FFmpeg process
                if not any(u == uuid for (_, u) in sessions.keys()):
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
                    except:
                        pass

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
