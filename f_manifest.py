from fastapi import FastAPI
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

app = FastAPI(root_path="/streamer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

baseURL = "https://kodi.lyhnemail.com/streamer/stream"
STREAM_DIR = "/tmp/ramdrive/stream"
processes = {}          # channel → Popen
last_access = {}        # channel → timestamp
sessions = {}           # (client_id, uuid) -> timestamp of last killed stream
flagged_clients = {}    # (client_id, uuid) => timed out client : kicked due to no segment request

MIN_FREE_BYTES=200000000

MPD=True
kodiURL="http://kodi.lyhnemail.com"
kodiPort="19981"

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


def start_channel_hls(uuid):
    out_dir = f"{STREAM_DIR}/{uuid}"
    #print("Creating directory:", out_dir)
    #os.makedirs(out_dir, exist_ok=True)

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

    streamURL= f"{kodiURL.rstrip('/')}:{kodiPort}/stream/channelid/{uuid}?profile=pass"

    cmd = [
    "ffmpeg",
    "-loglevel", "warning",
    "-stats",
    "-fflags", "+discardcorrupt+genpts+igndts",
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
    "-vf", "scale=-1:720",
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

    "-c:a", "aac",
    "-ac", "2",
    "-ar", "48000",
    "-b:a", "128k",
    "-g", "100",
    "-keyint_min", "100",

    "-f", "hls",
    "-hls_time", "2",
    "-hls_list_size", "20",
    "-hls_flags", "independent_segments+delete_segments+program_date_time",
    "-hls_segment_type", "mpegts",
    "-hls_segment_filename", f"{out_dir}/hq_segment-%05d.ts",
    "-hls_base_url", base,
    f"{out_dir}/copy_index.m3u8",



    # Re-encode section, low quality
    "-sn",
    "-map", "0:v:0",
    "-map", "0:a:0",
    "-map", "-0:s",

    "-vf", "yadif=1,scale=-1:720",
    "-pix_fmt", "yuv420p",
    "-c:v", "libx264",
    "-profile:v", "high",
    "-level", "5.1",
    "-preset", "ultrafast",
    "-crf", "23",
    "-maxrate", "3000k",
    "-bufsize", "6000k",

    "-c:a", "aac",
    "-ac", "2",
    "-ar", "48000",
    "-b:a", "128k",
    "-g", "100",
    "-keyint_min", "100",

    "-f", "hls",
    "-hls_time", "2",
    "-hls_list_size", "20",
    "-hls_flags", "independent_segments+delete_segments+program_date_time",
    "-hls_segment_type", "mpegts",
    "-hls_segment_filename", f"{out_dir}/lq_segment-%05d.ts",
    "-hls_base_url", base,
    f"{out_dir}/reenc_index.m3u8"
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

    # Start a cleanup thread for this channel
    if not UseGlobalCleaner:
      threading.Thread(target=cleanup_worker, args=(uuid,), daemon=True).start()

    # Clean directory before start
    for f in os.listdir(out_dir):
        os.remove(os.path.join(out_dir, f))

    streamURL= f"{kodiURL.rstrip('/')}:{kodiPort}/stream/channelid/{uuid}?profile=pass"

    # FFmpeg LIVE DASH command
    cmd = [
        "ffmpeg",
        "-loglevel", "warning",
        "-stats",
        "-fflags", "+discardcorrupt+genpts+igndts",
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

        # Audio encoding
        "-c:a", "aac",
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
        "-media_seg_name", f"{MPD_ChunkName}-$RepresentationID$-$Number$.m4s",
        "-seg_duration", "2",
        "-use_template", "1",
        "-use_timeline", "1",
        "-remove_at_exit", "1",

        f"{out_dir}/{MPD_Manifest}"
    ]

    print("Starting FFmpeg:", " ".join(cmd))
    return subprocess.Popen(cmd)

@app.get("/stream/{uuid}/index.m3u8")
def serve_manifest_hls(uuid: str):
    out_dir = f"{STREAM_DIR}/{uuid}"
    mpd_path = f"{out_dir}/reenc_index.m3u8"
    mpd_path_adaptive = f"{out_dir}/index.m3u8"
    print("Creating directory:", out_dir)
    os.makedirs(out_dir, exist_ok=True)
    plu = PlaylistURL(uuid) #playlist url
    create_master_playlist(mpd_path_adaptive, plu)
#    os.makedirs(out_dir, exist_ok=True)

    # Start ffmpeg if not running
    if uuid not in processes or processes[uuid].poll() is not None:
        processes[uuid] = start_channel_hls(uuid)

    # Wait up to 8 seconds for ffmpeg to generate a REAL mpd
    # A real mpd has size > 500 bytes typically
    for _ in range(150):  # 150 × 0.1 sec = 15 seconds
        if os.path.exists(mpd_path) and os.path.getsize(mpd_path) > 500:
          return FileResponse(mpd_path_adaptive, media_type="application/vnd.apple.mpegurl")
        time.sleep(0.1)

    # If still no real MPD → fail properly (player will retry)
    raise HTTPException(status_code=503, detail="MPD not ready")


@app.get("/stream/{uuid}/manifest.mpd")
def serve_manifest_mpd(uuid: str, request: Request):
    out_dir = f"{STREAM_DIR}/{uuid}"
    mpd_path = f"{out_dir}/manifest.mpd"
    print("Asking for manifest.mpd for UUID = :", uuid)
#    os.makedirs(out_dir, exist_ok=True)


    client_id = f"{request.client.host}|{request.headers.get('user-agent', 'unknown')}"

    # check if this client has recently had this stream killed
    # some times chromecast just asks again after timeout. Need to tell it that resources expired 
    # hopefully this will stop requests for manifest
    if (client_id, uuid) in flagged_clients:
        # client was in the list for this UUID
        flagged_time = flagged_clients[(client_id, uuid)]
        elapsed = time.time() - flagged_time
        if elapsed < FlagTimeOut:  # still in cooldown
            raise HTTPException(status_code=410, detail="Stream ended for this client")
        else:
            # cooldown expired -> remove from flagged
            del flagged_clients[(client_id, uuid)]



    # Start ffmpeg if not running
    if uuid not in processes or processes[uuid].poll() is not None:
        print("Creating new process with UUID = :", uuid)
        processes[uuid] = start_channel_mpd(uuid)
        # Set the last_access to initialise it. Otherwise it is not present in the inactivity monitor and it will not work
        print("And set last_access for for UUID = :", uuid) 
        last_access[uuid] = time.time()
        # Set the session for client requesting the UUID
        sessions[(client_id, uuid)] = time.time() #track last segment request (init with manifest request)

    # Wait up to 8 seconds for ffmpeg to generate a REAL mpd
    # A real mpd has size > 500 bytes typically
    for _ in range(150):  # 150 × 0.1 sec = 15 seconds
        if os.path.exists(mpd_path) and os.path.getsize(mpd_path) > 500:
          return FileResponse(mpd_path, media_type="application/dash+xml")
        time.sleep(0.1)

    # If still no real MPD → fail properly (player will retry)
    raise HTTPException(status_code=503, detail="MPD not ready")

@app.get("/stream/{uuid}/{segment}")
def serve_segment(uuid: str, segment: str, request: Request):
    path = f"{STREAM_DIR}/{uuid}/{segment}"

    if not os.path.isfile(path):
        print(f"Requested file not found: {path}")
        raise HTTPException(status_code=404, detail="Segment not found")

    # mark activity time
    # Only update last_access for actual segments, not playlists
    if is_segment_file(segment):
      last_access[uuid] = time.time()
      client_id = f"{request.client.host}|{request.headers.get('user-agent', 'unknown')}"
      sessions[(client_id, uuid)] = time.time() #track last segment request
    else:
      print("Non-segment requested")

    # Set proper MIME type
    if segment.endswith(".ts"):
        media_type = "video/MP2T"
    elif segment.endswith(".m4s"):
        media_type = "video/iso.segment"
    elif segment.endswith(".m3u8"):
        media_type = "application/vnd.apple.mpegurl"
    elif segment.endswith(".mpd"):
        media_type = "application/dash+xml"
    else:
        media_type = "application/octet-stream"


    return FileResponse(path, media_type=media_type)

    raise HTTPException(status_code=404, detail="Segment not found")

def is_segment_file(filename: str) -> bool:
    """Return True if this is a media segment (TS or fMP4), False if playlist/manifest."""
    return filename.endswith((".ts", ".m4s"))  # HLS TS or DASH/fMP4

def PlaylistURL(uuid):
    return os.path.join(baseURL, uuid)


def create_master_playlist(pth, plu):
#creates the adaptive streaming m3u8 runtime
    """
   Create an HLS master playlist file at the given path.
    The path is also prefixed to the variant playlist filenames.
    
    Args:
        pth (str): Path to the output master.m3u8 file AND folder prefix for variants.
    """
    content = f"""#EXTM3U
#EXT-X-VERSION:3

# Copy variant (original resolution)
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
{plu}/copy_index.m3u8

# Transcoded 720p variant
#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720
{plu}/reenc_index.m3u8
"""
    master_file = pth
    with open(master_file, "w") as f:
        f.write(content)
    print(f"{master_file} created successfully")


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
    print(f"{master_file} created successfully")

def monitor_inactivity(timeout=InactivityTimeOut):
    while True:
        print("Monitor_inactivity looped")
        print(f"processes keys = {list(processes.keys())}")
        now = time.time()

        #monitor if client is requesting segments
        for (client_id, uuid), last_time in list(sessions.items()):
            if time.time() - last_time > timeout:
                # Remove session entry
                del sessions[(client_id, uuid)]
                # Flag this client+uuid to not send more mpds. prevent immediate restart            stopped_clients[(client_id, uuid)] = time.time()
                flagged_clients[(client_id, uuid)] = time.time()
                print(f"Client is repeatedly asking for manifest without segment request. Flagged")
                # Optional: if no other client is active, kill the FFmpeg process
                if not any(u == uuid for (_, u) in sessions.keys()):
                    proc = processes.get(uuid)
                    if proc:
                        try:
                            proc.kill()
                        except Exception as e:
                            print(f"Error killing FFmpeg for {uuid}: {e}")
                        del processes[uuid]


        for uuid, proc in list(processes.items()):
            if uuid in last_access:
                print(f"Timediff = {now - last_access[uuid] > timeout}")
                if now - last_access[uuid] > timeout:
                    print(f"Stopping FFmpeg for channel {uuid} due to inactivity")

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
                            print(f"Cleaned RAM directory for channel {uuid}")
                        except Exception as e:
                            print(f"Could not clean RAM directory for {uuid}: {e}")

        time.sleep(5)


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
        print("[STARTUP] Directory missing — creating it.")
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
            print(f"[STARTUP] Failed removing {path}: {e}")

    print(f"[STARTUP] Finished purging {STREAM_DIR}")

@app.on_event("startup")
def startup_event():
    purge_stream_dir()


threading.Thread(target=monitor_inactivity, daemon=True).start()
