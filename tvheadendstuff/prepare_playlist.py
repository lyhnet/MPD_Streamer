import requests
import re
from dotenv import load_dotenv
import os
import json
import sys
from pathlib import Path


# 1. Get the parent folder of the current file
parent_folder = Path(__file__).resolve().parent.parent

# 2. Load .env from parent folder
load_dotenv(dotenv_path=parent_folder / ".env")

# 3. Read environment variables
EPG_URL = os.getenv("XMLTV")
OUTPUT = "../streamer.m3u"
STREAM_BASE =  os.getenv("baseURL") 




# 4. Load channels.json
def load_channelsjson(jsonpath: Path, channels: json):
    CHANNELS_FILE = jsonpath
    try:
        with CHANNELS_FILE.open("r", encoding="utf-8") as f:
            CHANNELS = json.load(f)
    except FileNotFoundError:
        print(f"Error: '{CHANNELS_FILE}' not found. Have you run 'create_grid_json.py'?")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse '{CHANNELS_FILE}': {e}")
        print("Make sure 'create_grid_json.py' has been run successfully.")
        sys.exit(1)
    print(f"Loaded {len(CHANNELS)} channels from {CHANNELS_FILE}")





def select_stream_format():
    while True:
        print("Select stream format:")
        print("  1) HLS (index.m3u8)")
        print("  2) MPEG-DASH (manifest.mpd)")
        choice = input("Enter choice [1/2]: ").strip()

        if choice == "1":
            return "index.m3u8"
        elif choice == "2":
            return "manifest.mpd"
        else:
            print("Invalid choice, try again.\n")



def generate_playlist():
    segment_index = select_stream_format()

    JSONPath = Path("channels.json")
    CHANNELS = []


    load_channelsjson(JSONPath, CHANNELS)
    # Build m3u from CHANNELS (fallback to reading JSONPath if CHANNELS empty)
    if not CHANNELS:
        try:
            with JSONPath.open("r", encoding="utf-8") as f:
                CHANNELS = json.load(f)
        except Exception as e:
            print("Failed to load channels.json:", e)
            sys.exit(1)

    m3u_lines = ["#EXTM3U", f"#EXTXMLTV: {EPG_URL}"]

    for ch in CHANNELS:
        cid = ch.get("id", "")
        name = ch.get("name", "CH-UNKNOWN")
        chno = ch.get("channelnumber", ch.get("number"))
        chno_attr = f' tvg-chno="{chno}"' if chno is not None else ""
        extinf = f'#EXTINF:-1 tvg-id="{cid}" tvg-name="{name}"{chno_attr} group-title="TV",{name}'
        stream_url = f'{STREAM_BASE.rstrip("/")}/stream/{cid}/{segment_index}'
        m3u_lines.append(extinf)
        m3u_lines.append(stream_url)

    m3u = "\n".join(m3u_lines) + "\n"


    out = m3u.splitlines()

    # Write file
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write("\n".join(out))

    print("Saved:", OUTPUT)


if __name__ == "__main__":
    generate_playlist()
