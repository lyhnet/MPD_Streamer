import requests
import re

TVH_M3U_URL = "http://kodi.lyhnemail.com:19981/playlist/channels.m3u"
EPG_URL = "https://kodi.lyhnemail.com/epg.xml"
OUTPUT = "streamer.m3u"

STREAM_BASE = "https://kodi.lyhnemail.com/streamer/stream/"

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

    print("Downloading TVH M3U...")
    m3u = requests.get(TVH_M3U_URL).text

    out = ["#EXTM3U", f"#EXTXMLTV: {EPG_URL}"]

    # Grab each EXTINF block + following URL
    entries = re.findall(r'(#EXTINF:-1[^\n]*\n)([^\n]+\n)', m3u, re.MULTILINE)

    for info_line, url_line in entries:
        info = info_line.strip()
        url = url_line.strip()
        print("INFO:", info)
        print("URL :", url)

        # 1) Preserve original tvg-id if present (hex uuid in your input)
        tvg_id_match = re.search(r'tvg-id="([^"]+)"', info)
        orig_tvg_id = tvg_id_match.group(1) if tvg_id_match else None

        # 2) Preserve tvg-chno if present
        chno_match = re.search(r'tvg-chno="([^"]+)"', info)
        tvg_chno = chno_match.group(1) if chno_match else None

        # 3) Extract display name: the text after the last comma on the EXTINF line
        #    e.g. '#EXTINF:-1 ... ,DR1 HD' -> 'DR1 HD'
        disp_match = re.search(r',\s*([^,\n]+)\s*$', info)
        display_name = disp_match.group(1).strip() if disp_match else None

        # If no display name, try tvg-name attribute, else fallback to placeholder
        tvg_name_attr = re.search(r'tvg-name="([^"]+)"', info)
        display_name = display_name or (tvg_name_attr.group(1) if tvg_name_attr else f"CH-UNKNOWN")

        # 4) Extract numeric channel id from URL (the TVHeadend numeric id)
        channelid_match = re.search(r'/channelid/(\d+)', url)
        if not channelid_match:
            print("No channelid found in URL, skipping:", url)
            continue

        full_channel_id = channelid_match.group(1)
        # Trim last digit if required by your streamer (TVH sometimes appends PID).
        #short_channel_id = full_channel_id[:-1]
        # If your streamer expects the full id, comment out the line above and set:
        short_channel_id = full_channel_id

        # Build new HLS URL
        stream_url = f"{STREAM_BASE}{short_channel_id}/{segment_index}"
        # Choose the tvg-id to write: prefer original hex id, otherwise fall back to numeric id
        write_tvg_id = orig_tvg_id if orig_tvg_id else full_channel_id

        # Build EXTINF line (include tvg-chno if we have it)
        chno_fragment = f' tvg-chno="{tvg_chno}"' if tvg_chno else ""
        extinf = (f'#EXTINF:-1 tvg-id="{write_tvg_id}" tvg-name="{display_name}"'
                  f'{chno_fragment} group-title="TV",{display_name}')

        # Append to output
        out.append(extinf)
        out.append(stream_url)

    # Write file
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write("\n".join(out))

    print("Saved:", OUTPUT)


if __name__ == "__main__":
    generate_playlist()
