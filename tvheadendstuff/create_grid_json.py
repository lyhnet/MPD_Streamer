import requests
import json
from urllib.parse import urljoin
import os
from urllib.parse import urljoin
# Replace this with your TVHeadend/Kodi endpoint


TVH_URL="http://hts:hts@kodi.lyhnemail.com:19981"
API_URL = f"{TVH_URL}/api/channel/grid"
PlayURL = f"{TVH_URL}/play/ticket/stream/channel/{{uuid}}"
BaseURL = "https://lyhnemail.com/streamer/"

def create_url(uuid):
    return PlayURL.format(uuid=uuid)


try:
    # Fetch the data
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    # Convert into structured CHANNELS format with playback URL
    entries = data.get("entries", [])

    def _num_val(n):
        try:
            return int(n)
        except (TypeError, ValueError):
            try:
                return int(float(n))
            except (TypeError, ValueError):
                return float("inf")

    sorted_entries = sorted(entries, key=lambda e: _num_val(e.get("number")))

    CHANNELS = []
    for idx, entry in enumerate(sorted_entries, start=1):
        CHANNELS.append({
            "id": entry["uuid"],
            "number": entry.get("number"),
            "channelnumber": idx,
            "name": entry.get("name"),
            "logo": f"{BaseURL.rstrip('/')}/picons/{entry.get('icon_public_url', '').lstrip('/')}",
            "url": create_url(entry["uuid"])
        })

    # Save to JSON file
    with open("channels.json", "w") as f:
        json.dump(CHANNELS, f, indent=4)

    print("Channels saved to channels.json:")
    print(json.dumps(CHANNELS, indent=4))

except requests.exceptions.RequestException as e:
    print("Error fetching data:", e)
except json.JSONDecodeError as e:
    print("Error decoding JSON:", e)
