import requests
import json

# Replace this with your TVHeadend/Kodi endpoint
URL = "http://hts:hts@kodi.lyhnemail.com:19981/api/channel/grid"  # or the URL you have
PlayURL="http://hts:hts@kodi.lyhnemail.com:19981/stream/channelid/{uuid}?profile=pass"


def create_url(uuid):
    return PlayURL.format(uuid=uuid)

try:
    # Fetch the data
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    # Convert into structured CHANNELS format with playback URL
    CHANNELS = [
        {
            "id": entry["uuid"],
            "name": entry["name"],
            "logo": entry.get("icon"),
            #"group": entry.get("bouquet", ""),
            "url": create_url(entry["uuid"])
        }
        for entry in data.get("entries", [])
    ]

    # Save to JSON file
    with open("channels.json", "w") as f:
        json.dump(CHANNELS, f, indent=4)

    print("Channels saved to channels.json:")
    print(json.dumps(CHANNELS, indent=4))

except requests.exceptions.RequestException as e:
    print("Error fetching data:", e)
except json.JSONDecodeError as e:
    print("Error decoding JSON:", e)
