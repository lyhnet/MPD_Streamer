
#!/bin/bash

# Navigate to the folder where your Python file is
cd /var/www/html/MPD_Streamer

# set python source
source venv/bin/activate
# Start Uvicorn
python3 f_manifest.py
