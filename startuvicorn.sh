
#!/bin/bash

# Navigate to the folder where your Python file is
cd /var/www/html/

# Start Uvicorn
uvicorn f_manifest:app --host 0.0.0.0 --port 5000
