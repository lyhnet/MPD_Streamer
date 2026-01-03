#!/bin/bash
# setup_mpd_streamer.sh
# This script sets up MPD_Streamer with venv and systemd service

PROJECT_DIR="/var/www/html/MPD_Streamer"
VENV_DIR="$PROJECT_DIR/venv"
SERVICE_FILE="mpd.service"
SYSTEMD_DIR="/etc/systemd/system"

# Step 0: Confirm
echo "This will setup MPD_Streamer in $PROJECT_DIR"
read -p "Proceed? [y/N]: " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborting."
    exit 1
fi

# Step 1: Create virtual environment
read -p "Create Python virtual environment in $VENV_DIR? [y/N]: " confirm
if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
    python3 -m venv "$VENV_DIR"
    echo "Virtual environment created at $VENV_DIR"
fi

# Step 2: Activate venv and install packages
read -p "Install required Python packages in venv? [y/N]: " confirm
if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
    source "$VENV_DIR/bin/activate"
    pip install --upgrade pip
    pip install fastapi uvicorn python-dotenv pydantic
    deactivate
    echo "Packages installed in venv"
fi

# Step 3: Prepare run_server.py
RUN_SERVER="$PROJECT_DIR/run_server.py"
if [[ ! -f "$RUN_SERVER" ]]; then
    read -p "Create run_server.py entrypoint? [y/N]: " confirm
    if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
        cat > "$RUN_SERVER" <<EOF
import uvicorn
from f_manifest import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
EOF
        echo "run_server.py created"
    fi
fi

# Step 4: Copy systemd service
read -p "Copy systemd service file ($SERVICE_FILE) to $SYSTEMD_DIR? [y/N]: " confirm
if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
    if [[ ! -f "$PROJECT_DIR/$SERVICE_FILE" ]]; then
        echo "Service file $PROJECT_DIR/$SERVICE_FILE not found. Aborting."
        exit 1
    fi
    sudo cp "$PROJECT_DIR/$SERVICE_FILE" "$SYSTEMD_DIR/"
    echo "Service file copied"

    # Update ExecStart to use venv Python
    sudo sed -i "s|ExecStart=.*|ExecStart=$VENV_DIR/bin/python $PROJECT_DIR/run_server.py|" "$SYSTEMD_DIR/$SERVICE_FILE"

    # Reload systemd daemon
    sudo systemctl daemon-reload

    # Enable service
    read -p "Enable MPD_Streamer service to start on boot? [y/N]: " confirm
    if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
        sudo systemctl enable "$SERVICE_FILE"
        echo "Service enabled"
    fi

    # Start service now
    read -p "Start MPD_Streamer service now? [y/N]: " confirm
    if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
        sudo systemctl start "$SERVICE_FILE"
        sudo systemctl status "$SERVICE_FILE" --no-pager
    fi
fi

echo "Setup complete."
