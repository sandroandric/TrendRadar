#!/bin/bash

# Get the absolute path to the project directory
PROJECT_DIR="/Users/sandroandric/trends"

# Navigate to the project directory
cd "$PROJECT_DIR" || exit

# Activate virtual environment if it exists (optional, but good practice)
# source venv/bin/activate

# Run the python script
/usr/bin/python3 main.py >> "$PROJECT_DIR/cron.log" 2>&1
