#!/bin/bash

# Set the default Python script path
default_python_script_path="/home/ebedber/bgf_test/data_export/consumer_script.py"

# Check if the Python script path is provided as an argument
if [ $# -eq 0 ]; then
  python_script_path=$default_python_script_path
else
  python_script_path=$1
fi

# Add a cron job to execute the Python script daily
(crontab -l 2>/dev/null; echo "0 3 * * * python3.6 $python_script_path") | crontab -

# Display confirmation message
echo "Python script scheduled to run daily using path: $python_script_path"
