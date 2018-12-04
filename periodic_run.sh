#!/bin/bash

if command -v python3 &>/dev/null; then
  python_command="python3" 
else
  python_command="python" 
fi

echo "using ${python_command} as python command"

while ${python_command} -m twitch_api_worker; do 
  sleep 600 
  echo "Waiting 10 minutes..."
done
