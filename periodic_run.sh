#!/bin/bash

while python3 -m twitch_api_worker; do sleep 20; done
