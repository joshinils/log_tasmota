#!/usr/bin/env bash

# to be invoked by cron

# make callable from anywhere
cd "$( dirname "${BASH_SOURCE[0]}" )" || exit

# check if a screen session with name "tasmota_log" exists,
# if not, create it

echo $RANDOM > /dev/null
if ! screen -list | grep -q "tasmota_log"; then
    # sleep random amount between 0.1 and 0.9 seconds
    sleep "$(echo "$(shuf -i 1000-9000 -n 1)/10000" | bc -l)"
else
    exit 1
fi


if ! screen -list | grep -q "tasmota_log"; then
    screen -dm -S tasmota_log /usr/bin/zsh -c "./main.py 2>&1 | tee -a log.log"
fi


if screen -list | grep "tasmota_log" | grep -q "Dead"; then
    screen -wipe
fi
