#!/bin/sh

exit $(( (`date +%s` - `stat -L -c %Y /log/powerstation-mqtt-status${powerstation_instance}.log` ) > 60 ))
