#!/bin/sh
# select the baked config via $CONF (defaults to anonymous); copy into place and run mosquitto
CONF="${CONF:-mosquitto.conf}"
cp "/configs/${CONF}" /mosquitto/config/mosquitto.conf
cp /configs/mosquitto_pwd /mosquitto/config/mosquitto_pwd
exec /usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf
