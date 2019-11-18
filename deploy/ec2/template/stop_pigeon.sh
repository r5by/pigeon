#!/bin/bash
# Stop pigeon locally

APPCHK=$(ps aux | grep -v grep | grep -c PigeonDaemon)

if [ $APPCHK = '0' ]; then
  echo "Pigeon is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep PigeonDaemon |grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped Pigeon process"
exit 0;
