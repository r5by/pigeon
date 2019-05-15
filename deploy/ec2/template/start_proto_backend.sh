#!/bin/bash
# Start backend

LOG=backend.log

APPCHK=$(ps aux | grep -v grep | grep -c ProtoBackend)
IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`

ip_there=`cat backend.conf |grep hostname`
if [ "X$ip_there" == "X" ]; then
  echo -e "# IP address;\nhostname = $IP" >> backend.conf
fi

master_socket_there=`cat backend.conf |grep master_socket`
if [ "X$master_socket_there" == "X" ]; then
  echo -e "# The master address this executor belongs to;\nmaster.socket = $1" >> backend.conf
fi

worker_type_there=`cat backend.conf |grep worker_type`
if [ "X$worker_type_there" == "X" ]; then
  echo -e "# Let the master node know if this worker is a HW (1) or LW (0);\nworker.type = $2" >> backend.conf
fi

if [ ! $APPCHK = '0' ]; then
  echo "Backend already running, cannot start it."
  exit 1;
fi

# Wait for daemon ready
sleep 5
nohup java -cp pigeon-1.0-SNAPSHOT.jar edu.utarlington.pigeon.examples.SimpleBackend -c backend.conf  > $LOG 2>&1 &

PID=$!
echo "Logging to $LOG"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Proto backend failed to start"
  exit 1;
else
  echo "Proto backend started with pid $PID"
  exit 0;
fi
