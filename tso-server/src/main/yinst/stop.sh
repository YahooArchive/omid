#!/bin/bash
#

PID_FILE=/home/y/var/$(PRODUCT_NAME)/run/$(PRODUCT_NAME).pid
HARD_STOP=60     # seconds to wait for the process to exit

# kills pid and all it's children with signal sig
killtree() {
    sig=$1
    pid=$2
    pkill $sig -P $pid .   # all children
    kill $sig $pid         # must kill parent last
}

# returns 0 if running pid is running
checkpid() {
    pid=$1
    if [ `ps -p $pid -o pid=` ]; then
	return 0
    fi
    return 1
}

# waits for pid to exit for up to tmout secs
# returns 0 on success
waitfor() {
    pid=$1
    tmout=$2
    for ((i=0; i < $tmout; i++)); do
	checkpid $pid
	if [ "$?" -ne "0" ]; then return 0; fi
	sleep 1
    done
    return 1
}

# MAIN

# set -x
prog=${0##*/}

# Stop app only if its running
if [ ! -e $PID_FILE ]; then
    echo "$prog: not running"
    exit 0
fi

FILE_PID=`cat $PID_FILE`
OS_PID=`ps -p $FILE_PID -o pid=`

if [ ! "$OS_PID" ]; then
    OS_PID=-1
fi

if [ $OS_PID != $FILE_PID ]; then
    echo "$prog: FAILED: not running, removing $PID_FILE" >&2
    rm $PID_FILE
    exit 0
fi

killtree -15 $FILE_PID

# Wait for the app process to exit cleanly for
# up to $HARD_STOP seconds, before trying kill -9

waitfor $FILE_PID $HARD_STOP

if [ "$?" -ne "0" ]; then
    echo "$prog: WARNING: didn't stop within $HARD_STOP sec after kill, using kill -9" >&2
    killtree -9 $FILE_PID
    waitfor $FILE_PID $HARD_STOP
    if [ "$?" -ne "0" ]; then
	echo "$prog: FAILED: kill -9 $FILE_PID didn't stop the process" >&2
	exit 1
    fi
fi

rm $PID_FILE

exit 0
