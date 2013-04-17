#!/bin/bash

########################################################################
#
# Copyright (c) 2011 Yahoo! Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.
#
########################################################################


BUFFERSIZE=1000;
BATCHSIZE=0;

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;
CLASSPATH=../conf
for j in ../target/omid*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

tso() {
    exec java -Xmx1024m -cp $CLASSPATH -Domid.maxItems=1000000 -Domid.maxCommits=1000000 -Dlog4j.configuration=log4j.properties com.yahoo.omid.tso.TSOServer -port 1234 -batch $BATCHSIZE -zk localhost:2181 -ha -fsLog /tmp/omid
}

notifsrv() {
    exec java -Xmx1024m -cp $CLASSPATH -Dlog4j.configuration=log4j.properties com.yahoo.omid.notifications.DeltaOmidServer -zk localhost:2181 -scanIntervalMs 20000
}

tsobench() {
    exec java -Xmx1024m -cp $CLASSPATH -Dlog4j.configuration=log4j.properties com.yahoo.omid.tso.TransactionClient localhost 1234 100000 10 5
}

bktest() {
    exec java -cp $CLASSPATH -Dlog4j.configuration=log4j.properties org.apache.bookkeeper.util.LocalBookKeeper 5
}

tranhbase() {
    pwd
    echo $CLASSPATH
    exec java -cp $CLASSPATH org.apache.hadoop.hbase.LocalHBaseCluster 
}

testtable() {
    exec java -cp $CLASSPATH:../target/test-classes com.yahoo.omid.TestTable
}

usage() {
    echo "Usage: omid.sh <command>"
    echo "where <command> is one of:"
    echo "  tso           Starts the timestamp oracle server."
    echo "  notif-srv     Starts the notification framework."
    echo "  tsobench      Runs a simple benchmark of the TSO."
    echo "  bktest        Starts test bookkeeper ensemble. Starts zookeeper also."
    echo "  tran-hbase    Starts hbase with transaction support."
    echo "  test-table    Creates test table"
}

# if no args specified, show usage
if [ $# = 0 ]; then
    usage;
    exit 1
fi

COMMAND=$1

if [ "$COMMAND" = "tso" ]; then
    tso;
elif [ "$COMMAND" = "notif-srv" ]; then
    notifsrv;
elif [ "$COMMAND" = "tsobench" ]; then
    tsobench;
elif [ "$COMMAND" = "bktest" ]; then
    bktest;
elif [ "$COMMAND" = "tran-hbase" ]; then
    tranhbase;
elif [ "$COMMAND" = "test-table" ]; then
    testtable;
else
    usage;
fi


