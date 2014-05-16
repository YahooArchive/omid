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

. ../conf/omid-env.sh

# for source release
for j in ../target/tso*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

# for binary release
for j in ../tso*.jar; do
    CLASSPATH=$CLASSPATH:$j
done
for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

tso() {
    exec java $JVM_FLAGS -cp $CLASSPATH com.yahoo.omid.tso.TSOServer @../conf/omid.conf $@
}

tsobench() {
    exec java $JVM_FLAGS -cp $CLASSPATH com.yahoo.omid.tso.util.TransactionClient $@
}

createHBaseCommitTable() {
    exec java -cp $CLASSPATH com.yahoo.omid.committable.hbase.CreateTable $@
}

createHBaseTimestampTable() {
    exec java -cp $CLASSPATH com.yahoo.omid.tso.hbase.CreateTable $@
}

usage() {
    echo "Usage: omid.sh <command>"
    echo "where <command> is one of:"
    echo "  tso           Starts the timestamp oracle server."
    echo "  tsobench      Runs a simple benchmark of the TSO."
    echo "  create-hbase-commit-table     Creates the hbase commit table."
    echo "  create-hbase-timestamp-table  Creates the hbase timestamp table."
}

# if no args specified, show usage
if [ $# = 0 ]; then
    usage;
    exit 1
fi

COMMAND=$1
shift

if [ "$COMMAND" = "tso" ]; then
    tso $@;
elif [ "$COMMAND" = "tsobench" ]; then
    tsobench $@;
elif [ "$COMMAND" = "create-hbase-commit-table" ]; then
    createHBaseCommitTable $@;
elif [ "$COMMAND" = "create-hbase-timestamp-table" ]; then
    createHBaseTimestampTable $@;
else
    exec java -cp $CLASSPATH $COMMAND $@
fi


