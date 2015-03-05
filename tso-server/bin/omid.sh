#!/bin/bash

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

createHBaseCommitTable() {
    exec java -cp $CLASSPATH com.yahoo.omid.committable.hbase.CreateTable $@
}

createHBaseTimestampTable() {
    exec java -cp $CLASSPATH com.yahoo.omid.tso.hbase.CreateTable $@
}

usage() {
    echo "Usage: omid.sh <command> <options>"
    echo "where <command> is one of:"
    echo "  tso                           Starts The Status Oracle server (TSO)"
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
elif [ "$COMMAND" = "create-hbase-commit-table" ]; then
    createHBaseCommitTable $@;
elif [ "$COMMAND" = "create-hbase-timestamp-table" ]; then
    createHBaseTimestampTable $@;
else
    exec java -cp $CLASSPATH $COMMAND $@
fi


