#!/bin/bash

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;
CLASSPATH=../conf:$(HBASE_CONF_DIR):$(HADOOP_CONF_DIR)

. ../conf/omid-env.sh

# for source release
for j in ../target/omid-tso*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

# for binary release
for j in ../omid-tso*.jar; do
    CLASSPATH=$CLASSPATH:$j
done
for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

tso() {
    exec java $JVM_FLAGS -cp $CLASSPATH org.apache.omid.tso.TSOServer @../conf/omid.conf $@
}

tsoRelauncher() {
    until ./omid.sh tso $@; do
        echo "TSO Server crashed with exit code $?.  Re-launching..." >&2
        sleep 1
    done
}

createHBaseCommitTable() {
    exec java -cp $CLASSPATH org.apache.omid.tools.hbase.OmidTableManager commit-table $@
}

createHBaseTimestampTable() {
    exec java -cp $CLASSPATH org.apache.omid.tools.hbase.OmidTableManager timestamp-table $@
}

usage() {
    echo "Usage: omid.sh <command> <options>"
    echo "where <command> is one of:"
    echo "  tso                           Starts The Status Oracle server (TSO)"
    echo "  tso-relauncher                Starts The Status Oracle server (TSO) re-launching it if the process exits"
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
elif [ "$COMMAND" = "tso-relauncher" ]; then
    tsoRelauncher $@;
elif [ "$COMMAND" = "create-hbase-commit-table" ]; then
    createHBaseCommitTable $@;
elif [ "$COMMAND" = "create-hbase-timestamp-table" ]; then
    createHBaseTimestampTable $@;
else
    exec java -cp $CLASSPATH $COMMAND $@
fi


