#!/bin/bash

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;
CLASSPATH=../conf

. ./omid-env.sh

# for source release
for j in ../target/omid-benchmarks*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

# for binary release
for j in ../omid-benchmarks*.jar; do
    CLASSPATH=$CLASSPATH:$j
done
for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

tso() {
    exec java $JVM_FLAGS -Dlog4j.configuration=log4j.xml -cp $CLASSPATH com.yahoo.omid.benchmarks.tso.TransactionClient $@
}

usage() {
    echo "Usage: benchmarks.sh <benchmark> <options>"
    echo "where <benchmark> is one of:"
    echo "  tso           Starts the tso benchmark."
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
else
    exec java -cp $CLASSPATH $COMMAND $@
fi


