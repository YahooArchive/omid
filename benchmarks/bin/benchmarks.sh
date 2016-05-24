#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


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
    exec java $JVM_FLAGS -Dlog4j.configuration=file:../conf/log4j.xml -cp $CLASSPATH org.apache.omid.benchmarks.tso.TSOServerBenchmark $@
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


