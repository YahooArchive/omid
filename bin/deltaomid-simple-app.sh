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


SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;
CLASSPATH=../conf
for j in ../target/omid*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

scaffolding() {
    exec java -Xmx1024m -cp $CLASSPATH -Dlog4j.configuration=log4j.properties com.yahoo.omid.examples.notifications.OmidInfrastructure
}

createdb() {
    exec java -Xmx1024m -cp $CLASSPATH -Dlog4j.configuration=log4j.properties com.yahoo.omid.examples.notifications.SimpleAppInjector -hbase localhost:2181 -createDB
}

app() {
    exec java -Xmx1024m -cp $CLASSPATH -Dlog4j.configuration=log4j.properties com.yahoo.omid.examples.notifications.SimpleApp -zk localhost:2181 -omid localhost:1234 -port 16666
}

inject() {
    exec java -Xmx1024m -cp $CLASSPATH -Dlog4j.configuration=log4j.properties com.yahoo.omid.examples.notifications.SimpleAppInjector -hbase localhost:2181 -omid localhost:1234 -injectors 1 -txRate 1 -metricsOutputDir ~/metrics
}

usage() {
    echo "Usage: deltaomid-simple-app.sh <command>"
    echo "where <command> is one of:"
    echo "  scaffolding   Starts the infrastructure required by the Simple App in a local environment, including DB"
    echo "  createdb      Creates the Simple App DB."    
    echo "  app           Starts the Simple App."
    echo "  inject        Starts the Simple App Injectors."
}

# if no args specified, show usage
if [ $# = 0 ]; then
    usage;
    exit 1
fi

COMMAND=$1

if [ "$COMMAND" = "scaffolding" ]; then
    scaffolding;
elif [ "$COMMAND" = "createdb" ]; then
    createdb;
elif [ "$COMMAND" = "app" ]; then
    app;
elif [ "$COMMAND" = "inject" ]; then
    inject;
else
    usage;
fi
