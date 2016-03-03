#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------------------------------------------------

function show_help() {

    echo "
    ###################################################################################################################
    #
    # This script allows to run small examples showing Omid functionality and APIs. These are the examples provided:
    #
    # 1) basic -> Executes a transaction encompassing a multi-row modification in HBase
    # 2) si -> Shows how Omid preserves Snapshot Isolation guarantees when concurrent transactions access shared data
    # 3) in -> basic + Instrumentation, see examples-hbase-omid-client-config-with-metrics.yml
    #
    # See the source-code to get more details about each example.
    #
    # The Omid examples can be executed with the following command pattern:
    #
    # $ $0 <example> [ tablename ] [column family name]
    #
    # where:
    # - <example> can be [ basic | si | in ]
    #
    # Example execution:
    # ------------------
    #
    # $0 basic
    #
    # will run the basic example with the default options
    #
    # $ $0 basic myHBaseTable myCf
    #
    # will run the basic example using 'myHBaseTable' as table name and 'myCf' as column family.
    # All Omid related configuration setting assume their default values, see
    # omid-client-config.yml and hbase-omid-client-config-default.yml in source code
    #
    ###################################################################################################################
    "

}

fileNotFound(){
    echo "omid-examples.jar not found!";
    exit 1;
}

# ---------------------------------------------------------------------------------------------------------------------
# --------------------------------------------- SCRIPT STARTS HERE ----------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;
SCRIPTDIR=`pwd`

# ---------------------------------------------------------------------------------------------------------------------
# Check if HADOOP_CONF_DIR and HBASE_CONF_DIR are set
# ---------------------------------------------------------------------------------------------------------------------

if [ -z ${HADOOP_CONF_DIR+x} ]; then echo "WARNING: HADOOP_CONF_DIR is unset"; else echo "HADOOP_CONF_DIR is set to '$HADOOP_CONF_DIR'"; fi
if [ -z ${HBASE_CONF_DIR+x} ]; then echo "WARNING: HBASE_CONF_DIR is unset"; else echo "HBASE_CONF_DIR is set to '$HBASE_CONF_DIR'"; fi

# ---------------------------------------------------------------------------------------------------------------------
# Check if jar exists and configure classpath
# ---------------------------------------------------------------------------------------------------------------------

[ ! -f "omid-examples.jar" ] && fileNotFound

KLASSPATH=omid-examples.jar:${SCRIPTDIR}:${HBASE_CONF_DIR}:${HADOOP_CONF_DIR}

for jar in ./lib/*.jar; do
    if [ -f "$jar" ]; then
        KLASSPATH=$KLASSPATH:$jar
    fi
done

# ---------------------------------------------------------------------------------------------------------------------
# Parse CLI user args
# ---------------------------------------------------------------------------------------------------------------------

USER_OPTION=$1
shift
case ${USER_OPTION} in
    basic)
        java -cp $KLASSPATH com.yahoo.omid.examples.BasicExample "$@"
        ;;
    si)
        java -cp $KLASSPATH com.yahoo.omid.examples.SnapshotIsolationExample "$@"
        ;;
    in)
        java -cp $KLASSPATH com.yahoo.omid.examples.InstrumentationExample "$@"
        ;;
    *)
        show_help
        ;;
esac
