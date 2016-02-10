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
    #
    # See the source-code to get more details about each example.
    #
    # The Omid examples can be executed with the following command pattern:
    #
    # $ $0 <example> [ options ]
    #
    # where:
    # - <example> can be [ basic | si ]
    # - [ options ] is a list of configuration options for the example that can be shown with the -help parameter
    #
    # Example execution:
    # ------------------
    #
    # $0 basic -help
    #
    # will show all the options for running the basic example
    #
    # $0 basic
    #
    # will run the basic example with the default options
    #
    # $ $0 basic -hbaseClientPrincipal foo0@bar.YAHOO.COM -hbaseClientKeytab /foo/bar.keytab -tsoPort 1234
    #
    # will run the basic example contacting a TSO in localhost:1234 for secure HBase specifying the required args:
    #   -hbaseClientPrincipal <pincipal>@<domain>
    #   -hbaseClientKeytab <full_path_to_*.keytab>
    #   -tsoPort <omid_tso_port>
    #
    # $ $0 si -tsoHost a.remote.host -commitTableName MY_COMMIT_TABLE
    #
    # will run the snapshot isolation example for not secure HBase contacting a remote host and a specific commit table:
    #   -tsoHost <omid_tso_host>
    #   -commitTableName [<my_namespace>:]<my_commit_table_name>
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
    *)
        show_help
        ;;
esac
