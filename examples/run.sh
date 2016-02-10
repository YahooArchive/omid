#!/bin/bash

# Command line example for secure HBase
# -------------------------------------
# run.sh \
#   -hbaseClientPrincipal <pincipal>@<domain> \
#   -hbaseClientKeytab <full path to *.keytab> \
#   -tsoHost <omid-tso-host> \
#   -commitTableName [my_namespace:]<omid_commit_table>
#
# Example:
#
# run.sh -hbaseClientPrincipal foo0@bar.YAHOO.COM -hbaseClientKeytab /foo/bar.keytab -tsoHost localhost -commitTableName OMID_COMMIT_TABLE

# Command line example for not secure HBase
# -----------------------------------------
# run.sh -tsoHost <omid-tso-host> -commitTableName [my_namespace:]<omid_commit_table>
#
# Example:
#
# run.sh -tsoHost localhost -commitTableName OMID_COMMIT_TABLE

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;

fileNotFound(){
    echo "omid-examples.jar not found!";
    exit 1;
}

# Check if HADOOP_CONF_DIR and HBASE_CONF_DIR are set
if [ -z ${HADOOP_CONF_DIR+x} ]; then echo "WARNING: HADOOP_CONF_DIR is unset"; else echo "HADOOP_CONF_DIR is set to '$HADOOP_CONF_DIR'"; fi
if [ -z ${HBASE_CONF_DIR+x} ]; then echo "WARNING: HBASE_CONF_DIR is unset"; else echo "HBASE_CONF_DIR is set to '$HBASE_CONF_DIR'"; fi

#Check if jar exists
[ ! -f "omid-examples.jar" ] && fileNotFound

# Configure classpath
KLASSPATH=omid-examples.jar:${SCRIPTDIR}:${HBASE_CONF_DIR}:${HADOOP_CONF_DIR}

for jar in ./lib/*.jar; do
    if [ -f "$jar" ]; then
        KLASSPATH=$KLASSPATH:$jar
    fi
done

java -cp $KLASSPATH com.yahoo.omid.examples.BasicExample "$@"