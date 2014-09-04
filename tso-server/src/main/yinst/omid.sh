#!/bin/bash

# Setup JAVA_HOME, and java.library.path

if [ "x${yjava_jdk__JAVA_HOME}" != "x" ]; then
    JAVA_HOME="${yjava_jdk__JAVA_HOME}"
else
    JAVA_HOME=/home/y/share/yjava_jdk/java/
fi

# Determine whether we're using the 32 or 64bit JVM (you can use 32 bit JVM on 64bit OS)

MULTIMODELIBEXEC=`/home/y/bin/yahoo-mode-choice -e yjava_jdk`

if [ "${MULTIMODELIBEXEC}" == "/home/y/libexec64" ]; then
    USE_64BIT_JVM=true
    export USE_64BIT_JVM
    ARCH_LIBEXEC_DIR=/home/y/libexec64
    ARCH_LIB_DIR=/home/y/lib64
    ARCH_BIN_DIR=/home/y/bin64
    YOURKIT_AGENT=${ARCH_LIBEXEC_DIR}/${YOURKIT_PKG_DIR}/bin/linux-x86-64/libyjpagent.so
    echo "$0 ARCH = 64bit"
else
    ARCH_LIBEXEC_DIR=/home/y/libexec
    ARCH_LIB_DIR=/home/y/lib
    ARCH_BIN_DIR=/home/y/bin
    YOURKIT_AGENT=${ARCH_LIBEXEC_DIR}/${YOURKIT_PKG_DIR}/bin/linux-x86-32/libyjpagent.so
    echo "$0 ARCH = 32bit"
fi

echo "$0 JAVA_HOME = ${JAVA_HOME}"

# Resolve KLASSPATH

KLASSPATH=$(CONFIG_FOLDER):$(HBASE_CONF_DIR):$(HADOOP_CONF_DIR)

for jar in /home/y/lib/jars/$(PRODUCT_NAME)/*.jar; do
	if [ -f "$jar" ]; then
		KLASSPATH=$KLASSPATH:$jar
	fi
done
JVM_ARGS="-Xmx$(HEAP_SIZE_IN_MIB)m $(JVM_ARGS) ${YOURKIT_OPTS}"

tso() {
    exec java -server $JVM_ARGS -cp $KLASSPATH com.yahoo.omid.tso.TSOServer \
              -timestampStore $(TIMESTAMP_STORE) -hbaseTimestampTable $(HBASE_TIMESTAMP_TABLE) \
              -commitTableStore $(COMMIT_TABLE_STORE) -hbaseCommitTable $(HBASE_COMMIT_TABLE) \
              -port $(PORT) -maxItems $(MAX_ITEMS) -metricsProvider $(METRICS_PROVIDER) -metricsConfigs $(METRICS_CONFIGS) \
              -hbaseClientPrincipal $(HBASE_CLIENT_PRINCIPAL) -hbaseClientKeytab $(HBASE_CLIENT_KEYTAB)
}

tsobench() {
    exec java -server -cp $KLASSPATH com.yahoo.omid.tso.util.TransactionClient -hbaseCommitTable $(HBASE_COMMIT_TABLE) \
              -tsoPort $(PORT) -hbaseClientPrincipal $(HBASE_CLIENT_PRINCIPAL) \
              -hbaseClientKeytab $(HBASE_CLIENT_KEYTAB) $@
}

createHBaseCommitTable() {
    exec java -server -cp $KLASSPATH com.yahoo.omid.committable.hbase.CreateTable -tableName $(HBASE_COMMIT_TABLE) \
       -hbaseClientPrincipal $(HBASE_CLIENT_PRINCIPAL) -hbaseClientKeytab $(HBASE_CLIENT_KEYTAB) $@
}

createHBaseTimestampTable() {
    exec java -server -cp $KLASSPATH com.yahoo.omid.tso.hbase.CreateTable -tableName $(HBASE_TIMESTAMP_TABLE) \
      -hbaseClientPrincipal $(HBASE_CLIENT_PRINCIPAL) -hbaseClientKeytab $(HBASE_CLIENT_KEYTAB)
}

usage() {
    echo "Usage: sudo -u yahoo omid.sh <command>"
    echo "where <command> is one of:"
    echo "  tso           Starts the timestamp oracle server."
    echo "  tsobench      Runs a simple benchmark of the TSO."
    echo "  create-hbase-commit-table -numSplits <num>   Creates the hbase commit table."
    echo "  create-hbase-timestamp-table Creates the hbase timestamp table."
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

