#!/bin/sh

$(PRODUCT_NAME)-fix-ownership.sh
PIDFILE=/home/y/var/$(PRODUCT_NAME)/run/$(PRODUCT_NAME).pid

if [ -e $PIDFILE ]; then
    FILE_PID=`cat $PIDFILE`
    OS_PID=`ps -p $FILE_PID -o pid=`

    if [ "x$OS_PID" = "x" ]; then
        OS_PID=-1
    fi

    if [ $OS_PID = $FILE_PID ]; then
        echo "$(PRODUCT_NAME) is already running"
        exit 0
    else
        echo "removing orphaned $(PRODUCT_NAME).pid file"
        rm $PIDFILE
    fi
fi


# Setup JAVA_HOME, and java.library.path

if [ "x${yjava_jdk__JAVA_HOME}" != "x" ]; then
    JAVA_HOME="${yjava_jdk__JAVA_HOME}"
else
    JAVA_HOME=${ROOT}/share/yjava_jdk/java/
fi

export JAVA_HOME
echo "$0 JAVA_HOME = ${JAVA_HOME}"

# Determine whether we're using the 32 or 64bit JVM (you can use 32 bit JVM on 64bit OS)

MULTIMODELIBEXEC=`${ROOT}/bin/yahoo-mode-choice -e yjava_jdk`
YOURKIT_PKG_DIR=yjava_yourkit

if [ "${MULTIMODELIBEXEC}" == "${ROOT}/libexec64" ]; then
    USE_64BIT_JVM=true
    export USE_64BIT_JVM
    ARCH_LIBEXEC_DIR=${ROOT}/libexec64
    ARCH_LIB_DIR=${ROOT}/lib64
    ARCH_BIN_DIR=${ROOT}/bin64
    YOURKIT_AGENT=${ARCH_LIBEXEC_DIR}/${YOURKIT_PKG_DIR}/bin/linux-x86-64/libyjpagent.so
    echo "$0 ARCH = 64bit"
else
    ARCH_LIBEXEC_DIR=${ROOT}/libexec
    ARCH_LIB_DIR=${ROOT}/lib
    ARCH_BIN_DIR=${ROOT}/bin
    YOURKIT_AGENT=${ARCH_LIBEXEC_DIR}/${YOURKIT_PKG_DIR}/bin/linux-x86-32/libyjpagent.so
    echo "$0 ARCH = 32bit"
fi

if [ "x${PRELOAD}" != "x" ]; then
  PRELOAD="${ARCH_LIBEXEC_DIR}/yjava_daemon_preload.so:${PRELOAD}"
else
  PRELOAD="${ARCH_LIBEXEC_DIR}/yjava_daemon_preload.so"
fi



#YourKit options.
#Enforced options :
#1. listen on port 9091
#2. Only all local connections for security reasons
#3. Add a startup delay of 30 seconds. This delays telemetry collection
#   so that all (J2EE server's) MBeans are initialized before they are accessed

if [ "x${$(PRODUCT_NAME)__YOURKIT_ENABLED}" != "x1" ]; then
    YOURKIT_OPTS=""
else
    if [ "x${$(PRODUCT_NAME)__YOURKIT_OPTIONS}" != "x" ]; then
        ADDL_YOURKIT_OPTS=",${$(PRODUCT_NAME)__yourkitOptions}"
    else
        ADDL_YOURKIT_OPTS=""
    fi

    YOURKIT_OPTS="-agentpath:${YOURKIT_AGENT}=port=9091,onlylocal,delay=30000${ADDL_YOURKIT_OPTIONS}"
fi

echo "$0 YOURKIT_OPTS = ${YOURKIT_OPTS}"

# Resolve KLASSPATH

KLASSPATH=$(CONFIG_FOLDER):$(HBASE_CONF_DIR):$(HADOOP_CONF_DIR)

for jar in lib/jars/$(PRODUCT_NAME)/*.jar; do
	if [ -f "$jar" ]; then
		KLASSPATH=$KLASSPATH:/home/y/$jar
	fi
done

if [ "x${ynet_filter}" != "x" ]; then
	YNET_FILTER=${ynet_filter}
else
 	YNET_FILTER="FILTER_YAHOO"
fi

echo "$0 YNET-FILTER: $YNET_FILTER"

if [ "x$(PUBLISH_HOSTPORT_IN_ZK)" == "xtrue" ]; then
    PUBLISH_HOST_AND_PORT_IN_ZK="-publishHostAndPortInZK"
else
    PUBLISH_HOST_AND_PORT_IN_ZK=""
fi

JVM_ARGS="-Xmx$(HEAP_SIZE_IN_MIB)m $(JVM_ARGS) ${YOURKIT_OPTS}"
( cd /home/y/var/$(PRODUCT_NAME)/run ; env LD_LIBRARY_PATH=${ARCH_LIB_DIR} LD_PRELOAD=${PRELOAD} ${ARCH_BIN_DIR}/yjava_daemon \
 -jvm server -pidfile /home/y/var/$(PRODUCT_NAME)/run/$(PRODUCT_NAME).pid -ynet ${YNET_FILTER}  \
 -procs 1 -user $(RUN_AS_USER) -outfile /home/y/var/$(PRODUCT_NAME)/run/$(PRODUCT_NAME).out \
 -errfile /home/y/var/$(PRODUCT_NAME)/run/$(PRODUCT_NAME).err -cp ${KLASSPATH} \
 -home /home/y/share/yjava_jdk/java -Djava.library.path=${ARCH_LIB_DIR} ${JVM_ARGS} \
com.yahoo.omid.tso.TsoServerDaemon \
 -timestampStore $(TIMESTAMP_STORE) -hbaseTimestampTable $(HBASE_TIMESTAMP_TABLE) \
 -commitTableStore $(COMMIT_TABLE_STORE) -hbaseCommitTable $(HBASE_COMMIT_TABLE) \
 -port $(PORT) -maxItems $(MAX_ITEMS) \
 -metricsProviderModule $(METRICS_PROVIDER_MODULE) -hbaseClientPrincipal $(HBASE_CLIENT_PRINCIPAL) \
 -hbaseClientKeytab $(HBASE_CLIENT_KEYTAB) \
 -maxBatchSize $(MAX_BATCH_SIZE) \
 -networkIface $(NETWORK_INTERFACE) \
 ${PUBLISH_HOST_AND_PORT_IN_ZK} \
 -zkCluster $(ZK_CLUSTER)
 )
