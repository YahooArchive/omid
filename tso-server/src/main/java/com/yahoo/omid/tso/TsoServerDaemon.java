package com.yahoo.omid.tso;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * For yjava_daemon
 */
public class TsoServerDaemon implements Daemon {
    private static final Logger LOG = LoggerFactory.getLogger(TsoServerDaemon.class);
    private TSOServer tsoServer;

    @Override
    public void init(DaemonContext daemonContext) throws Exception {
        final String[] arguments = daemonContext.getArguments();
        LOG.info("Starting TSOServer, args: {}", StringUtils.join(" ", Arrays.asList(arguments)));
        TSOServerCommandLineConfig config = TSOServerCommandLineConfig.parseConfig(arguments);
        tsoServer = TSOServer.getInitializedTsoServer(config);
    }

    @Override
    public void start() throws Exception {
        tsoServer.startAndWait();
    }

    @Override
    public void stop() throws Exception {
        tsoServer.stopAndWait();
    }

    @Override
    public void destroy() {

    }
}
