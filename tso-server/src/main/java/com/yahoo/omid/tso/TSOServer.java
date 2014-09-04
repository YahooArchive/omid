package com.yahoo.omid.tso;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.yahoo.omid.committable.hbase.HBaseCommitTableStorageModule;
import com.yahoo.omid.committable.hbase.HBaseLogin;
import com.yahoo.omid.metrics.CodahaleMetricsConfig;
import com.yahoo.omid.metrics.MetricsProvider.Provider;
import com.yahoo.omid.metrics.YMonMetricsConfig;
import com.yahoo.omid.timestamp.storage.ZKTimestampStorageModule;
import com.yahoo.omid.tso.TSOServerCommandLineConfig.CommitTableStore;
import com.yahoo.omid.tso.TSOServerCommandLineConfig.TimestampStore;
import com.yahoo.omid.tso.hbase.HBaseTimestampStorageModule;

@Singleton
public class TSOServer extends AbstractIdleService {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServer.class);

    private final TSOServerCommandLineConfig config;

    private RequestProcessor requestProc;

    private ChannelFactory factory;
    private ChannelGroup channelGroup;

    @Inject
    public TSOServer(TSOServerCommandLineConfig config, RequestProcessor requestProc) {
        this.config = config;
        this.requestProc = requestProc;
    }

    static TSOServer getInitializedTsoServer(TSOServerCommandLineConfig config) throws IOException {
        LOG.info("Configuring TSO Server...");
        GuiceConfigBuilder guiceConfigBuilder = new GuiceConfigBuilder(config);
        Injector injector = Guice.createInjector(guiceConfigBuilder.buildModuleList());
        LOG.info("TSO Server configured. Creating instance...");
        return injector.getInstance(TSOServer.class);
    }

    // NOTE: The guice config is in here following the best practices in:
    // https://code.google.com/p/google-guice/wiki/AvoidConditionalLogicInModules
    // This is due to the fact that the target storage for timestamps or
    // commit table can be selected from the command line
    private static class GuiceConfigBuilder {

        private final TSOServerCommandLineConfig config;

        private final List<Module> guiceModules = new ArrayList<Module>();

        public GuiceConfigBuilder(TSOServerCommandLineConfig config) {
            this.config = config;
        }

        public List<Module> buildModuleList() throws IOException {
            addMetricsProviderModule();
            addTSOModule();
            addTimestampStorageModule();
            addCommitTableStorageModule();
            return guiceModules;
        }

        private void addMetricsProviderModule() {
            Provider metricsProvider = config.getMetricsProvider();
            switch (metricsProvider) {
            case CODAHALE:
                guiceModules.add(new CodahaleModule(config, new CodahaleMetricsConfig()));
                break;
            case YMON:
                guiceModules.add(new YMonModule(new YMonMetricsConfig()));
                break;
            default:
                throw new IllegalArgumentException("Unknown metrics provider" + metricsProvider);
            }
            LOG.info("\t* Metrics provider set to {}", metricsProvider);
        }

        private void addTSOModule() {
            guiceModules.add(new TSOModule(config));
        }

        private void addTimestampStorageModule() throws IOException {
            TimestampStore timestampStore = config.getTimestampStore();
            switch (timestampStore) {
            case HBASE:
                guiceModules.add(new HBaseTimestampStorageModule());
                HBaseLogin.loginIfNeeded(config.getLoginFlags());
                break;
            case MEMORY:
                guiceModules.add(new InMemoryTimestampStorageModule());
                break;
            case ZK:
                guiceModules.add(new ZKTimestampStorageModule(config));
                break;
            default:
                throw new IllegalArgumentException("Unknown timestamp store" + timestampStore);
            }
            LOG.info("\t* Timestamp store set to {}", timestampStore);
        }

        private void addCommitTableStorageModule() throws IOException {
            CommitTableStore commitTableStore = config.getCommitTableStore();
            switch (commitTableStore) {
            case HBASE:
                guiceModules.add(new HBaseCommitTableStorageModule());
                HBaseLogin.loginIfNeeded(config.getLoginFlags());
                break;
            case MEMORY:
                guiceModules.add(new InMemoryCommitTableStorageModule());
                break;
            default:
                throw new IllegalArgumentException("Unknown commit table store" + commitTableStore);
            }
            LOG.info("\t* Commit table store set to {}", commitTableStore);
        }

    }

    @Override
    protected void startUp() throws Exception {
        startIt();
    }

    @Override
    protected void shutDown() throws Exception {
        stopIt();
    }

    public void startIt() {
        // Setup netty listener
        factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("boss-%d").build()), Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("worker-%d").build()), (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);

        // Create the global ChannelGroup
        channelGroup = new DefaultChannelGroup(TSOServer.class.getName());

        final TSOHandler handler = new TSOHandler(channelGroup, requestProc);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new TSOPipelineFactory(handler));

        // Add the parent channel to the group
        Channel channel = bootstrap.bind(new InetSocketAddress(config.getPort()));
        channelGroup.add(channel);

        LOG.info("********** TSO Server initialized on port {} **********", config.getPort());
    }

    public void stopIt() {
        // Netty shutdown
        if(channelGroup != null) {
            channelGroup.close().awaitUninterruptibly();
        }
        if(factory != null) {
            factory.releaseExternalResources();
        }
        LOG.info("********** TSO Server stopped successfully **********");
    }

    public void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stopAndWait();
            }
        });
        LOG.info("Shutdown Hook Attached");
    }

    /**
     * This is where all starts on the server side
     */
    public static void main(String[] args) throws Exception {

        TSOServerCommandLineConfig config = TSOServerCommandLineConfig.parseConfig(args);

        if (config.hasHelpFlag()) {
            config.usage();
            System.exit(0);
        }

        TSOServer tsoServer = getInitializedTsoServer(config);
        tsoServer.attachShutDownHook();
        tsoServer.startAndWait();

    }

}