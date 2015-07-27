package com.yahoo.omid.tso;

import static com.yahoo.omid.ZKConstants.CURRENT_TSO_PATH;
import static com.yahoo.omid.ZKConstants.TSO_LEASE_PATH;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
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

    // Default network interface where this instance is running
    static final String DEFAULT_TSO_NET_IFACE = "eth0";

    // Default lease period
    static final long DEFAULT_LEASE_PERIOD_IN_MSECS = 10 * 1000; // 10 Secs

    public static final String TSO_HOST_AND_PORT_KEY = "tso.hostandport";

    public static final String TSO_EPOCH_KEY = "tso.epoch";

    private final TSOServerCommandLineConfig config;

    @Inject
    private CuratorFramework zkClient;

    @Inject
    @Named(TSO_HOST_AND_PORT_KEY)
    private String tsoHostAndPortAsString;

    private RequestProcessor requestProc;

    private ChannelFactory factory;
    private ChannelGroup channelGroup;

    // ------------------------------------------------------------------------
    // High availability related variables
    // ------------------------------------------------------------------------

    @Inject
    private LeaseManager leaseManager;

    // ------------------------------------------------------------------------

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

    private static class HBaseConfigModule extends AbstractModule {
        @Override
        protected void configure() {
        }

        @Provides
        public Configuration provideHBaseConfig() {
            return HBaseConfiguration.create();
        }
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
            addHBaseConfigModuleIfRequired();
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
                break;
            case MEMORY:
                guiceModules.add(new InMemoryCommitTableStorageModule());
                break;
            default:
                throw new IllegalArgumentException("Unknown commit table store" + commitTableStore);
            }
            LOG.info("\t* Commit table store set to {}", commitTableStore);
        }

        private void addHBaseConfigModuleIfRequired() throws IOException {
            if (config.getCommitTableStore() == CommitTableStore.HBASE
                    || config.getTimestampStore() == TimestampStore.HBASE) {
                guiceModules.add(new HBaseConfigModule());
                HBaseLogin.loginIfNeeded(config.getLoginFlags());
            }
        }

    }

    /**
     * Encompasses all the required elements to control the leases required for
     * identifying the master instance when running multiple TSO instances for HA
     * This includes publishing the instance information when getting the lease.
     */
    @VisibleForTesting
    public static class LeaseManager extends AbstractScheduledService {

        private final CuratorFramework zkClient;

        private final String tsoHostAndPort;

        private final RequestProcessor requestProcessor;

        private final long leasePeriodInMs;
        private int leaseNodeVersion;
        private final AtomicLong endLeaseInMs = new AtomicLong(0L);
        private final AtomicLong baseTimeInMs = new AtomicLong(0L);

        private final String leasePath;
        private final String currentTSOPath;

        LeaseManager(String tsoHostAndPort, RequestProcessor requestProcessor, long leasePeriodInMs,
                CuratorFramework zkClient) {
            this(tsoHostAndPort, requestProcessor, leasePeriodInMs, TSO_LEASE_PATH, CURRENT_TSO_PATH, zkClient);
        }

        @VisibleForTesting
        LeaseManager(String tsoHostAndPort,
                     RequestProcessor requestProcessor,
                     long leasePeriodInMs,
                     String leasePath,
                     String currentTSOPath,
                     CuratorFramework zkClient) {
            this.tsoHostAndPort = tsoHostAndPort;
            this.requestProcessor = requestProcessor;
            this.leasePeriodInMs = leasePeriodInMs;
            this.leasePath = leasePath;
            this.currentTSOPath = currentTSOPath;
            this.zkClient = zkClient;
            LOG.info("LeaseManager {} initialized. Lease period {}ms", toString(), leasePeriodInMs);
        }

        void tryToGetInitialLeasePeriod() throws Exception {
            baseTimeInMs.set(System.currentTimeMillis());
            if (canAcquireLease()) {
                endLeaseInMs.set(baseTimeInMs.get() + leasePeriodInMs);
                LOG.info("{} got the lease (Master) Ver. {}/End of lease: {}ms", tsoHostAndPort,
                        leaseNodeVersion, endLeaseInMs);
                advertiseTSOServerInfoThroughZK(requestProcessor.epoch());
            }
        }

        void tryToRenewLeasePeriod() throws Exception {
            baseTimeInMs.set(System.currentTimeMillis());
            if (canAcquireLease()) {
                if (System.currentTimeMillis() > getEndLeaseInMs()) {
                    LOG.warn("{} expired lease! Releasing lease to start Master re-election", tsoHostAndPort);
                    endLeaseInMs.set(0L);
                } else {
                    endLeaseInMs.set(baseTimeInMs.get() + leasePeriodInMs);
                    LOG.trace("{} renewed lease: Version {}/End of lease at {}ms",
                            tsoHostAndPort, leaseNodeVersion, endLeaseInMs);
                }
            } else {
                endLeaseInMs.set(0L);
                LOG.warn("{} lost the lease (Ver. {})! Other instance is now Master",
                        tsoHostAndPort, leaseNodeVersion);
            }
        }

        boolean haveLease() {
            return stillInLeasePeriod();
        }

        /**
         * Check if this instance still is under the lease period
         */
        public boolean stillInLeasePeriod() {
            return System.currentTimeMillis() <= getEndLeaseInMs();
        }

        public long getEndLeaseInMs() {
            return endLeaseInMs.get();
        }

        private boolean canAcquireLease() throws Exception {
            try {
                int previousLeaseNodeVersion = leaseNodeVersion;
                final byte[] instanceInfo = tsoHostAndPort.getBytes(Charsets.UTF_8);
                // Try to acquire the lease
                Stat stat = zkClient.setData().withVersion(previousLeaseNodeVersion)
                        .forPath(leasePath, instanceInfo);
                leaseNodeVersion = stat.getVersion();
                LOG.trace("{} got new lease version {}", tsoHostAndPort, leaseNodeVersion);
            } catch (KeeperException.BadVersionException e) {
                return false;
            }
            return true;
        }

        // ------------------------------------------------------------------------
        // --------------- AbstractScheduledService implementation ----------------
        // ------------------------------------------------------------------------

        @Override
        protected void startUp() throws Exception {
            createLeaseManagementZNode();
            createCurrentTSOZNode();
        }

        @Override
        protected void shutDown() throws Exception {
        }

        @Override
        protected void runOneIteration() throws Exception {

            if (!haveLease()) {
                tryToGetInitialLeasePeriod();
            } else {
                tryToRenewLeasePeriod();
            }

        }

        @Override
        protected Scheduler scheduler() {

            final long guardLeasePeriodInMs = leasePeriodInMs / 4;

            return new AbstractScheduledService.CustomScheduler() {

                @Override
                protected Schedule getNextSchedule() throws Exception {
                    if (!haveLease()) {
                        // Get the current node version...
                        Stat stat = zkClient.checkExists().forPath(leasePath);
                        leaseNodeVersion = stat.getVersion();
                        LOG.trace("{} will try to get lease (with Ver. {}) in {}ms", tsoHostAndPort, leaseNodeVersion,
                                leasePeriodInMs);
                        // ...and wait the lease period
                        return new Schedule(leasePeriodInMs, TimeUnit.MILLISECONDS);
                    } else {
                        long waitTimeInMs = getEndLeaseInMs() - System.currentTimeMillis() - guardLeasePeriodInMs;
                        LOG.trace("{} will try to renew lease (with Ver. {}) in {}ms", tsoHostAndPort,
                                leaseNodeVersion, waitTimeInMs);
                        return new Schedule(waitTimeInMs, TimeUnit.MILLISECONDS);
                    }
                }
            };

        }

        // ************************* Helper methods *******************************

        @Override
        public String toString() {
            return tsoHostAndPort;
        }

        void createLeaseManagementZNode() throws Exception {

            EnsurePath path = zkClient.newNamespaceAwareEnsurePath(leasePath);
            path.ensure(zkClient.getZookeeperClient());
            Stat stat = zkClient.checkExists().forPath(leasePath);
            Preconditions.checkNotNull(stat);
            LOG.info("Path {} ensured", path.getPath());

        }

        void createCurrentTSOZNode() throws Exception {

            EnsurePath path = zkClient.newNamespaceAwareEnsurePath(currentTSOPath);
            path.ensure(zkClient.getZookeeperClient());
            Stat stat = zkClient.checkExists().forPath(currentTSOPath);
            Preconditions.checkNotNull(stat);
            LOG.info("Path {} ensured", path.getPath());

        }

        int advertiseTSOServerInfoThroughZK(long epoch) throws Exception {

            LOG.info("Advertising TSO host:port {} (Epoch {}) through ZK", tsoHostAndPort, epoch);
            String tsoInfoAsString = tsoHostAndPort + "#" + Long.toString(epoch);
            byte[] tsoInfoAsBytes = tsoInfoAsString.getBytes(Charsets.UTF_8);
            Stat currentTSOZNodeStat = zkClient.setData().forPath(currentTSOPath, tsoInfoAsBytes);
            return currentTSOZNodeStat.getVersion();

        }

    }

    // ------------------------------------------------------------------------
    // ------------------- AbstractIdleService implementation -----------------
    // ------------------------------------------------------------------------

    @Override
    protected void startUp() throws Exception {
        startIt();
    }

    @Override
    protected void shutDown() throws Exception {
        stopIt();
    }


    // ------------------------------------------------------------------------

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

        // TODO Remove the variable of the condition in the future if ZK
        // becomes the only possible way of configuring the TSOClient
        if (config.shouldHostAndPortBePublishedInZK) {

            LOG.info("Connecting to ZK cluster [{}]", zkClient.getState());
            zkClient.start();
            try {
                if (zkClient.blockUntilConnected(10, TimeUnit.SECONDS)) {
                    LOG.info("Connection to ZK cluster [{}]", zkClient.getState());
                    leaseManager.startAndWait();
                } else {
                    LOG.warn("Can't contact ZK after 3 seconds. TSO host:port won't be published");
                }
            } catch (InterruptedException e) {
                LOG.error("LeaseManager interrupted!", e);
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("********** TSO Server running on port {} **********", config.getPort());
    }

    public void stopIt() {
        if (config.shouldHostAndPortBePublishedInZK && leaseManager != null) {
            leaseManager.stop();
        }
        // Netty shutdown
        if (channelGroup != null) {
            channelGroup.close().awaitUninterruptibly();
        }
        if (factory != null) {
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
