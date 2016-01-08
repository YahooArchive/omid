package com.yahoo.omid.tso;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.yahoo.omid.committable.hbase.HBaseLogin;
import com.yahoo.omid.tso.TSOServerCommandLineConfig.CommitTableStore;
import com.yahoo.omid.tso.TSOServerCommandLineConfig.TimestampStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class TSOServer extends AbstractIdleService {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServer.class);

    // Default network interfaces where this instance is running
    static final String MAC_TSO_NET_IFACE = "en0";
    static final String LINUX_TSO_NET_IFACE = "eth0";

    // Default lease period
    static final long DEFAULT_LEASE_PERIOD_IN_MSECS = 10 * 1000; // 10 Secs

    public static final String TSO_HOST_AND_PORT_KEY = "tso.hostandport";

    @Inject
    private TSOServerCommandLineConfig config;

    @Inject
    private TSOStateManager tsoStateManager;
    @Inject
    private RequestProcessor requestProcessor;

    // ------------------------------------------------------------------------
    // High availability related variables
    // ------------------------------------------------------------------------

    @Inject
    private LeaseManagement leaseManagement;

    // ------------------------------------------------------------------------

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

        GuiceConfigBuilder(TSOServerCommandLineConfig config) {
            this.config = config;
        }

        List<Module> buildModuleList() throws IOException {
            addMetricsProviderModule();
            addTSOModule();
            addTimestampStorageModule();
            addCommitTableStorageModule();
            addHBaseConfigModuleIfRequired();
            return guiceModules;

        }

        private void addMetricsProviderModule() {
            // Dynamically instantiate metrics module from class name
            String metricsProviderModule = config.getMetricsProviderModule();
            Module metricsModule = instantiateGuiceMetricsModule(metricsProviderModule, config.getMetricsConfigs());
            guiceModules.add(metricsModule);
            LOG.info("\t* Metrics provider module set to {}", metricsProviderModule);
        }

        private void addTSOModule() {
            guiceModules.add(new TSOModule(config));
        }

        private void addTimestampStorageModule() throws IOException {
            String className = null;
            try {
                TimestampStore timestampStore = config.getTimestampStore();
                className = timestampStore.toString();
                Constructor<?> constructor = Class.forName(className).getConstructor();
                Module module = (Module) constructor.newInstance();
                guiceModules.add(module);
                LOG.info("\t* Timestamp store set to {}", module);
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new IllegalStateException(
                    String.format("Class named '%s' was not found in classpath", className));
            }
        }


        private void addCommitTableStorageModule() throws IOException {
            String className = null;
            try {

                CommitTableStore commitTableStore = config.getCommitTableStore();
                className = commitTableStore.toString();
                Constructor<?> constructor = Class.forName(className).getConstructor();
                Module module = (Module) constructor.newInstance();
                guiceModules.add(module);
                LOG.info("\t* Commit table store set to {}", module);
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new IllegalStateException(
                    String.format("Class named '%s' was not found in classpath", className));
            }
        }

        private void addHBaseConfigModuleIfRequired() throws IOException {
            if (config.getCommitTableStore() == CommitTableStore.HBASE
                || config.getTimestampStore() == TimestampStore.HBASE) {
                guiceModules.add(new HBaseConfigModule());
                HBaseLogin.loginIfNeeded(config.getLoginFlags());
            }
        }

        static Module instantiateGuiceMetricsModule(String className, List<String> metricsConfigs) {
            try {
                return Module.class.cast(Class.forName(className)
                                             .getConstructor(List.class)
                                             .newInstance(metricsConfigs));
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException
                | NoSuchMethodException | IllegalStateException | InvocationTargetException e) {
                LOG.error("Error instantiating and casting Guice metrics module from class {})", className, e);
                throw new IllegalStateException(e);
            }
        }

    }

    // ------------------------------------------------------------------------
    // ------------------- AbstractIdleService implementation -----------------
    // ------------------------------------------------------------------------

    @Override
    protected void startUp() throws Exception {
        tsoStateManager.register(requestProcessor);
        leaseManagement.startService();
        LOG.info("********** TSO Server running **********");
    }

    @Override
    protected void shutDown() throws Exception {
        leaseManagement.stopService();
        tsoStateManager.unregister(requestProcessor);
        LOG.info("********** TSO Server stopped successfully **********");
    }

    // ------------------------------------------------------------------------

    void attachShutDownHook() {
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
    public static void main(String[] args) {

        TSOServerCommandLineConfig config = TSOServerCommandLineConfig.parseConfig(args);

        if (config.hasHelpFlag()) {
            config.usage();
            System.exit(0);
        }

        try {
            TSOServer tsoServer = getInitializedTsoServer(config);
            tsoServer.attachShutDownHook();
            tsoServer.startAndWait();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }

    }

    // ************************* Helper methods *******************************

    static String getDefaultNetworkIntf() {
        BaseOperatingSystem currentOperatingSystem = BaseOperatingSystem.get();
        switch (currentOperatingSystem) {
            case Mac:
                return MAC_TSO_NET_IFACE;
            case Linux:
                return LINUX_TSO_NET_IFACE;
            default:
                throw new IllegalArgumentException(currentOperatingSystem.name());
        }
    }

    static final String OS_SYSTEM_PROPERTY = "os.name";

    enum BaseOperatingSystem {

        Mac("mac"),
        Linux("linux");

        private final String osId;

        BaseOperatingSystem(String osId) {
            this.osId = osId.toLowerCase();
        }

        private boolean isIncludedIn(String targetOSId) {

            if (targetOSId.indexOf(osId) != -1) {
                return true;
            }
            return false;
        }

        static BaseOperatingSystem get() {
            return get(OS_SYSTEM_PROPERTY, "Unknown OS");
        }

        /**
         * This method is intended only for testing purposes. Use get() instead
         */
        @VisibleForTesting
        static BaseOperatingSystem get(String sysProperty, String defaultValue) {
            String currentBaseOS = System.getProperty(sysProperty, defaultValue).toLowerCase();

            for (BaseOperatingSystem osValue : values()) {
                if (osValue.isIncludedIn(currentBaseOS)) {
                    return osValue;
                }
            }
            throw new IllegalArgumentException("Operating system not contemplated: " + currentBaseOS);
        }

    }

}
