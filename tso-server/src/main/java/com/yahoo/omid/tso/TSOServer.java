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
import com.yahoo.omid.tso.hbase.HBaseStorageModule;

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

    static TSOServer getInitializedTsoServer(String[] args) throws IOException {
        TSOServerCommandLineConfig config = TSOServerCommandLineConfig.parseConfig(args);

        if (config.hasHelpFlag()) {
            config.usage();
            return null;
        }

        // NOTE: The guice config is in here following the best practices in:
        // https://code.google.com/p/google-guice/wiki/AvoidConditionalLogicInModules
        // This is due to the fact that the target storage can be selected from the
        // command line
        List<Module> guiceModules = new ArrayList<Module>();
        guiceModules.add(new TSOModule(config));
        if (config.isHBase()) {
            guiceModules.add(new HBaseStorageModule());
        } else {
            guiceModules.add(new InMemoryStorageModule());
        }
        Injector injector = Guice.createInjector(guiceModules);
        return injector.getInstance(TSOServer.class);
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
        channelGroup.close().awaitUninterruptibly();
        factory.releaseExternalResources();
        LOG.info("********** TSO Server stopped successfully **********");
    }

    /**
     * This is where all starts on the server side
     */
    public static void main(String[] args) throws Exception {
        
        TSOServer tsoServer = getInitializedTsoServer(args);
        if(tsoServer != null)
            tsoServer.startAndWait();

    }
    
}