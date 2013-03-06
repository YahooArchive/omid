package com.yahoo.omid.notifications.conf;

import org.apache.commons.cli.Options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Server side options 
 */
public class DeltaOmidServerConfig extends Options {

    final static String HELP_OPT = "help";
    final static String ZK_OPT = "zk";

    public static DeltaOmidServerConfig parseConfig(String args[]){
        DeltaOmidServerConfig config = new DeltaOmidServerConfig();

        new JCommander(config, args);

        return config;
    }
    
    public static DeltaOmidServerConfig getDefaultConfig(){
        return new DeltaOmidServerConfig();
    }
    
    public DeltaOmidServerConfig() {
        this.zkServers = System.getProperty("ZK_SERVERS", "localhost:2181");
    }
    
    @Parameter(names = "-zk", description = "ZooKeeper ensemble -> host1:port1,host2:port2...")
    private String zkServers;
    
    
    public String getZkServers(){
        return zkServers;
    }
    
}
