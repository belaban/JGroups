package org.jgroups.demos;

import org.jgroups.blocks.Cache;
import org.jgroups.blocks.MemcachedConnector;
import org.jgroups.blocks.PartitionedHashMap;
import org.jgroups.jmx.JmxConfigurator;

import javax.management.MBeanServer;
import java.net.InetAddress;
import java.lang.management.ManagementFactory;

/** Server process which listens for memcached requests and forwards them to an instance of PartitionedHashMap.
 * Uses MemcachedConnector and PartitionedHashMap.
 * @author Bela Ban
 */
public class MemcachedServer {
    private MemcachedConnector connector;
    private PartitionedHashMap<String, byte[]> cache;
    private static final String BASENAME="memcached";


    private void start(String props, InetAddress bind_addr, int port, int min_threads, int max_threads,
                       long rpc_timeout, long caching_time, boolean migrate_data, boolean use_l1_cache,
                       int l1_max_entries, long l1_reaping_interval,
                       int l2_max_entries, long l2_reaping_interval) throws Exception {
        MBeanServer server=ManagementFactory.getPlatformMBeanServer();
        connector=new MemcachedConnector(bind_addr, port, null);
        connector.setThreadPoolCoreThreads(min_threads);
        connector.setThreadPoolMaxThreads(max_threads);
        JmxConfigurator.register(connector, server, BASENAME + ":name=connector");

        cache=new PartitionedHashMap(props, "memcached-cluster");
        cache.setCallTimeout(rpc_timeout);
        cache.setCachingTime(caching_time);
        cache.setMigrateData(migrate_data);
        JmxConfigurator.register(cache, server, BASENAME + ":name=cache");
        JmxConfigurator.register(cache.getL2Cache(), server, BASENAME + ":name=l2-cache");

        if(use_l1_cache) {
            Cache<String,byte[]> l1_cache=new Cache<>();
            cache.setL1Cache(l1_cache);
            if(l1_reaping_interval > 0)
                l1_cache.enableReaping(l1_reaping_interval);
            if(l1_max_entries > 0)
                l1_cache.setMaxNumberOfEntries(l1_max_entries);
            JmxConfigurator.register(cache.getL1Cache(), server, BASENAME + ":name=l1-cache");
        }

        if(l2_max_entries > 0 || l2_reaping_interval > 0) {
            Cache<String,byte[]> l2_cache=cache.getL2Cache();
            if(l2_max_entries > 0)
                l2_cache.setMaxNumberOfEntries(l2_max_entries);
            if(l2_reaping_interval > 0)
                l2_cache.enableReaping(l2_reaping_interval);
        }

        connector.setCache(cache);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                cache.stop();
                try {connector.stop();} catch(Exception e) {}
            }
        });

        cache.start();
        connector.start();
    }



    public static void main(String[] args) throws Exception {
        InetAddress bind_addr=null;
        int port=11211;
        String props="udp.xml";
        int min_threads=1, max_threads=500;
        long rpc_timeout=1500L, caching_time=30000L;
        boolean migrate_data=true, use_l1_cache=true;
        int l1_max_entries=5000, l2_max_entries=-1;
        long l1_reaping_interval=-1, l2_reaping_interval=30000L;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-bind_addr")) {
                bind_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-port") || args[i].equals("-p")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-min_threads")) {
                min_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-max_threads")) {
                max_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-rpc_timeout")) {
                rpc_timeout=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-caching_time")) {
                caching_time=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-migrate_data")) {
                migrate_data=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-use_l1_cache")) {
                use_l1_cache=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-l1_max_entries")) {
                l1_max_entries=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-l1_reaping_interval")) {
                l1_reaping_interval=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-l2_max_entries")) {
                l2_max_entries=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-l2_reaping_interval")) {
                l2_reaping_interval=Long.parseLong(args[++i]);
                continue;
            }
            
            help();
            return;
        }
        new MemcachedServer().start(props, bind_addr, port, min_threads, max_threads, rpc_timeout, caching_time,
                                    migrate_data, use_l1_cache, l1_max_entries, l1_reaping_interval,
                                    l2_max_entries, l2_reaping_interval);
    }

    private static void help() {
        System.out.println("MemcachedServer [-help] [-bind_addr <address>] [-port <port>] [-props <props>] " +
                "[-min_threads <min>] [-max_threads <max>] [-rpc_timeout <ms>] [-caching_time <ms>] " +
                "[-migrate_data <true|false>] [-use_l1_cache <true|false>] " +
                "[-l1_max_entries <num>] [-l1_reaping_interval <ms>] " +
                "[-l2_max_entries <num>] [-l2_reaping_interval <ms>] ");
    }
}
