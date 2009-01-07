package org.jgroups.demos;

import org.jgroups.blocks.Cache;
import org.jgroups.blocks.ReplCache;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;

/**
 * Runs an instance of ReplCache and allows a user to add, get and remove elements
 * @author Bela Ban
 * @version $Id: ReplCacheServer.java,v 1.3 2009/01/07 13:14:56 belaban Exp $
 */
public class ReplCacheServer {
    private ReplCache<String,String> cache;
    private static final String BASENAME="replcache";


    private void start(String props, InetAddress bind_addr, int port, int min_threads, int max_threads,
                       long rpc_timeout, long caching_time, boolean migrate_data, boolean use_l1_cache,
                       int l1_max_entries, long l1_reaping_interval,
                       int l2_max_entries, long l2_reaping_interval) throws Exception {
        MBeanServer server=ManagementFactory.getPlatformMBeanServer();

        cache=new ReplCache(props, "replcache-cluster");
        cache.setCallTimeout(rpc_timeout);
        cache.setCachingTime(caching_time);
        cache.setMigrateData(migrate_data);
        JmxConfigurator.register(cache, server, BASENAME + ":name=cache");
        JmxConfigurator.register(cache.getL2Cache(), server, BASENAME + ":name=l2-cache");

        if(use_l1_cache) {
            Cache<String,String> l1_cache=new Cache<String,String>();
            cache.setL1Cache(l1_cache);
            if(l1_reaping_interval > 0)
                l1_cache.enableReaping(l1_reaping_interval);
            if(l1_max_entries > 0)
                l1_cache.setMaxNumberOfEntries(l1_max_entries);
            JmxConfigurator.register(cache.getL1Cache(), server, BASENAME + ":name=l1-cache");
        }

        if(l2_max_entries > 0 || l2_reaping_interval > 0) {
            Cache<String, ReplCache.Value<String>> l2_cache=cache.getL2Cache();
            if(l2_max_entries > 0)
                l2_cache.setMaxNumberOfEntries(l2_max_entries);
            if(l2_reaping_interval > 0)
                l2_cache.enableReaping(l2_reaping_interval);
        }


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                cache.stop();
            }
        });

        cache.start();

        mainLoop();
    }


    private void mainLoop() throws IOException {
        while(true) {
            int c;
            System.in.skip(System.in.available());
            System.out.println("\n[1] Put [2] Get [3] Remove [4] Dump [5] view [x] Exit");
            c=System.in.read();
            switch(c) {
                case -1:
                    break;
                case '1':
                    put();
                    break;
                case '2':
                    String key=readString("key");
                    String val=cache.get(key);
                    System.out.println("val = " + val);
                    break;
                case '3':
                    key=readString("key");
                    cache.remove(key);
                    break;
                case '4':
                    System.out.println(cache.dump());
                    break;
                case '5':
                    System.out.println("view = " + cache.getView());
                    break;
                case 'x':
                    cache.stop();
                    return;
                default:
                    break;
            }
        }
    }

    private void put() throws IOException {
        String key=readString("key");
        String val=readString("value");
        String tmp=readString("replication count");
        short count=Short.parseShort(tmp);
        tmp=readString("timeout");
        long timeout=Long.parseLong(tmp);
        cache.put(key, val, count, timeout);
    }



    private static void skip(InputStream in) throws IOException {
        System.in.skip(in.available());
    }

    private static String readString(String s) throws IOException {
        int c;
        boolean looping=true;
        StringBuilder sb=new StringBuilder();
        System.out.print(s + ": ");
        System.out.flush();
        skip(System.in);

        while(looping) {
            c=System.in.read();
            switch(c) {
                case -1:
                case '\n':
                case 13:
                    looping=false;
                    break;
                default:
                    sb.append((char)c);
                    break;
            }
        }

        return sb.toString();
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
        new ReplCacheServer().start(props, bind_addr, port, min_threads, max_threads, rpc_timeout, caching_time,
                                    migrate_data, use_l1_cache, l1_max_entries, l1_reaping_interval,
                                    l2_max_entries, l2_reaping_interval);
    }

    private static void help() {
        System.out.println("ReplCacheServer [-help] [-bind_addr <address>] [-port <port>] [-props <props>] " +
                "[-min_threads <min>] [-max_threads <max>] [-rpc_timeout <ms>] [-caching_time <ms>] " +
                "[-migrate_data <true|false>] [-use_l1_cache <true|false>] " +
                "[-l1_max_entries <num>] [-l1_reaping_interval <ms>] " +
                "[-l2_max_entries <num>] [-l2_reaping_interval <ms>] ");
    }
}
