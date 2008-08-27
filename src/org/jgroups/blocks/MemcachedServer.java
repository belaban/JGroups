package org.jgroups.blocks;

import java.net.InetAddress;

/** Server process which listens for memcached requests and forwards them to an instance of PartitionedHashmap.
 * Uses MemcachedConnector and PartitionedHashMap.
 * @author Bela Ban
 * @version $Id: MemcachedServer.java,v 1.1 2008/08/27 07:01:40 belaban Exp $
 */
public class MemcachedServer {
    private MemcachedConnector connector;
    private PartitionedHashMap<String,byte[]> cache;


    private void start(String props, InetAddress bind_addr, int port) throws Exception {
        connector=new MemcachedConnector(bind_addr, port, null);
        cache=new PartitionedHashMap(props, "memcached-cluster");

        
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
        int port=22122;
        String props="udp.xml";

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
            
            help();
            return;
        }
        new MemcachedServer().start(props, bind_addr, port);
    }

    private static void help() {
        System.out.println("MemcachedServer [-help] [-bind_addr <address>] [-port <port>] [-props <props>]");
    }
}
