package org.jgroups.demos;

import org.jgroups.blocks.PartitionedHashMap;
import org.jgroups.blocks.Cache;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author Bela Ban
 * @version $Id: PartitionedHashMapDemo.java,v 1.1 2008/08/25 14:17:51 belaban Exp $
 */
public class PartitionedHashMapDemo {

    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }



        PartitionedHashMap<String,String> map=new PartitionedHashMap<String,String>(props, "demo-cluster");
        Cache<String,String> l1_cache=new Cache<String,String>();
        l1_cache.setMaxNumberOfEntries(5);
        l1_cache.disableReaping();
        map.setL1Cache(l1_cache);
        Cache<String, String> l2_cache=map.getL2Cache();
        l2_cache.enableReaping(10000);
        map.start();

        while(true) {
            int ch=Util.keyPress("[1] put [2] get [3] print [q] quit");

            switch(ch) {
                case '1':
                    String key=readLine("key: ");
                    String val=readLine("val: ");
                    String caching_time=readLine("ttl: ");
                    map.put(key, val, Long.parseLong(caching_time));
                    break;
                case '2':
                    key=readLine("key: ");
                    val=map.get(key);
                    System.out.println("val = " + val);
                    break;
                case '3':
                    System.out.println("L1 cache:\n" + map.getL1Cache());
                    System.out.println("L2 cache:\n" + map.getL2Cache());
                    break;
                case 'q':
                    l1_cache.stop();
                    map.stop();
                    return;
            }
        }

    }


    private static void help() {
        System.out.println("PartitionedHashMapDemo [-props <props>]");
    }

    static String readLine(String msg) {
        BufferedReader reader=null;
        String tmp=null;

        try {
            System.out.print(msg);
            System.out.flush();
            System.in.skip(System.in.available());
            reader=new BufferedReader(new InputStreamReader(System.in));
            tmp=reader.readLine().trim();
            return tmp;
        }
        catch(Exception e) {
            return null;
        }
    }


}
