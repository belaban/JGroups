// $Id: DistributedHashtableTest.java,v 1.3 2004/03/30 06:47:34 belaban Exp $

package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.JChannelFactory;
import org.jgroups.blocks.DistributedHashtable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;




/**
 * Tests the DistributedHashtable interactively
 * @author Bela Ban
 */
public class DistributedHashtableTest implements Runnable {
    DistributedHashtable ht;
    long timeout=500;
    Thread t=null;


    public void start(String props, long timeout) throws Exception {
        this.timeout=timeout;
        ht=new DistributedHashtable("HashtableTest", new JChannelFactory(), props, timeout);
    }


    public void eventLoop() throws Exception {
        int c;

        while(true) {
            System.out.println("[1] Insert [2] Start [3] Stop [4] Delete [5] Size [6] Print [q] Quit");
            c=System.in.read();
            switch(c) {
                case -1:
                    break;
                case '1':
                    insertEntries();
                    break;
                case '2':
                    start();
                    break;
                case '3':
                    stop();
                    break;
                case '4':
                    deleteEntries();
                    break;
                case '5':
                    printSize();
                    break;
                case '6':
                    printContents();
                    break;
                case 'q':
                    ht.stop();
                    return;
                default:
                    break;
            }
        }
    }


    public void insertEntries() {
        try {
            DataInputStream in=new DataInputStream(System.in);
            Address local=ht.getLocalAddress();
            long start, stop;
            System.out.print("Number of entries: ");
            System.out.flush();
            System.in.skip(System.in.available());
            String line=in.readLine();
            int num=Integer.parseInt(line);
            start=System.currentTimeMillis();
            for(int i=0; i < num; i++) {
                if(i % 100 == 0)
                    System.out.print(i + " ");
                ht.put(local.toString() + "#" + i, new Integer(i));
            }
            stop=System.currentTimeMillis();
            double num_per_sec=num / ((stop-start)/1000.0);
            System.out.println("\nInserted " + num + " elements in " + (stop - start) +
                    " ms, size=" + ht.size() + " [" + num_per_sec + " / sec]");
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }

    public void deleteEntries() {
        try {
            DataInputStream in=new DataInputStream(System.in);
            Address local=ht.getLocalAddress();
            Set keys;
            long start, stop;
            System.out.print("Number of entries: ");
            System.out.flush();
            System.in.skip(System.in.available());
            String line=in.readLine();
            int num=Integer.parseInt(line);
            Object key;
            int i=0;

            Iterator it=ht.keySet().iterator();
            keys=new TreeSet();
            while(i++ < num) {
                try {
                    key=it.next();
                    keys.add(key);
                }
                catch(Exception ex) {
                    break;
                }
            }
            start=System.currentTimeMillis();
            for(it=keys.iterator(); it.hasNext();) {
                key=it.next();
                ht.remove(key);
            }
            stop=System.currentTimeMillis();
            double num_per_sec=num / ((stop-start)/1000.0);
            System.out.println("\nRemoved " + keys.size() + " elements in " + (stop - start) +
                    "ms, size=" + ht.size() + " [" + num_per_sec + " / sec]");
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }

    public void start() {
        if(t == null) {
            t=new Thread(this, "Modifier thread");
            t.start();
        }
    }

    public void stop() {
        if(t != null)
            t=null;
    }

    public void printSize() {
        if(ht != null)
            System.out.println("size=" + ht.size());
    }

    public void printContents() {
        Set s=ht.keySet();
        TreeSet ss=new TreeSet(s);
        Object key;

        for(Iterator it=ss.iterator(); it.hasNext();) {
            key=it.next();
            System.out.println(key + " --> " + ht.get(key));
        }
    }


    public void run() {
        while(t != null) {
            // todo: random insertion, deletion, reads
            Util.sleep(timeout);
        }
    }


    public static void main(String[] args) {
        long timeout=500; // timeout in ms between puts()/gets()/removes()
        String props="UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
                "ucast_recv_buf_size=16000;ucast_send_buf_size=16000;" +
                "mcast_send_buf_size=32000;mcast_recv_buf_size=64000;loopback=true):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800;max_xmit_size=8192):" +
                "UNICAST(timeout=2000):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=8192;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-help")) {
                help();
                return;
            }
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-timeout")) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
        }



        try {
            DistributedHashtableTest test=new DistributedHashtableTest();
            test.start(props, timeout);
            test.eventLoop();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }

    static void help() {
        System.out.println("DistributedHashtableTest [-help] [-props <props>] [-timeout <timeout>]");
    }
}
