package org.jgroups.tests;


import org.jgroups.JChannel;
import org.jgroups.JChannelFactory;
import org.jgroups.blocks.DistributedHashtable;
import org.jgroups.blocks.ReplicatedHashtable;

import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;


/**
 * Add 1000 1k items to a hashtable (Distributed- or Replicated Hashtable) simultaneously
 * @author Bela Ban
 */
public class HashtableTest {
    static int  NUM_ITEMS=1000;
    static long start, stop;

    
    
    static class Notifier implements DistributedHashtable.Notification, ReplicatedHashtable.Notification {
	int num_items=0;
	int tmp;

	Notifier(int n) {num_items=n;}

	public void entrySet(Object key, Object value) {
	    tmp=((Integer)key).intValue();
	    if(tmp % 100 == 0)
		System.out.println("** entrySet(" + key + ')');
	    if(tmp >= num_items) {
		stop=System.currentTimeMillis();
		System.out.println(num_items + " elements took " + 
				   (stop - start) + " msecs to receive and insert");
	    }
	}

	public void entryRemoved(Object key) {

	}

	public void viewChange(Vector new_mbrs, Vector old_mbrs) {
	    System.out.println("** viewChange(" + new_mbrs + ", " + old_mbrs + ')');
	}

        public void contentsSet(Map m) {
            System.out.println("** contentsSet (" + (m != null? m.size()+"" : "0") + " items");
        }

        public void contentsCleared() {
            System.out.println("** contentsCleared()");
        }

    }
    

    public static void main(String[] args) {
	Hashtable            ht;
	int                  i;
	byte[]               buf;
	boolean              use_replicated_hashtable=false;



	
	String props="UDP(mcast_addr=224.0.0.36;mcast_port=55566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=5000):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=16000;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;" +
            "print_local_addr=true):" +
            "pbcast.STATE_TRANSFER";
	
	

	for(i=0; i < args.length; i++) {
	    if("-help".equals(args[i])) {
		help();
		return;
	    }
	    if("-use_rht".equals(args[i])) {
		use_replicated_hashtable=true;
		continue;
	    }
	    if("-props".equals(args[i])) {
		props=args[++i];
		continue;
	    }
	}




	
	
	try {
	    if(use_replicated_hashtable) {
		ht=new ReplicatedHashtable("HashtableTest", new JChannelFactory(), props, 1000);
		((ReplicatedHashtable)ht).addNotifier(new Notifier(NUM_ITEMS));
	    }
	    else {
		ht=new DistributedHashtable("HashtableTest", new JChannelFactory(), props, 1000);
		// ((DistributedHashtable)ht).addNotifier(new MyNotifier());
	    }

	    System.out.println("Hashtable already has " + ht.size() + " items");

	    System.out.print("Press key to insert " + NUM_ITEMS + " 1k items into DistributedHashtable");
	    System.in.read();

	    buf=new byte[1024];
	    for(i=0; i < buf.length; i++)
		buf[i]=(byte)'x';

	    start=System.currentTimeMillis();
	    for(i=1; i <= NUM_ITEMS; i++) {
		ht.put(new Integer(i), buf);
		if(i % 100 == 0)
		    System.out.println(i); // will slow down insertion
	    }
	    stop=System.currentTimeMillis();
	    System.out.println(i + " elements took " + (stop - start) + " msecs to " +
			       (use_replicated_hashtable? "send" : "insert"));
	    while(true) {
		System.out.println("Hashtable has " + ht.size() + " entries");
		System.in.read();
	    }
	}
	catch(Exception ex) {
	    System.err.println(ex);
	}
    }


    static void help() {
	System.out.println("HashtableTest [-help] [-use_rht] [-props <properties>]");
    }

}
