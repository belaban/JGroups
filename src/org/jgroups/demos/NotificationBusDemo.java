// $Id: NotificationBusDemo.java,v 1.7 2006/05/25 12:10:19 belaban Exp $


package org.jgroups.demos;


import org.jgroups.Address;
import org.jgroups.blocks.NotificationBus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Vector;




/**
 * Demoes the NotificationBus (without caching). Start a number of members and type in messages. All members will
 * receive the messages. View changes will also be displayed (e.g. member joined, left).
 * @author Bela Ban
 */
public class NotificationBusDemo implements NotificationBus.Consumer {
    NotificationBus bus=null;
    BufferedReader  in=null;
    String          line;
    final long      timeout=0;
    final Vector    cache=null;
    Log             log=LogFactory.getLog(getClass());



    public void start(String bus_name, String props) {
	try {

	    bus=new NotificationBus(bus_name, props);
        bus.setConsumer(this);
        bus.start();
	    //System.out.println("Getting the cache from coordinator:");
	    //cache=(Vector)bus.getCacheFromCoordinator(3000, 3);
	    //if(cache == null) cache=new Vector();
	    //System.out.println("cache is " + cache);

	    in=new BufferedReader(new InputStreamReader(System.in));	    
	    while(true) {
		try {
		    System.out.print("> "); System.out.flush();
		    line=in.readLine();
		    if(line.startsWith("quit") || line.startsWith("exit")) {
			bus.stop();
			bus=null;
			break;
		    }
		    bus.sendNotification(line);
		}
		catch(Exception e) {
		    log.error(e);
		}
	    }
	}
	catch(Exception ex) {
	    log.error(ex);
	}
	finally {
	    if(bus != null)
		bus.stop();
	}
    }


    
    public void handleNotification(Serializable n) {
	System.out.println("** Received notification: " + n);
	//if(cache != null)
	//  cache.addElement(n);
	//System.out.println("cache is " + cache);
    }

    
    public Serializable getCache() {
	// return cache;
	return null;
    }


    public void memberJoined(Address mbr) {
	System.out.println("** Member joined: " + mbr);
    }

    public void memberLeft(Address mbr) {
	System.out.println("** Member left: " + mbr);
    }



    public static void main(String[] args) {
	String name="BusDemo";
        String props="UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=32;" +
                "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
                "UNICAST(timeout=5000):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=8096;down_thread=false;up_thread=false):" +
                // "CAUSAL:" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
                "shun=false;print_local_addr=true)";

	for(int i=0; i < args.length; i++) {
	    if("-bus_name".equals(args[i])) {
		name=args[++i];
		continue;
	    }
	    if("-props".equals(args[i])) {
		props=args[++i];
		continue;
	    }
	    System.out.println("NotificationBusDemo [-help] [-bus_name <name>] " +
			       "[-props <properties>]");
	    return;
	}
	System.out.println("Starting NotificationBus with name " + name);
	new NotificationBusDemo().start(name, props);
    } 

   
}
