// $Id: Ping.java,v 1.15 2009/06/17 16:28:59 belaban Exp $

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Util;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;




/**
 Determines the initial set of members given a group name and properties and prints them to stdout. Does not
 connect to the group, but - using a minimal stack of UPD/PING or TCP/TCPPING - only sends a FIND_INITIAL_MBRS
 down the stack (PING or TCPPING has to be present for this to work) and waits for FIND_INITIAL_MBRS_OK. When
 received, the results are printed and then the app terminates.<p>
 To connect to any group, it is essential that the groupname given (-group) is the same as the one of the group
 and the properties are the same as the bottom 2 layers of the group properties, e.g. if the group properties are:
 <pre>
 TCP(start_port=7800):TCPPING(initial_hosts=daddy.nms.fnc.fujitsu.com[7800];port_range=2;timeout=5000;num_initial_members=3;up_thread=true;down_thread=true):pbcast.STABLE(desired_avg_gossip=200000;down_thread=false;up_thread=false):pbcast.NAKACK(down_thread=true;up_thread=true;gc_lag=100;retransmit_timeout=3000):pbcast.GMS(join_timeout=5000;print_local_addr=false;down_thread=true;up_thread=true)
 </pre>
 ,then the stack properties should be:
 <pre>
 TCP(start_port=7800):TCPPING(initial_hosts=daddy.nms.fnc.fujitsu.com[7800];port_range=2;timeout=5000;num_initial_members=3)
 </pre>
 @author Bela Ban, June 12 2001
 */
public class Ping implements UpHandler {
    Channel channel=null;
    boolean print_all_events=false;


    public Ping(String props, boolean printall) throws Exception {
        print_all_events=printall;
        channel=new JChannel(props);
        channel.setUpHandler(this);
    }


    public void go(String groupname) {

        try {
            channel.connect(groupname);
            List<PingData> responses = (List<PingData>) channel.downcall(new Event(Event.FIND_INITIAL_MBRS));
            for(int i=0; i < responses.size(); i++) {
            	PingData rsp=responses.get(i);
                System.out.println("Rsp #" + (i + 1) + ": " + rsp);
            }

            if(!responses.isEmpty())
                verifyCoordinator(responses);

            System.exit(1);
        }
        catch(Exception e) {
            System.err.println("Ping.go(): " + e);
            System.exit(1);
        }
    }

    static void verifyCoordinator(List<PingData> rsps) {
        Hashtable votes=new Hashtable();  // coord address, list of members who voted for this guy
        PingData rsp;
        Vector v;
        Address coord, mbr;

        for(int i=0; i < rsps.size(); i++) {
            rsp=rsps.get(i);
            coord=rsp.getCoordAddress();
            mbr=rsp.getAddress();
            v=(Vector)votes.get(coord);
            if(v == null) {
                v=new Vector();
                votes.put(coord, v);
            }
            if(!v.contains(mbr))
                v.addElement(mbr);
        }

        System.out.println("");
        if(votes.size() > 1)
            System.err.println("*** Found more than 1 coordinator !");

        printVotes(votes);
    }


    static void printVotes(Hashtable votes) {
        Object key;
        Vector val;
        for(Enumeration e=votes.keys(); e.hasMoreElements();) {
            key=e.nextElement();
            val=(Vector)votes.get(key);
            System.out.println("\n\nCoord: " + key);
            System.out.println("Votes: " + val + '\n');
        }
    }


    public static void main(String[] args) {
        Ping ping=null;
        String groupname=Util.shortName(Util.getHostname());
        boolean printall=false;
        String props="UDP(mcast_addr=224.0.0.200;mcast_port=7500;ip_ttl=0;" +
                "ucast_send_buf_size=30000;ucast_recv_buf_size=60000):" +
                "PING(timeout=5000;num_initial_members=30)";

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                usage();
                return;
            }
            if("-printall".equals(args[i])) {
                printall=true;
                continue;
            }
            if("-group".equals(args[i])) {
                groupname=args[++i];
                continue;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            usage();
            return;
        }


        try {
            ping=new Ping(props, printall);
            ping.go(groupname);
        }
        catch(Exception e) {
            System.err.println("Ping.main(): " + e);
            System.exit(0);
        }
    }


    static void usage() {
        System.out.println("Ping [-help] [-group <groupname>] [-props <properties>] [-printall]");
    }


	public Object up(Event evt) {	
		return null;
	}


}





