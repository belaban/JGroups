// $Id: Ping.java,v 1.1.1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.protocols.PingRsp;
import org.jgroups.util.Util;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;




/**
 Determines the initial set of members given a group name and properties and prints them to stdout. Does not
 connect to the group, but - using a minimal stack of UPD/PING or TCP/TCPPING - only sends a FIND_INITIAL_MBRS
 down the stack (PING or TCPPING has to be present for this to work) and waits for FIND_INITIAL_MBRS_OK. When
 received, the results are printed and then the app terminates.<p>
 To connect to any group, it is essential that the groupname given (-group) is the same as the one of the group
 and the properties are the same as the bottom 2 layers of the group properties, e.g. if the group properties are:
 <pre>
 TCP(start_port=7800):TCPPING(initial_hosts=daddy.nms.fnc.fujitsu.com[7800];port_range=2;timeout=5000;num_initial_members=3;up_thread=true;down_thread=true):pbcast.STABLE(desired_avg_gossip=200000;down_thread=false;up_thread=false):pbcast.NAKACK(down_thread=true;up_thread=true;gc_lag=100;retransmit_timeout=3000):pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;print_local_addr=false;down_thread=true;up_thread=true)
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


    public Ping(String props, boolean trace, boolean printall) throws Exception {
        print_all_events=printall;
        if(trace) Trace.init();
        channel=new JChannel(props);
        channel.setUpHandler(this);
    }


    public void go(String groupname) {

        try {
            channel.connect(groupname);
            Trace.setIdentifier(channel.getLocalAddress().toString());
            channel.down(new Event(Event.FIND_INITIAL_MBRS));
        }
        catch(Exception e) {
            System.err.println("Ping.go(): " + e);
            System.exit(1);
        }
    }


    /** UpHandler interface */
    public void up(Event evt) {
        Vector v;
        PingRsp rsp;

        if(evt.getType() == Event.FIND_INITIAL_MBRS_OK) {
            v=(Vector)evt.getArg();

            System.out.println("Found " + v.size() + " members");
            for(int i=0; i < v.size(); i++) {
                rsp=(PingRsp)v.elementAt(i);
                System.out.println("Rsp #" + (i + 1) + ": " + rsp);
            }

            if(v.size() > 0)
                verifyCoordinator(v);

            System.exit(1);
        }
        else {
            if(print_all_events)
                System.out.println(">> " + evt);
        }
    }


    static void verifyCoordinator(Vector rsps) {
        Hashtable votes=new Hashtable();  // coord address, list of members who voted for this guy
        PingRsp rsp;
        Vector v;
        Address coord, mbr;

        for(int i=0; i < rsps.size(); i++) {
            rsp=(PingRsp)rsps.elementAt(i);
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
            System.out.println("Votes: " + val + "\n");
        }
    }


    public static void main(String[] args) {
        Ping ping=null;
        boolean trace=false;
        String groupname=Util.shortName(Util.getHostname());
        boolean printall=false;
        String props="UDP(mcast_addr=224.0.0.200;mcast_port=7500;ip_ttl=0;" +
                "ucast_send_buf_size=30000;ucast_recv_buf_size=60000):" +
                "PING(timeout=5000;num_initial_members=30)";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-help")) {
                usage();
                return;
            }
            if(args[i].equals("-trace")) {
                trace=true;
                continue;
            }
            if(args[i].equals("-printall")) {
                printall=true;
                continue;
            }
            if(args[i].equals("-group")) {
                groupname=args[++i];
                continue;
            }
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            usage();
            return;
        }


        try {
            ping=new Ping(props, trace, printall);
            ping.go(groupname);
        }
        catch(Exception e) {
            System.err.println("Ping.main(): " + e);
            System.exit(0);
        }
    }


    static void usage() {
        System.out.println("Ping [-help] [-trace] [-group <groupname>] [-props <properties>] [-printall]");
    }


}





