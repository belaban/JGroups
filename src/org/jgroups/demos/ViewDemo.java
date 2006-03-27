// $Id: ViewDemo.java,v 1.11 2006/03/27 08:34:24 belaban Exp $

package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.Util;

import java.util.HashMap;


/**
 * Demos the reception of views using a PullPushAdapter. Just start a number of members, and kill them
 * randomly. The view should always be correct.
 */
public class ViewDemo implements MembershipListener {
    private Channel channel;


    public void viewAccepted(View new_view) {
        System.out.println("** New view: " + new_view);
        // ", channel.getView()=" + channel.getView());
    }


    /**
     * Called when a member is suspected
     */
    public void suspect(Address suspected_mbr) {
        System.out.println("Suspected(" + suspected_mbr + ')');
    }


    /**
     * Block sending and receiving of messages until ViewAccepted is called
     */
    public void block() {

    }


    public void start(String props, boolean use_additional_data) throws Exception {

        channel=new JChannel(props);
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);

        if(use_additional_data) {
            HashMap m=new HashMap();
            m.put("additional_data", "bela".getBytes());
            channel.down(new Event(Event.CONFIG, m));
        }

        channel.connect("ViewDemo");
        new PullPushAdapter(channel, this);

        while(true) {
            Util.sleep(10000);
        }
    }


    public static void main(String args[]) {
        ViewDemo t=new ViewDemo();
        boolean use_additional_data=false;
        String props="UDP(mcast_addr=228.5.5.5;mcast_port=45566;ip_ttl=32;" +
                "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
                "UNICAST(timeout=600,1200,2400):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
                "shun=false;print_local_addr=true)";



        /*
          String props="TCP(start_port=7800):" +
          "TCPPING(initial_hosts=66.87.163.92[7800];port_range=2;" +
          "timeout=5000;num_initial_members=3;up_thread=true;down_thread=true):" +
          "VERIFY_SUSPECT(timeout=1500):" +
          "pbcast.STABLE(desired_avg_gossip=200000;down_thread=false;up_thread=false):"+
          "pbcast.NAKACK(down_thread=true;up_thread=true;gc_lag=100;retransmit_timeout=3000):" +
          "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;" +
          "print_local_addr=true;down_thread=true;up_thread=true)";
        */


        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-use_additional_data".equals(args[i])) {
                use_additional_data=new Boolean(args[++i]).booleanValue();
                continue;
            }
            help();
            return;
        }

        try {
            t.start(props, use_additional_data);
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println("ViewDemo [-props <properties>] [-help] [-use_additional_data <flag>]");
    }

}
