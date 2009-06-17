// $Id: RpcDispatcherStressTest.java,v 1.11 2009/06/17 16:28:59 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;



/**
 * Example for RpcDispatcher (see also MessageDispatcher). Multiple threads will invoke print() on
 * all members and wait indefinitely for all responses (excluding of course crashed members). Run this
 * on 2 nodes for an extended period of time to see whether GroupRequest.doExecute() hangs.
 * @author Bela Ban
 */
public class RpcDispatcherStressTest implements MembershipListener {
    Channel            channel;
    RpcDispatcher      disp;
    RspList            rsp_list;
    Publisher[]        threads=null;
    int[]              results;

    public int print(int number) throws Exception {
        return number * 2;
    }


    public void start(String props, int num_threads, long interval, boolean discard_local) throws Exception {
        channel=new JChannel(props);
        if(discard_local)
            channel.setOpt(Channel.LOCAL, Boolean.FALSE);
        disp=new RpcDispatcher(channel, null, this, this);
        channel.connect("RpcDispatcherStressTestGroup");

        threads=new Publisher[num_threads];
        results=new int[num_threads];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Publisher(i, interval);
            results[i]=0;
        }

        System.out.println("-- Created " + threads.length + " threads. Press enter to start them " +
                           "('-' for sent message, '+' for received message)");
        System.out.println("-- Press enter to stop the threads");
        
        System.out.flush();
        System.in.read();
        System.in.skip(System.in.available());

        for(int i=0; i < threads.length; i++)
            threads[i].start();

        System.out.flush();
        System.in.read();
        System.in.skip(System.in.available());

        for(int i=0; i < threads.length; i++) {
            threads[i].stopThread();
            threads[i].join(2000);
        }

        System.out.println("\n");
        for(int i=0; i < threads.length; i++) {
            System.out.println("-- thread #" + i + ": called remote method " + results[i] + " times");            
        }


        System.out.println("Closing channel");
        channel.close();
        System.out.println("Closing channel: -- done");

        System.out.println("Stopping dispatcher");
        disp.stop();
        System.out.println("Stopping dispatcher: -- done");
    }


    /* --------------------------------- MembershipListener interface ---------------------------------- */

    public void viewAccepted(View new_view) {
        System.out.println("-- new view: " + new_view);
    }

    public void suspect(Address suspected_mbr) {
        System.out.println("-- suspected " + suspected_mbr);
    }



    public void block() {
        ;
    }

    /* ------------------------------ End of MembershipListener interface -------------------------------- */



    class Publisher extends Thread {
        int     rank=0;
        boolean running=true;
        int     num_calls=0;
        long    interval=1000;

        Publisher(int rank, long interval) {
            super();
            setDaemon(true);
            this.rank=rank;
            this.interval=interval;
        }

        public void stopThread() {
            running=false;
        }

        public void run() {
            while(running) {
                System.out.print(rank + "- ");
                disp.callRemoteMethods(null, "print", new Object[]{new Integer(num_calls)},
                        new Class[]{int.class}, GroupRequest.GET_ALL, 0);
                num_calls++;
                System.out.print(rank + "+ ");
                Util.sleep(interval);
            }
            results[rank]=num_calls;
        }
    }




    public static void main(String[] args) {
        String  props;
        int     num_threads=1;
        long    interval=1000;
        boolean discard_local=false;

        props="UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
                "ucast_recv_buf_size=16000;ucast_send_buf_size=16000;" +
                "mcast_send_buf_size=32000;mcast_recv_buf_size=64000;loopback=true):"+
                "PING(timeout=2000;num_initial_members=3):"+
                "MERGE2(min_interval=5000;max_interval=10000):"+
                "FD_SOCK:"+
                "VERIFY_SUSPECT(timeout=1500):"+
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=1000,1500,2000,3000):"+
                "UNICAST(timeout=1000,1500,2000,3000):"+
                "pbcast.STABLE(desired_avg_gossip=10000):"+
                "FRAG(frag_size=8192;down_thread=false;up_thread=false):"+
                "pbcast.GMS(join_timeout=5000;print_local_addr=true):"+
                "pbcast.STATE_TRANSFER";
        
        try {
            for(int i=0; i < args.length; i++) {
                if("-num_threads".equals(args[i])) {
                    num_threads=Integer.parseInt(args[++i]);
                    continue;
                }
                if("-interval".equals(args[i])) {
                    interval=Long.parseLong(args[++i]);
                    continue;
                }
                if("-props".equals(args[i])) {
                    props=args[++i];
                    continue;
                }
                if("-discard_local".equals(args[i])) {
                    discard_local=true;
                    continue;
                }
                help();
                return;
            }


            new RpcDispatcherStressTest().start(props, num_threads, interval, discard_local);
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }


    static void help() {
        System.out.println("RpcDispatcherStressTest [-help] [-interval <msecs>] " +
                           "[-num_threads <number>] [-props <stack properties>] [-discard_local]");
    }
}
