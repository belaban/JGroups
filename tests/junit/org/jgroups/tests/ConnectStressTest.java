
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Vector;
import java.util.concurrent.CyclicBarrier;


/**
 * Creates 1 channel, then creates NUM channels, all try to join the same channel concurrently.
 * @author Bela Ban Nov 20 2003
 * @version $Id: ConnectStressTest.java,v 1.28 2008/04/21 16:18:13 belaban Exp $
 */
public class ConnectStressTest extends ChannelTestBase {
    CyclicBarrier    start_connecting=null;
    CyclicBarrier    connected=null;
    CyclicBarrier    received_all_views=null;
    CyclicBarrier    start_disconnecting=null;
    CyclicBarrier    disconnected=null;
    static final int NUM=20;
    final MyThread[] threads=new MyThread[NUM];
    JChannel         channel=null;
    static String    groupname=getUniqueClusterName("ConnectStressTest");
    String           PROPS=null;



    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }


    @Test
    public void testConcurrentJoinsAndLeaves() throws Exception {
        start_connecting=new CyclicBarrier(NUM +1);
        connected=new CyclicBarrier(NUM +1);
        received_all_views=new CyclicBarrier(NUM +1);
        start_disconnecting=new CyclicBarrier(NUM +1);
        disconnected=new CyclicBarrier(NUM +1);

        long start, stop;

        //  create main channel - will be coordinator for JOIN requests
        channel=createChannel(true);
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        changeProperties(channel);
        PROPS=channel.getProperties();

        start=System.currentTimeMillis();
        channel.connect(groupname);
        stop=System.currentTimeMillis();
        log(channel.getLocalAddress() + " connected in " + (stop-start) + " msecs (" +
                    channel.getView().getMembers().size() + " members). VID=" + channel.getView().getVid());
        assert channel.getView().getMembers().size() == 1 : "view should have size == 1 after initial connect ";

        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(i);
            threads[i].start();
        }

        // signal the threads to start connecting to their channels
        start_connecting.await();
        start=System.currentTimeMillis();

        try {
            connected.await();
            stop=System.currentTimeMillis();
            System.out.println("-- took " + (stop-start) + " msecs for all " + NUM + " threads to connect");

            // coordinator attempts to get complete view within 50 (5*10) seconds 
            // otherwise, exits gracefully
            int num_members=-1;
            for(int i=0; i < 10; i++) {
                View v=channel.getView();
                num_members=v.getMembers().size();
                System.out.println("*--* number of members connected: " + num_members + ", (expected: " +(NUM+1) +
                        "), v=" + v);
                if(num_members == NUM+1)
                    break;
                Util.sleep(5*1000);
            }
            assert num_members == (NUM + 1) : "coordinator unable to obtain complete view";
            
            received_all_views.await();
            stop=System.currentTimeMillis();
            System.out.println("-- took " + (stop-start) + " msecs for all " + NUM + " threads to see all views");
        }
        catch(Exception ex) {
            assert false : ex.toString();
        }
        
        // test split to avoid dependency and resulting timeout
        // testConcurrentJoins ended here; testConcurrentLeaves started here
        
        start_disconnecting.await();
        // long start, stop;
        start=System.currentTimeMillis();

        disconnected.await();
        stop=System.currentTimeMillis();
        System.out.println("-- took " + (stop-start) + " msecs for " + NUM + " threads to disconnect");

        int num_members=0;
        for(int i=0; i < 10; i++) {
            View v=channel.getView();
            Vector mbrs=v != null? v.getMembers() : null;
            if(mbrs != null) {
                num_members=mbrs.size();
                System.out.println("*--* number of members connected: " + num_members + ", (expected: 1), view=" + v);
                if(num_members <= 1)
                    break;
            }
            Util.sleep(3000);
        }
        assert num_members == 1 : "view should have size == 1 after disconnect ";
        log("closing all channels");
        for(int i=0; i < threads.length; i++) {
            MyThread t=threads[i];
            t.closeChannel();
        }
        channel.close();
    }




    public class MyThread extends Thread {
        int                 index=-1;
        long                total_connect_time=0, total_disconnect_time=0;
        private JChannel    ch=null;
        private Address     my_addr=null;

        public MyThread(int i) {
            super("thread #" + i);
            index=i;
        }

        public void closeChannel() {
            if(ch != null) {
                ch.close();
            }
        }


        public void run() {
            View view;

            try {
                ch=createChannelWithProps(PROPS);
                // changeProperties(ch);
                ch.setOpt(Channel.AUTO_RECONNECT, true);

                start_connecting.await();

                long start=System.currentTimeMillis(), stop;
                ch.connect(groupname);
                stop=System.currentTimeMillis();
                total_connect_time=stop-start;
                view=ch.getView();
                my_addr=ch.getLocalAddress();
                log(my_addr + " connected in " + total_connect_time + " msecs (" +
                    view.getMembers().size() + " members). VID=" + view.getVid());

                connected.await();

                int num_members=0;
                while(true) {
                    View v=ch.getView();
                    Vector mbrs=v != null? v.getMembers() : null;
                    if(mbrs == null) {
                        System.err.println("mbrs is null, v=" + v);
                    }
                    else {
                        num_members=mbrs.size();
                        log("num_members=" + num_members);
                        if(num_members == NUM+1) // all threads (NUM) plus the first channel (1)
                            break;
                    }
                    Util.sleep(2000);
                }
                log("reached " + num_members + " members");
                received_all_views.await();

                start_disconnecting.await();
                start=System.currentTimeMillis();
                ch.disconnect();
                stop=System.currentTimeMillis();

                log(my_addr + " disconnected in " + (stop-start) + " msecs");
                disconnected.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }


    private static void changeProperties(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol("GMS");
        if(gms != null) {
            gms.setViewBundling(true);
            gms.setMaxBundlingTime(300);
        }
        MERGE2 merge=(MERGE2)stack.findProtocol("MERGE2");
        if(merge != null) {
            merge.setMinInterval(5000);
            merge.setMaxInterval(10000);
        }
    }

  


}
