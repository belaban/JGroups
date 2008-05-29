
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;


/**
 * Tests concurrent leaves of all members of a channel
 * @author Bela Ban
 * @version $Id: DisconnectStressTest.java,v 1.13 2008/05/29 11:38:53 belaban Exp $
 */
@Test(groups="temp")
public class DisconnectStressTest extends ChannelTestBase {
    CyclicBarrier           all_disconnected=null;
    CyclicBarrier           start_disconnecting=null;
    static final int        NUM=30;
    static final long       TIMEOUT=50000;
    final MyThread[]        threads=new MyThread[NUM];
    static String           groupname="DisconnectStressTest";



    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }


    @Test
    public void testConcurrentStartupAndMerging() throws Exception {
        all_disconnected=new CyclicBarrier(NUM+1);
        start_disconnecting=new CyclicBarrier(NUM+1);

        JChannel first=null;
        for(int i=0; i < threads.length; i++) {
            JChannel ch;
            if(first == null) {
                ch=createChannel(true);
                first=ch;
            }
            else {
                ch=createChannel(first);
            }
            modifyStack(ch);
            threads[i]=new MyThread(i, ch);

            synchronized(threads[i]) {
                threads[i].start();
                threads[i].wait(20000);
            }
        }

        log("DISCONNECTING");
        start_disconnecting.await(); // causes all channels to disconnect
        all_disconnected.await();  // notification when all threads have disconnected
    }



    private static void modifyStack(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol prot=stack.findProtocol(MERGE2.class);
        if(prot != null) {
            MERGE2 merge=(MERGE2)prot;
            merge.setMinInterval(3000);
            merge.setMaxInterval(5000);
        }
        prot=stack.findProtocol(STABLE.class);
        if(prot != null) {
            STABLE stable=(STABLE)prot;
            stable.setDesiredAverageGossip(5000);
        }
        NAKACK nak=(NAKACK)stack.findProtocol(NAKACK.class);
        if(nak != null) {
            nak.setLogDiscardMessages(false);
        }

        GMS gms=(GMS)stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setLogCollectMessages(false);
    }


    public class MyThread extends Thread {
        int                 index=-1;
        long                total_connect_time=0, total_disconnect_time=0;
        private JChannel    ch=null;
        private Address     my_addr=null;

        public MyThread(int i, JChannel ch) {
            super("thread #" + i);
            this.ch=ch;
            index=i;
        }

        public void closeChannel() {
            if(ch != null) {
                ch.close();
            }
        }

        public int numMembers() {
            return ch.getView().size();
        }

        public void run() {
            View view;

            try {
                log("connecting to channel");
                long start=System.currentTimeMillis(), stop;
                ch.connect(groupname);
                stop=System.currentTimeMillis();
                synchronized(this) {
                    this.notify();
                }
                total_connect_time=stop-start;
                view=ch.getView();
                my_addr=ch.getLocalAddress();
                log(my_addr + " connected in " + total_connect_time + " msecs (" +
                    view.getMembers().size() + " members). VID=" + ch.getView());

                start_disconnecting.await();

                start=System.currentTimeMillis();
                ch.disconnect();
                stop=System.currentTimeMillis();

                log(my_addr + " disconnected in " + (stop-start) + " msecs");
                all_disconnected.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }




    }




}
