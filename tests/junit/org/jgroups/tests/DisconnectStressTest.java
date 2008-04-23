
package org.jgroups.tests;



import org.testng.annotations.*;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.Protocol;

import java.util.concurrent.CyclicBarrier;
import java.util.Properties;


/**
 * Tests concurrent leaves of all members of a channel
 * @author Bela Ban
 * @version $Id: DisconnectStressTest.java,v 1.10 2008/04/23 14:53:47 belaban Exp $
 */
@Test(groups={"temp", "single"})
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

        String props=null;
        for(int i=0; i < threads.length; i++) {
            JChannel ch;
            if(props == null) {
                ch=createChannel(true);
                props=ch.getProperties();
            }
            else {
                ch=createChannelWithProps(props);
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
        Properties props=new Properties();
        Protocol prot=stack.findProtocol(MERGE2.class);
        if(prot != null) {
            props.clear();
            props.setProperty("min_interval", "3000");
            props.setProperty("max_interval", "5000");
            prot.setProperties(props);
        }
        prot=stack.findProtocol(STABLE.class);
        props.clear();
        props.setProperty("desired_avg_gossip", "5000");
        prot.setProperties(props);
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
