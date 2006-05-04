
package org.jgroups.tests;


import EDU.oswego.cs.dl.util.concurrent.CyclicBarrier;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;


/**
 * Tests concurrent leaves of all members of a channel
 * @author Bela Ban
 * @version $Id: DisconnectStressTest.java,v 1.2 2006/05/04 12:28:45 belaban Exp $
 */
public class DisconnectStressTest extends TestCase {
    static CyclicBarrier    all_disconnected=null;
    static CyclicBarrier    start_disconnecting=null;
    static final int        NUM=30;
    static final long       TIMEOUT=50000;
    static final MyThread[] threads=new MyThread[NUM];
    static String           groupname="ConcurrentTestDemo";


    static String props="UDP(mcast_addr=228.8.8.9;mcast_port=7788;ip_ttl=1;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=3000;num_initial_members=3):" +
            "MERGE2(min_interval=3000;max_interval=5000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=300,600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=5000):" +
            "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=false;print_local_addr=false;view_ack_collection_timeout=5000;" +
            "digest_timeout=0;merge_timeout=30000;handle_concurrent_startup=true)";



    public DisconnectStressTest(String name) {
        super(name);
    }


    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }


    public void testConcurrentStartupAndMerging() throws Exception {
        all_disconnected=new CyclicBarrier(NUM+1);
        start_disconnecting=new CyclicBarrier(NUM+1);

        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(i);
            synchronized(threads[i]) {
                threads[i].start();
                threads[i].wait(20000);
            }
        }

        log("DISCONNECTING");
        start_disconnecting.barrier(); // causes all channels to disconnect

        all_disconnected.barrier();  // notification when all threads have disconnected
    }





    public static class MyThread extends Thread {
        int                index=-1;
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

        public int numMembers() {
            return ch.getView().size();
        }

        public void run() {
            View view;

            try {
                ch=new JChannel(props);
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

                start_disconnecting.barrier();

                start=System.currentTimeMillis();
                ch.disconnect();
                stop=System.currentTimeMillis();

                log(my_addr + " disconnected in " + (stop-start) + " msecs");
                all_disconnected.barrier();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }


    }


    public static Test suite() {
        return new TestSuite(DisconnectStressTest.class);
    }

    public static void main(String[] args) {
        String[] testCaseName={DisconnectStressTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


}
