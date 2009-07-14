package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.stack.GossipRouter;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.TUNNEL;
import org.jgroups.protocols.PING;
import org.jgroups.util.Util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 * @version $Id: GossipRouterTest.java,v 1.1.2.6 2009/07/14 21:37:26 rachmatowicz Exp $
 */
public class GossipRouterTest extends TestCase {
    String props = null;
    GossipRouter router;
    String router_host = null ;
    int router_port = 0 ;
    JChannel c1, c2;

    protected void setUp() throws Exception {
        super.setUp();
        
    	router_host = System.getProperty("jgroups.tunnel.router_host", "127.0.0.1") ;
        router_port = Integer.parseInt(System.getProperty("jgroups.tunnel.router_port", "12001")) ;
        
        props = getTUNNELProps(router_host, router_port) ;
    }

    protected void tearDown() throws Exception {
        if(router != null) {
        	stopRouter() ;
        }
        if(c2 != null) {
            c2.close();
        }
        if(c1 != null) {
            c1.close();
        }
        super.tearDown();
    }

    private String getTUNNELProps(String routerHost, int routerPort) {
        return
	    "TUNNEL(router_port=" + router_port + ";router_host=" + router_host + ";loopback=true):" +
            "PING(timeout=2000;num_initial_members=2;gossip_refresh=10000;gossip_host=" + router_host +";gossip_port=" + router_port + ";num_ping_requests=1):" +
            "MERGE2(min_interval=5000;max_interval=20000):" +
            "FD(timeout=2000;max_tries=3;shun=true):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(use_mcast_xmit=false;gc_lag=0;retransmit_timeout=300,600,1200,2400,4800;discard_delivered_msgs=true):" +
            "UNICAST(timeout=300,600,1200,2400,3600):" +
            "pbcast.STABLE(stability_delay=1000;desired_avg_gossip=5000;max_bytes=400000):" +
            "pbcast.GMS(join_timeout=3000;join_retry_timeout=2000;print_local_addr=true;shun=false;view_bundling=true;view_ack_collection_timeout=5000):" +
            "FC(max_credits=2000000;min_threshold=0.10):" +
            "FRAG2(frag_size=60000):" +
            "pbcast.STATE_TRANSFER:" +
            "pbcast.FLUSH(timeout=0)" ;
    }
    
    /**
     * Tests the following scenario (http://jira.jboss.com/jira/browse/JGRP-682):
     * - First node is started with tunnel.xml, cannot connect
     * - Second node is started *with* GossipRouter
     * - Now first node should be able to connect and first and second node should be able to merge into a group
     * - SUCCESS: a view of 2
     */
    public void testLateStart() throws Exception {
        final Lock lock=new ReentrantLock();
        final Condition cond=lock.newCondition();
        AtomicBoolean done=new AtomicBoolean(false);

        System.out.println("-- starting first channel");
        c1=new JChannel(props);
        changeMergeInterval(c1);
        setReconnectInterval(c1);
        setRefreshInterval(c1);
        c1.setReceiver(new MyReceiver("c1", done, lock, cond));
        c1.connect("demo");

        System.out.println("-- starting second channel");
        c2=new JChannel(props);
        changeMergeInterval(c2);
        setReconnectInterval(c2);
        setRefreshInterval(c2);
        c2.setReceiver(new MyReceiver("c2", done, lock, cond));
        c2.connect("demo");

        System.out.println("-- starting GossipRouter");
        startRouter(router_host, router_port) ;

        System.out.println("-- waiting for merge to happen --");
        long target_time=System.currentTimeMillis() + 40000;
        lock.lock();
        try {
            while(System.currentTimeMillis() < target_time && done.get() == false) {
                cond.await(1000, TimeUnit.MILLISECONDS);
            }
        }
        finally {
            lock.unlock();
        }

        Util.sleep(500);
        View view=c1.getView();
        System.out.println("view=" + view);
        assertEquals(2, view.size());

        c2.close();
        c1.close();
    }

    private static void changeMergeInterval(JChannel c1) {
        MERGE2 merge=(MERGE2)c1.getProtocolStack().findProtocol(MERGE2.class);
        if(merge != null) {
            merge.setMinInterval(1000);
            merge.setMaxInterval(3000);
        }
    }

    private static void setReconnectInterval(JChannel channel) {
        TUNNEL tunnel=(TUNNEL)channel.getProtocolStack().getTransport();
        if(tunnel != null) {
            tunnel.setReconnectInterval(2000);
        }
    }

    private static void setRefreshInterval(JChannel channel) {
        PING ping=(PING)channel.getProtocolStack().findProtocol(PING.class);
        if(ping != null) {
            ping.setGossipRefresh(1000);
        }
    }

    private void startRouter(String router_host, int router_port) throws Exception {
        router=new GossipRouter(router_port, router_host);
        router.start();
    }

    private void stopRouter() {
        router.stop();
        router = null ;
    }

    private static class MyReceiver extends ReceiverAdapter {
        private final String name;
        private final Lock lock;
        private final AtomicBoolean done;
        private final Condition cond;

        public MyReceiver(String name, AtomicBoolean done, Lock lock, Condition cond) {
            this.name=name;
            this.done=done;
            this.lock=lock;
            this.cond=cond;
        }

        public void viewAccepted(View new_view) {
            if(new_view.size() == 2) {
                System.out.println("[" + name + "]: view=" + new_view);
                lock.lock();
                try {
                    done.set(true);
                    cond.signalAll();
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }
}
