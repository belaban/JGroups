package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Bela Ban
 */
@Test(groups={Global.STACK_INDEPENDENT,Global.GOSSIP_ROUTER},sequential=true)
public class GossipRouterTest {
    GossipRouter                router;
    JChannel                    a, b;
    String                      bind_addr=null;
    private int                 gossip_router_port;
    private String              gossip_router_hosts;

    @BeforeClass
    protected void setUp() throws Exception {
        bind_addr=Util.getProperty(Global.BIND_ADDR);
        if(bind_addr == null) {
            StackType type=Util.getIpStackType();
            if(type == StackType.IPv6)
                bind_addr="::1";
            else
                bind_addr="127.0.0.1";
        }

        gossip_router_port=ResourceManager.getNextTcpPort(InetAddress.getByName(bind_addr));
        gossip_router_hosts=bind_addr + "[" + gossip_router_port + "]";
    }


    @AfterMethod (alwaysRun=true)
    protected void tearDown() throws Exception {
        if(router != null) {
            router.stop();
            router=null;
        }
        Util.close(b,a);
    }

    /**
     * Tests the following scenario (http://jira.jboss.com/jira/browse/JGRP-682):
     * - First node is started with tunnel.xml, cannot connect
     * - Second node is started *with* GossipRouter
     * - Now first node should be able to connect and first and second node should be able to merge into a group
     * - SUCCESS: a view of 2
     */
    @Test
    public void testLateStart() throws Exception {
        final Lock lock=new ReentrantLock();
        final Condition cond=lock.newCondition();
        AtomicBoolean done=new AtomicBoolean(false);

        System.out.println("-- starting first channel");
        a=createTunnelChannel("A");
        a.setReceiver(new MyReceiver("c1", done, lock, cond));
        a.connect("demo");

        System.out.println("-- starting second channel");
        b=createTunnelChannel("B");
        b.setReceiver(new MyReceiver("c2", done, lock, cond));
        b.connect("demo");

        System.out.println("-- starting GossipRouter");
        router=new GossipRouter(gossip_router_port, bind_addr);
        router.start();

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
        View view=a.getView();
        System.out.println("view=" + view);
        assert view.size() == 2 : "view=" + view;
        Util.close(b,a);
    }

    protected JChannel createTunnelChannel(String name) throws Exception {
        TUNNEL tunnel=(TUNNEL)new TUNNEL().setValue("enable_bundling",false);
        tunnel.setGossipRouterHosts(gossip_router_hosts);
        JChannel ch=Util.createChannel(tunnel,
                                       new PING(),
                                       new MERGE2().setValue("min_interval", 1000).setValue("max_interval", 3000),
                                       new FD().setValue("timeout", 2000).setValue("max_tries", 2),
                                       new VERIFY_SUSPECT(),
                                       new NAKACK2().setValue("use_mcast_xmit", false),
                                       new UNICAST(), new STABLE(), new GMS());
        if(name != null)
            ch.setName(name);
        return ch;
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
