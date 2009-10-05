package org.jgroups.tests;

import org.jgroups.stack.GossipRouter;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Global;
import org.jgroups.protocols.MERGE2;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Bela Ban
 * @version $Id: GossipRouterTest.java,v 1.14 2009/10/05 19:35:44 vlada Exp $
 */
@Test(groups={Global.STACK_INDEPENDENT,Global.GOSSIP_ROUTER},sequential=true)
public class GossipRouterTest {
    final static String PROPS="tunnel.xml";
    GossipRouter router;
    JChannel c1, c2;


    @AfterMethod (alwaysRun=true)
    protected void tearDown() throws Exception {
        if(router != null) {
            router.stop();
            router=null;
        }
        Util.close(c2, c1);
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
        c1=new JChannel(PROPS);
        changeMergeInterval(c1);
        c1.setReceiver(new MyReceiver("c1", done, lock, cond));
        c1.connect("demo");

        System.out.println("-- starting second channel");
        c2=new JChannel(PROPS);
        changeMergeInterval(c2);
        c2.setReceiver(new MyReceiver("c2", done, lock, cond));
        c2.connect("demo");

        System.out.println("-- starting GossipRouter");
        router=new GossipRouter(12001, "127.0.0.1");
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
        View view=c1.getView();
        System.out.println("view=" + view);
        Assert.assertEquals(2, view.size());

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
