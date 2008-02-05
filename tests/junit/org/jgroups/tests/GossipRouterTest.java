package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.stack.GossipRouter;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.ReceiverAdapter;
import org.jgroups.util.Util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 * @version $Id: GossipRouterTest.java,v 1.1.2.2 2008/02/05 09:58:03 belaban Exp $
 */
public class GossipRouterTest extends TestCase {
    final static String PROPS="tunnel.xml";
    GossipRouter router;
    JChannel c1, c2;

    protected void setUp() throws Exception {
        super.setUp();

    }

    protected void tearDown() throws Exception {
        if(router != null) {
            router.stop();
            router=null;
        }
        if(c2 != null) {
            c2.close();
        }
        if(c1 != null) {
            c1.close();
        }
        super.tearDown();
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
        c1=new JChannel(PROPS);
        c1.setReceiver(new MyReceiver("c1", done, lock, cond));
        c1.connect("demo");

        Util.sleep(5000);
        System.out.println("-- starting GossipRouter");
        router=new GossipRouter(12001, "127.0.0.1");
        router.start();

        System.out.println("-- starting second channel");
        c2=new JChannel(PROPS);
        c2.setReceiver(new MyReceiver("c2", done, lock, cond));
        c2.connect("demo");

        System.out.println("-- waiting for merge to happen --");
        long target_time=System.currentTimeMillis() + 30000;
        lock.lock();
        try {
            while(System.currentTimeMillis() < target_time) {
                cond.await(1000, TimeUnit.MILLISECONDS);
            }
        }
        finally {
            lock.unlock();
        }

        View view=c2.getView();
        System.out.println("view=" + view);
        assertEquals(2, view.size());
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
            System.out.println("[" + name + "]: view=" + new_view);
            if(new_view.size() == 2) {
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
