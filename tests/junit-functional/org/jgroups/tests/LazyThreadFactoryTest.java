package org.jgroups.tests;

import org.jgroups.util.LazyThreadFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  4.0.16
 */
@Test
public class LazyThreadFactoryTest {
    protected LazyThreadFactory factory=new LazyThreadFactory("thread", false, true);
    protected static final int  NUM=100;

    /** Tests that threads who are in TERMINATED state have been removed from the factory's queue */
    public void testRemovalOfTerminatedThreads() throws Exception {
        factory.setPattern("cl");
        factory.setClusterName("my-cluster");
        factory.setAddress("localhost-12345");

        for(int i=1; i <= NUM; i++) {
            Thread thread=factory.newThread(new MyThread(), "thread-" + i);
            thread.start();
        }

        Util.sleep(1000);
        factory.removeTerminatedThreads();
        assert factory.size() == 0 : "found threads that are still alive: " + factory.dumpThreads();
    }

    /** Tests that threads whose names have been replaced (no <ADDR> or <CLUSTER>) have been removed from the factory's queue */
    public void testRemovalOfCompleteThreads() throws Exception {
        factory.setPattern("cl");

        for(int i=1; i <= NUM; i++) {
            Thread thread=factory.newThread(new MyThread(), "thread-" + i);
            thread.start();
        }

        factory.setClusterName("my-cluster");
        assert factory.size() == NUM;
        factory.setAddress("localhost-12345");
        assert factory.size() == 0 : "found threads in the queue: " + factory.dumpThreads();
    }

    protected static class MyThread extends Thread {
        @Override
        public void run() {
            Util.sleep(500);
        }
    }
}
