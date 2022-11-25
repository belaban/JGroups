package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests that the thread pool increases dynamically based on view changes (https://issues.redhat.com/browse/JGRP-2655)
 * @author Bela Ban
 * @since  5.2.10
 */
@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
public class ThreadPoolIncreaseTest {
    protected JChannel[]       channels;
    protected static final int NUM=20;

    @BeforeMethod
    protected void setup() throws Exception {
        channels=new JChannel[NUM];
        for(int i=0; i < NUM; i++)
            channels[i]=createChannel(String.valueOf(i));
    }

    @AfterMethod
    protected void destroy() {
        Util.close(channels);
    }


    public void testDynamicIncrease() throws Exception {
        for(JChannel ch: channels)
            ch.connect(ThreadPoolIncreaseTest.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        for(JChannel ch: channels) {
            int max_threads=ch.getProtocolStack().getTransport().getThreadPool().getMaxThreads();
            System.out.printf("%s: thread_pool.max_size=%d\n", ch.getAddress(), max_threads);
            assert max_threads >= NUM;
        }
    }


    protected static JChannel createChannel(String name) throws Exception {
        JChannel ch=new JChannel(Util.getTestStack()).name(name);
        ch.getProtocolStack().getTransport().getThreadPool().setMaxThreads(10);
        return ch;
    }
}
