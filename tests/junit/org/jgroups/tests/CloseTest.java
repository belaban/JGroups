// $Id: CloseTest.java,v 1.4 2005/09/20 13:58:12 belaban Exp $

package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.ChannelClosedException;
import org.jgroups.JChannel;
import org.jgroups.util.Util;


/**
 * Demos the creation of a channel and subsequent connection and closing. Demo application should exit (no
 * more threads running)
 */
public class CloseTest extends TestCase {
    JChannel channel, channel1, channel2;


    public CloseTest(String name) {
        super(name);
    }


    public void testCreationAndClose() throws Exception {
        System.out.println("-- creating channel1 --");
        channel1=new JChannel();
        System.out.println("-- connecting channel1 --");
        channel1.connect("CloseTest1");
        System.out.println("-- closing channel1 --");
        channel1.close();
        //Util.sleep(2000);
        System.out.println("-- done, threads are ");
        Util.printThreads();
    }


    public void testCreationAndClose2() throws Exception {
        System.out.println("-- creating channel2 --");
        channel2=new JChannel();
        System.out.println("-- connecting channel2 --");
        channel2.connect("CloseTest2");
        System.out.println("-- closing channel --");
        channel2.close();
        Util.sleep(2000);
        //System.out.println("-- done, threads are ");
        Util.printThreads();
    }


    public void testChannelClosedException() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel();
        System.out.println("-- connecting channel --");
        channel.connect("CloseTestLoop");
        System.out.println("-- closing channel --");
        channel.close();
        Util.sleep(2000);

        try {
            channel.connect("newGroup");
            fail(); // cannot connect to a closed channel
        }
        catch(ChannelClosedException ex) {
            assertTrue(true);
        }
    }

    public void testCreationAndCloseLoop() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel();

        for(int i=1; i <= 10; i++) {
            System.out.println("-- connecting channel (attempt #" + i + " ) --");
            channel.connect("CloseTestLoop2");
            System.out.println("-- closing channel --");
            channel.close();

            System.out.println("-- reopening channel --");
            channel.open();
        }
        channel.close();
    }

    public void testShutdown() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel();
        System.out.println("-- connecting channel --");
        channel.connect("bla");
        System.out.println("-- shutting down channel --");
        channel.shutdown();

        Thread threads[]=new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        System.out.println("-- active threads:");
        for(int i=0; i < threads.length; i++)
            System.out.println(threads[i]);
        assertTrue(threads.length < 5);
    }

    public static void main(String[] args) {
        String[] testCaseName={CloseTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
