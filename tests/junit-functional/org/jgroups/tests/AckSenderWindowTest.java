package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.stack.AckSenderWindow;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.Util;
import org.jgroups.util.TimeScheduler;

import java.util.HashMap;


/**
 * Test cases for AckSenderWindow
 * @author Bela Ban
 * @version $Id: AckSenderWindowTest.java,v 1.2.2.5 2009/09/14 15:43:06 belaban Exp $
 */
public class AckSenderWindowTest extends TestCase {
    AckSenderWindow win=null;
    long[] xmit_timeouts={1000, 2000, 4000, 8000};
    protected TimeScheduler timer=null;


    protected void setUp() throws Exception {
        super.setUp();
        timer=new TimeScheduler(10);
    }

    protected void tearDown() throws Exception {
        timer.stop();
        super.tearDown();
    }


    public void testLowest() {
        win=new AckSenderWindow(null, new StaticInterval(xmit_timeouts), timer);
        for(long i=1; i < 5; i++)
            win.add(i, new Message());
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assertEquals(0, win.getLowest());

        win.ack(3);
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assertEquals(4, win.getLowest());

        win.ack(4);
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assertEquals(5, win.getLowest());

        win.ack(2);
        assertEquals(5, win.getLowest());
    }



    public static Test suite() {
        TestSuite suite;
        suite=new TestSuite(AckSenderWindowTest.class);
        return (suite);
    }


}