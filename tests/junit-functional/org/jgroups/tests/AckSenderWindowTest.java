package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.AckSenderWindow;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.TimeScheduler;


/**
 * Test cases for AckSenderWindow
 * @author Bela Ban
 * @version $Id: AckSenderWindowTest.java,v 1.2.2.8 2009/09/15 07:15:47 belaban Exp $
 */
public class AckSenderWindowTest extends TestCase {
    private AckSenderWindow win;
    long[] xmit_timeouts={1000, 2000, 4000, 8000};
    protected TimeScheduler timer=null;



    protected void setUp() throws Exception {
        super.setUp();
        timer=new TimeScheduler(10);
        win=new AckSenderWindow(null, new StaticInterval(xmit_timeouts), timer);
    }

    protected void tearDown() throws Exception {
        timer.stop();
        win.reset();
        super.tearDown();
    }


    public void testLowest() {
        for(long i=1; i < 5; i++)
            win.add(i, new Message());
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assertEquals(Global.DEFAULT_FIRST_UNICAST_SEQNO, win.getLowest());

        win.ack(3);
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assertEquals(4, win.getLowest());

        win.ack(4);
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assertEquals(5, win.getLowest());

        win.ack(2);
        assertEquals(5, win.getLowest());
    }

    public void testGetLowestMessage() {
        long[] seqnos={1,2,3,4,5};
        Message[] msgs=new Message[]{new Message(),new Message(),new Message(),new Message(),new Message()};

        for(int i=0; i < seqnos.length; i++) {
            win.add(seqnos[i], msgs[i]);
        }
        System.out.println("win = " + win);

        Message msg=win.getLowestMessage();
        assertSame(msg, msgs[0]);

        win.ack(2);
        msg=win.getLowestMessage();
        assertSame(msg, msgs[2]);

        win.ack(7);
        msg=win.getLowestMessage();
        assertNull(msg);
    }


    public void testAdd() {
        for(int i=1; i <= 10; i++)
            win.add(i, new Message());
        System.out.println("win = " + win);
        assertEquals(10, win.size());
        win.ack(7);
        assertEquals(3, win.size());
    }



    public static Test suite() {
        TestSuite suite;
        suite=new TestSuite(AckSenderWindowTest.class);
        return (suite);
    }


}