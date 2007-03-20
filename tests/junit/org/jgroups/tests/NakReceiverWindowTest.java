// $Id: NakReceiverWindowTest.java,v 1.10 2007/03/20 09:16:46 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.Address;
import org.jgroups.stack.NakReceiverWindow;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Retransmitter;


public class NakReceiverWindowTest extends TestCase {

    private Address sender;
    private MyRetransmitCommand cmd=new MyRetransmitCommand();

    public NakReceiverWindowTest(String name) {
        super(name);
    }


    protected void setUp() throws Exception {
        super.setUp();
        sender=new IpAddress("127.0.0.1", 5555);
    }

    public void test1() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        assertEquals(0, win.getLowestSeen());
        assertEquals(0, win.getHighestSeen());
        assertNull(win.get(23));
    }

    public void test2() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100);
        assertEquals(0, win.getLowestSeen());
        assertEquals(0, win.getHighestSeen());
    }

    public void test3() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        assertNotNull(win.get(1));
        assertEquals(1, win.getLowestSeen());
        assertEquals(1, win.getHighestSeen());
        win.add(2, new Message());
        assertEquals(1, win.getLowestSeen());
        assertEquals(2, win.getHighestSeen());
        assertNotNull(win.get(2));
    }

    public void test4() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(2, new Message());
        assertEquals(0, win.getLowestSeen());
        assertEquals(0, win.getHighestSeen());
    }

    public void test5() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100);
        win.add(101, new Message());
        win.add(100, new Message());
        assertEquals(100, win.getLowestSeen());
        assertEquals(101, win.getHighestSeen());
    }

    public void test6() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100);
        win.add(101, new Message());
        win.add(100, new Message());
        while((win.remove()) != null) ;
        assertNotNull(win.get(100));
        assertNotNull(win.get(101));
        assertEquals(100, win.getLowestSeen());
        assertEquals(101, win.getHighestSeen());
    }

    public void test7() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        win.stable(4);
        assertEquals(true, win.getLowestSeen() == 1);
        assertEquals(4, win.getHighestSeen());
        assertNotNull(win.get(2));
    }


    public void test8() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        win.add(6, new Message());

        //System.out.println("highest received=" + win.getHighestReceived() +
        //	   "\nhighest_seen=" + win.getHighestSeen() +
        //	   "\nhighest_delivered=" + win.getHighestDelivered());
        assertEquals(4, win.getHighestSeen());
    }


    public void testAdd() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        assertEquals(0, win.getHighestSeen());
        win.add(0, new Message());
        assertEquals(0, win.getHighestSeen());
        win.add(1, new Message());
        assertEquals(1, win.getHighestSeen());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        assertEquals(4, win.getHighestSeen());
        win.add(6, new Message());
        assertEquals(4, win.getHighestSeen());
        win.add(5, new Message());
        assertEquals(6, win.getHighestSeen());
        while(win.remove() != null) ;
        assertEquals(6, win.getHighestSeen());
    }


    public void test9() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        win.add(6, new Message());
        while((win.remove()) != null) ;
        win.stable(6);
        assertNull(win.get(2));

        //System.out.println(win);
        //System.out.println("highest received=" + win.getHighestReceived() +
        //   "\nhighest_seen=" + win.getHighestSeen() +
        //   "\nhighest_delivered=" + win.getHighestDelivered());
        assertEquals(4, win.getLowestSeen());
        assertEquals(4, win.getHighestSeen());
    }


    public void testHighestSeen() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        assertEquals(4, win.getHighestSeen());
        win.add(10, new Message());
        assertEquals(4, win.getHighestSeen());
        assertEquals(10, win.getHighestReceived());
        System.out.println("win: " + win);
        win.add(9, new Message());
        win.add(7, new Message());
        win.add(8, new Message());
        win.add(6, new Message());
        win.add(5, new Message());
        System.out.println("win: " + win);
        while((win.remove()) != null) ;
        assertEquals(win.getHighestSeen(), 10);
    }


    public void testMissingMessages() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(5, new Message());
        assertEquals(1, win.getHighestSeen());
        win.add(6, new Message());
        assertEquals(1, win.getHighestSeen());
        System.out.println("win: " + win);
    }


    public void testMissingMessages2() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(5, new Message());
        assertEquals(1, win.getHighestSeen());
        win.add(8, new Message());
        assertEquals(1, win.getHighestSeen());
        win.add(9, new Message());
        assertEquals(1, win.getHighestSeen());
        System.out.println("win: " + win);
    }


    public void testMissingMessages3() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(5, new Message());
        assertEquals(1, win.getHighestSeen());
        win.add(8, new Message());
        assertEquals(1, win.getHighestSeen());
        win.add(9, new Message());
        assertEquals(1, win.getHighestSeen());
        System.out.println("win: " + win);
        win.add(2, new Message());
        assertEquals(2, win.getHighestSeen());
        win.add(3, new Message());
        win.add(4, new Message());
        assertEquals(5, win.getHighestSeen());
        win.add(7, new Message());
        assertEquals(5, win.getHighestSeen());
        win.add(6, new Message());
        assertEquals(9, win.getHighestSeen());
        win.add(10, new Message());
        assertEquals(10, win.getHighestSeen());
        win.add(11, new Message());
        assertEquals(11, win.getHighestSeen());
        System.out.println("win: " + win);
    }


    public void testMissingMessages4() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 101);
        win.add(101, new Message());
        win.add(105, new Message());
        assertEquals(101, win.getHighestSeen());
        win.add(108, new Message());
        assertEquals(101, win.getHighestSeen());
        win.add(109, new Message());
        assertEquals(101, win.getHighestSeen());
        System.out.println("win: " + win);
        win.add(102, new Message());
        assertEquals(102, win.getHighestSeen());
        win.add(103, new Message());
        win.add(104, new Message());
        assertEquals(105, win.getHighestSeen());
        win.add(107, new Message());
        assertEquals(105, win.getHighestSeen());
        win.add(106, new Message());
        assertEquals(109, win.getHighestSeen());
        win.add(110, new Message());
        assertEquals(110, win.getHighestSeen());
        win.add(110, new Message());
        assertEquals(110, win.getHighestSeen());
        System.out.println("win: " + win);
    }


    public void testMissingMessages5() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 101);
        win.add(101, new Message());
        assertEquals(101, win.getHighestSeen());
        win.add(108, new Message());
        assertEquals(101, win.getHighestSeen());
        win.add(109, new Message());
        assertEquals(101, win.getHighestSeen());
        System.out.println("win: " + win);
        win.add(102, new Message());
        assertEquals(102, win.getHighestSeen());
        win.add(103, new Message());
        win.add(104, new Message());
        assertEquals(104, win.getHighestSeen());
        win.add(107, new Message());
        assertEquals(104, win.getHighestSeen());
        win.add(106, new Message());
        win.add(105, new Message());
        assertEquals(109, win.getHighestSeen());
        win.add(110, new Message());
        assertEquals(110, win.getHighestSeen());
        win.add(110, new Message());
        assertEquals(110, win.getHighestSeen());
        System.out.println("win: " + win);
    }

    public void test10() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        assertEquals(1, win.getLowestSeen());
        assertEquals(4, win.getHighestSeen());
    }

    public void test10a() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        win.stable(4);
        assertEquals(4, win.getLowestSeen());
        assertEquals(4, win.getHighestSeen());

    }

    public void test11() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        win.reset();
        assertEquals(true, win.getLowestSeen() == 0);
        assertEquals(0, win.getHighestSeen());
    }


    public void test12() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);

        win.add(1, new Message(null, null, new Integer(1)));
        win.add(2, new Message(null, null, new Integer(2)));
        win.add(3, new Message(null, null, new Integer(3)));

        assertEquals(true, ((Integer)win.remove().getObject()).intValue() == 1);
        assertEquals(2, ((Integer)win.remove().getObject()).intValue());
        assertEquals(3, ((Integer)win.remove().getObject()).intValue());
    }


    public void test13() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        assertEquals(1, win.getLowestSeen());
        assertEquals(4, win.getHighestSeen());

        win.remove();
        win.remove();
        win.add(5, new Message());
        win.add(6, new Message());
        assertEquals(1, win.getLowestSeen());
        assertEquals(6, win.getHighestSeen());
        win.stable(2);
        assertEquals(2, win.getLowestSeen());
    }



    public void testAddOOBAtHead() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        boolean rc;
        rc=win.add(0, oob());
        assertFalse(rc);
        rc=win.add(1, oob());
        assertTrue(rc);
        rc=win.add(1, oob());
        assertFalse(rc);
    }


    public void testAddOOBAtTail() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        boolean rc;
        rc=win.add(1, oob());
        assertTrue(rc);
        rc=win.add(2, oob());
        assertTrue(rc);
        rc=win.add(2, oob());
        assertFalse(rc);
    }


    public void testAddOOBInTheMiddle() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        boolean rc;
        rc=win.add(3, oob());
        assertTrue(rc);
        rc=win.add(3, oob());
        assertFalse(rc);
        rc=win.add(1, oob());
        assertTrue(rc);
        rc=win.add(1, oob());
        assertFalse(rc);
        rc=win.add(2, oob());
        assertTrue(rc);
        rc=win.add(2, oob());
        assertFalse(rc);
    }


    private Message oob() {
        Message retval=new Message();
        retval.setFlag(Message.OOB);
        return retval;
    }


    public void testUpdateHighestSeen() {
        add(1000);
        add(2000);
        add(3000);
        add(4000);
        add(5000);
        add(10000);
        add(15000);
        add(20000);
        add(30000);
    }

    public void test1000() {
        add(1000);
    }

    public void test10000() {
        add(10000);
    }


    void add(int num_msgs) {
        long start, stop;
        double time_per_msg;
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        start=System.currentTimeMillis();
        for(int i=1; i < 1 + num_msgs; i++) {
            win.add(i, new Message());
        }
        stop=System.currentTimeMillis();
        time_per_msg=(stop-start) / (double)num_msgs;
        System.out.println("-- time for " + num_msgs + " msgs: " + (stop-start) + ", " + time_per_msg + " ms/msg");
    }


    private static class MyRetransmitCommand implements Retransmitter.RetransmitCommand {

        public void retransmit(long first_seqno, long last_seqno, Address sender) {
        }
    }


    public static Test suite() {
        TestSuite s=new TestSuite(NakReceiverWindowTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
