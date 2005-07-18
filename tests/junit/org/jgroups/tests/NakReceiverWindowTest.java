// $Id: NakReceiverWindowTest.java,v 1.8 2005/07/18 14:23:35 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.stack.NakReceiverWindow;


public class NakReceiverWindowTest extends TestCase {


    public NakReceiverWindowTest(String name) {
        super(name);
    }


    public void test1() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        assertTrue(win.getLowestSeen() == 0);
        assertTrue(win.getHighestSeen() == 0);
        assertNull(win.get(23));
    }

    public void test2() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 100);
        assertEquals(0, win.getLowestSeen());
        assertEquals(0, win.getHighestSeen());
    }

    public void test3() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        assertNotNull(win.get(1));
        assertTrue(win.getLowestSeen() == 1);
        assertTrue(win.getHighestSeen() == 1);
        win.add(2, new Message());
        assertTrue(win.getLowestSeen() == 1);
        assertTrue(win.getHighestSeen() == 2);
        assertNotNull(win.get(2));
    }

    public void test4() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(2, new Message());
        assertTrue(win.getLowestSeen() == 0);
        assertTrue(win.getHighestSeen() == 0);
    }

    public void test5() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 100);
        win.add(101, new Message());
        win.add(100, new Message());
        assertEquals(100, win.getLowestSeen());
        assertEquals(101, win.getHighestSeen());
    }

    public void test6() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 100);
        win.add(101, new Message());
        win.add(100, new Message());
        while((win.remove()) != null) ;
        assertNotNull(win.get(100));
        assertNotNull(win.get(101));
        assertTrue(win.getLowestSeen() == 100);
        assertTrue(win.getHighestSeen() == 101);
    }

    public void test7() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        win.stable(4);
        assertTrue(win.getLowestSeen() == 1);
        assertTrue(win.getHighestSeen() == 4);
        assertNotNull(win.get(2));
    }


    public void test8() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        win.add(6, new Message());

        //System.out.println("highest received=" + win.getHighestReceived() +
        //	   "\nhighest_seen=" + win.getHighestSeen() +
        //	   "\nhighest_delivered=" + win.getHighestDelivered());
        assertTrue(win.getHighestSeen() == 4);
    }


    public void testAdd() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 0);
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
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
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
        assertTrue(win.getLowestSeen() == 4);
        assertTrue(win.getHighestSeen() == 4);
    }


    public void testHighestSeen() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        assertEquals(win.getHighestSeen(), 4);
        win.add(10, new Message());
        assertEquals(win.getHighestSeen(), 4);
        assertEquals(win.getHighestReceived(), 10);
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
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        win.add(5, new Message());
        assertEquals(1, win.getHighestSeen());
        win.add(6, new Message());
        assertEquals(1, win.getHighestSeen());
        System.out.println("win: " + win);
    }


    public void testMissingMessages2() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
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
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
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
        NakReceiverWindow win=new NakReceiverWindow(null, 101);
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
        NakReceiverWindow win=new NakReceiverWindow(null, 101);
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
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        assertTrue(win.getLowestSeen() == 1);
        assertTrue(win.getHighestSeen() == 4);
    }

    public void test10a() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        win.stable(4);
        assertTrue(win.getLowestSeen() == 4);
        assertTrue(win.getHighestSeen() == 4);

    }

    public void test11() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        win.reset();
        assertTrue(win.getLowestSeen() == 0);
        assertTrue(win.getHighestSeen() == 0);
    }


    public void test12() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);

        win.add(1, new Message(null, null, new Integer(1)));
        win.add(2, new Message(null, null, new Integer(2)));
        win.add(3, new Message(null, null, new Integer(3)));

        assertTrue(((Integer)win.remove().getObject()).intValue() == 1);
        assertTrue(((Integer)win.remove().getObject()).intValue() == 2);
        assertTrue(((Integer)win.remove().getObject()).intValue() == 3);
    }


    public void test13() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
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
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        start=System.currentTimeMillis();
        for(int i=1; i < 1 + num_msgs; i++) {
            win.add(i, new Message());
        }
        stop=System.currentTimeMillis();
        time_per_msg=(stop-start) / (double)num_msgs;
        System.out.println("-- time for " + num_msgs + " msgs: " + (stop-start) + ", " + time_per_msg + " ms/msg");
    }



    public static Test suite() {
        TestSuite s=new TestSuite(NakReceiverWindowTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
