// $Id: NakReceiverWindowTest.java,v 1.3 2004/04/21 23:08:16 belaban Exp $

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

    public void setUp() {
    }

    public void tearDown() {
    }


    public void test1() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        assertTrue(win.getLowestSeen() == 0);
        assertTrue(win.getHighestSeen() == 0);
    }

    public void test2() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 100);
        assertTrue(win.getLowestSeen() == 0);
        assertTrue(win.getHighestSeen() == 0);
    }

    public void test3() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        assertTrue(win.getLowestSeen() == 1);
        assertTrue(win.getHighestSeen() == 1);
        win.add(2, new Message());
        assertTrue(win.getLowestSeen() == 1);
        assertTrue(win.getHighestSeen() == 2);
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
        assertTrue(win.getLowestSeen() == 100);
        assertTrue(win.getHighestSeen() == 101);
    }

    public void test6() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 100);
        win.add(101, new Message());
        win.add(100, new Message());
        while((win.remove()) != null) ;
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


    public void test9() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(null, 1);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        win.add(6, new Message());
        while((win.remove()) != null) ;
        win.stable(6);

        //System.out.println(win);
        //System.out.println("highest received=" + win.getHighestReceived() +
        //   "\nhighest_seen=" + win.getHighestSeen() +
        //   "\nhighest_delivered=" + win.getHighestDelivered());
        assertTrue(win.getLowestSeen() == 4);
        assertTrue(win.getHighestSeen() == 4);
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





    /*
    public void test12() {
	NakReceiverWindow win=new NakReceiverWindow(null, 22);
	
	System.out.println("win.getHighestReceived()=" + win.getHighestReceived() +
			   "\nwin.getHighestDelivered()=" + win.getHighestDelivered() +
			   "\nwin.getHighestSeen()=" + win.getHighestSeen());
    }
    */



    public static Test suite() {
        TestSuite s=new TestSuite(NakReceiverWindowTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
