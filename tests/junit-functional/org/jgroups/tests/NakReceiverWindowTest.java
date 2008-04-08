// $Id: NakReceiverWindowTest.java,v 1.4 2008/04/08 12:39:38 belaban Exp $

package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.NakReceiverWindow;
import org.jgroups.stack.Retransmitter;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;



@Test(groups=Global.FUNCTIONAL)
public class NakReceiverWindowTest {
    private Address sender;
    private MyRetransmitCommand cmd=new MyRetransmitCommand();


    @BeforeClass
    protected void setUp() throws Exception {
        sender=new IpAddress("127.0.0.1", 5555);
    }


    public void test1() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        check(win, 0, 1, 1);
        assert win.get(23) == null;
    }


    public void test2() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100);
        check(win, 0, 100, 100);
    }


    public void test3() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        assert win.get(1) != null;
        check(win, 0, 1, 0);
        win.add(2, new Message());
        check(win, 0, 2, 0);
        assert win.get(2) != null;
        win.remove();
        check(win, 0, 2, 1);
        win.remove();
        check(win, 0, 2, 2);
    }


    public void test4() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1);
        win.add(2, new Message());
        check(win, 0, 2, 1);
    }


    public void test5() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100);
        win.add(101, new Message());
        win.add(100, new Message());
        check(win, 0, 101, 100);
    }


    public void test6() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100);
        win.add(101, new Message());
        System.out.println("win: " + win);
        win.add(100, new Message());
        System.out.println("win: " + win);
        check(win, 0, 101, 100);
        win.remove();
        System.out.println("win: " + win);
        check(win, 0, 101, 101);
        while((win.remove()) != null);
        check(win, 0, 101, 101);
    }



    public void testLowerBounds() {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, 50, null);
        win.add(101, new Message());
        System.out.println("win: " + win);
        win.add(100, new Message());
        System.out.println("win: " + win);
        check(win, 50, 101, 100);
        win.remove();
        System.out.println("win: " + win);
        check(win, 50, 101, 101);
        while((win.remove()) != null);
        check(win, 50, 101, 101);
    }


    public void test7() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        check(win, 0, 4, 0);
        System.out.println("Note that the subsequent warning is expected:");
        win.stable(4); // no-op because we haven't even removed 4 messages
        check(win, 0, 4, 0);
        while(win.remove() != null);
        check(win, 0, 4, 4);
        win.stable(4);
        check(win, 4, 4, 4);
    }



    public void testLowerBounds2() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, 50, null);
        win.add(100, new Message());
        win.add(101, new Message());
        win.add(102, new Message());
        win.add(103, new Message());
        System.out.println("win: " + win);
        check(win, 50, 103, 100);
        System.out.println("Note that the subsequent warning is expected:");
        win.stable(103); // no-op because we haven't even removed 4 messages
        check(win, 50, 103, 100);
        while(win.remove() != null);
        check(win, 50, 103, 103);
        win.stable(103);
        System.out.println("win: " + win);
        check(win, 103, 103, 103);
    }


    public void test8() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        win.add(6, new Message());
        check(win, 0, 6, 0); // haven't delivered a message yet
        while(win.remove() != null);
        check(win, 0, 6, 4);
        win.add(5, new Message());
        check(win, 0, 6, 4);
        win.remove();
        check(win, 0, 6, 5);
        win.remove();
        check(win, 0, 6, 6);
        win.stable(4);
        check(win, 4, 6, 6);
        win.stable(6);
        check(win, 6, 6, 6);
    }



    public void testAdd() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        check(win, 0, 0, 0);
        win.add(0, new Message()); // discarded, next expected is 1
        check(win, 0, 0, 0);
        win.add(1, new Message());
        check(win, 0, 1, 0);
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        check(win, 0, 4, 0);
        win.add(6, new Message());
        check(win, 0, 6, 0);
        win.add(5, new Message());
        check(win, 0, 6, 0);
        while(win.remove() != null) ;
        check(win, 0, 6, 6);
        win.stable(4);
        check(win, 4, 6, 6);
        win.stable(6);
        check(win, 6, 6, 6);
    }



    public void test9() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        win.add(6, new Message());
        System.out.println("win: " + win);
        while((win.remove()) != null) ;
        win.stable(6); // 6 is ignore as it is >= highest delivered message
        System.out.println("win: " + win);
        assert win.get(2) != null;
        check(win, 0, 6, 4);
        win.add(5, new Message());
        check(win, 0, 6, 4);
        while((win.remove()) != null) ;
        check(win, 0, 6, 6);
        win.stable(6);
        check(win, 6, 6, 6);
    }



    public void testHighestDelivered() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        check(win, 0, 4, 0);
        win.add(10, new Message());
        check(win, 0, 10, 0);
        System.out.println("win: " + win);
        win.add(9, new Message());
        win.add(7, new Message());
        win.add(8, new Message());
        win.add(6, new Message());
        win.add(5, new Message());
        System.out.println("win: " + win);
        check(win, 0, 10, 0);
        while((win.remove()) != null) ;
        check(win, 0, 10, 10);
        win.stable(5);
        System.out.println("win: " + win);
        check(win, 5, 10, 10);
        win.stable(10);
        System.out.println("win: " + win);
        check(win, 10, 10, 10);
    }



    public void testMissingMessages() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(5, new Message());
        check(win, 0, 5, 0);
        win.add(6, new Message());
        check(win, 0, 6, 0);
        System.out.println("win: " + win);
    }



    public void testMissingMessages2() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(5, new Message());
        check(win, 0, 5, 0);
        win.add(8, new Message());
        check(win, 0, 8, 0);
        win.add(9, new Message());
        check(win, 0, 9, 0);
        System.out.println("win: " + win);
    }



    public void testMissingMessages3() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(5, new Message());
        check(win, 0, 5, 0);
        win.add(8, new Message());
        check(win, 0, 8, 0);
        win.add(9, new Message());
        check(win, 0, 9, 0);
        System.out.println("win: " + win);
        win.add(2, new Message());
        check(win, 0, 9, 0);
        win.add(3, new Message());
        win.add(4, new Message());
        check(win, 0, 9, 0);
        win.add(7, new Message());
        check(win, 0, 9, 0);
        win.add(6, new Message());
        check(win, 0, 9, 0);
        win.add(10, new Message());
        check(win, 0, 10, 0);
        win.add(11, new Message());
        check(win, 0, 11, 0);
        System.out.println("win: " + win);
    }



    public void testMissingMessages4() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100);
        win.add(101, new Message());
        win.add(105, new Message());
        check(win, 0, 105, 100);
        win.add(108, new Message());
        check(win, 0, 108, 100);
        win.add(109, new Message());
        check(win, 0, 109, 100);
        System.out.println("win: " + win);
        win.add(102, new Message());
        check(win, 0, 109, 100);
        win.add(103, new Message());
        win.add(104, new Message());
        check(win, 0, 109, 100);
        win.add(107, new Message());
        check(win, 0, 109, 100);
        win.add(106, new Message());
        check(win, 0, 109, 100);
        win.add(110, new Message());
        check(win, 0, 110, 100);
        win.add(110, new Message());
        check(win, 0, 110, 100);
        System.out.println("win: " + win);
    }



    public void testMissingMessages5() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100);
        win.add(101, new Message());
        check(win, 0, 101, 100);
        win.add(108, new Message());
        check(win, 0, 108, 100);
        win.remove();
        win.add(109, new Message());
        check(win, 0, 109, 101);
        System.out.println("win: " + win);
        win.add(102, new Message());
        check(win, 0, 109, 101);
        win.add(103, new Message());
        win.add(104, new Message());
        check(win, 0, 109, 101);
        win.add(107, new Message());
        check(win, 0, 109, 101);
        win.add(106, new Message());
        win.add(105, new Message());
        check(win, 0, 109, 101);
        win.add(110, new Message());
        check(win, 0, 110, 101);
        win.add(110, new Message());
        check(win, 0, 110, 101);
        System.out.println("win: " + win);
    }


    public void test10() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        check(win, 0, 4, 4);
    }


    public void test10a() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        win.stable(4);
        check(win, 4, 4, 4);

    }


    public void test11() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        while((win.remove()) != null) ;
        win.reset();
        check(win, 0, 0, 0);
    }



    public void test12() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);

        win.add(1, new Message(null, null, new Integer(1)));
        win.add(2, new Message(null, null, new Integer(2)));
        win.add(3, new Message(null, null, new Integer(3)));

        Assert.assertEquals(1, ((Integer)win.remove().getObject()).intValue());
        Assert.assertEquals(2, ((Integer)win.remove().getObject()).intValue());
        Assert.assertEquals(3, ((Integer)win.remove().getObject()).intValue());
    }



    public void test13() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        win.add(1, new Message());
        win.add(2, new Message());
        win.add(3, new Message());
        win.add(4, new Message());
        check(win, 0, 4, 0);
        win.remove();
        win.remove();
        win.add(5, new Message());
        win.add(6, new Message());
        check(win, 0, 6, 2);
        win.stable(2);
        check(win, 2, 6, 2);
    }




    public void testAddOOBAtHead() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        boolean rc;
        rc=win.add(0, oob());
        assert !(rc);
        rc=win.add(1, oob());
        assert rc;
        rc=win.add(1, oob());
        assert !(rc);
    }



    public void testAddOOBAtTail() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        boolean rc;
        rc=win.add(1, oob());
        assert rc;
        rc=win.add(2, oob());
        assert rc;
        rc=win.add(2, oob());
        assert !(rc);
    }



    public void testAddOOBInTheMiddle() throws Exception {
        NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0);
        boolean rc;
        rc=win.add(3, oob());
        assert rc;
        rc=win.add(3, oob());
        assert !(rc);
        rc=win.add(1, oob());
        assert rc;
        rc=win.add(1, oob());
        assert !(rc);
        rc=win.add(2, oob());
        assert rc;
        rc=win.add(2, oob());
        assert !(rc);
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


    private static Message oob() {
        Message retval=new Message();
        retval.setFlag(Message.OOB);
        return retval;
    }


    private static void check(NakReceiverWindow win, long lowest, long highest_received, long highest_delivered) {
        Assert.assertEquals(win.getLowestSeen(), lowest, "lowest=" + lowest + ", win.lowest=" + win.getLowestSeen());
        Assert.assertEquals(win.getHighestReceived(), highest_received, "highest_received=" + highest_received + ", win.highest_received=" + win.getHighestReceived());
        Assert.assertEquals(win.getHighestDelivered(), highest_delivered, "highest_delivered=" + highest_delivered + ", win.highest_delivered=" + win.getHighestDelivered());
    }


    private static class MyRetransmitCommand implements Retransmitter.RetransmitCommand {

        public void retransmit(long first_seqno, long last_seqno, Address sender) {
        }
    }


}
