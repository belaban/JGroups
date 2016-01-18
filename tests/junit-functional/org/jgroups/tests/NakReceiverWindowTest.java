
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.NakReceiverWindow;
import org.jgroups.stack.AbstractRetransmitter;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;


@Test(groups=Global.FUNCTIONAL)
public class NakReceiverWindowTest {
    private static final Address sender=Util.createRandomAddress();
    private static final MyRetransmitCommand cmd=new MyRetransmitCommand();


    @DataProvider(name="createTimer")
    Object[][] createTimer() {
        return Util.createTimer();
    }


    @Test(dataProvider="createTimer")
    public void test1(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1, timer);
            check(win, 1, 1);
            assert win.get(23) == null;
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test2(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, timer);
            check(win, 100, 100);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test3(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            assert win.get(1) != null;
            check(win, 1, 0);
            win.add(2, new Message());
            check(win, 2, 0);
            assert win.get(2) != null;
            win.remove();
            check(win, 2, 1);
            win.remove();
            check(win, 2, 2);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test4(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1, timer);
            win.add(2, new Message());
            check(win, 2, 1);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test5(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, timer);
            win.add(101, new Message());
            win.add(100, new Message());
            check(win, 101, 100);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test6(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, timer);
            win.add(101, new Message());
            System.out.println("win: " + win);
            win.add(100, new Message());
            System.out.println("win: " + win);
            check(win, 101, 100);
            win.remove();
            System.out.println("win: " + win);
            check(win, 101, 101);
            while((win.remove()) != null);
            check(win, 101, 101);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testLowerBounds(TimeScheduler timer) {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, timer);
            win.add(101, new Message());
            System.out.println("win: " + win);
            win.add(100, new Message());
            System.out.println("win: " + win);
            check(win, 101, 100);
            win.remove();
            System.out.println("win: " + win);
            check(win, 101, 101);
            while((win.remove()) != null);
            check(win, 101, 101);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test7(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(2, new Message());
            win.add(3, new Message());
            win.add(4, new Message());
            check(win, 4, 0);
            System.out.println("Note that the subsequent warning is expected:");
            win.stable(4); // no-op because we haven't even removed 4 messages
            check(win, 4, 0);
            while(win.remove() != null);
            check(win, 4, 4);
            win.stable(4);
            check(win, 4, 4);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testLowerBounds2(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, timer);
            win.add(100, new Message());
            win.add(101, new Message());
            win.add(102, new Message());
            win.add(103, new Message());
            System.out.println("win: " + win);
            check(win, 103, 100);
            System.out.println("Note that the subsequent warning is expected:");
            win.stable(103); // no-op because we haven't even removed 4 messages
            check(win, 103, 100);
            while(win.remove() != null);
            check(win, 103, 103);
            win.stable(103);
            System.out.println("win: " + win);
            check(win, 103, 103);
        }
        finally {
            timer.stop();
        }
    }



    @Test(dataProvider="createTimer")
    public void test8(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(2, new Message());
            win.add(3, new Message());
            win.add(4, new Message());
            win.add(6, new Message());
            check(win, 6, 0); // haven't delivered a message yet
            while(win.remove() != null);
            check(win, 6, 4);
            win.add(5, new Message());
            check(win, 6, 4);
            win.remove();
            check(win, 6, 5);
            win.remove();
            check(win, 6, 6);
            win.stable(4);
            check(win, 6, 6);
            win.stable(6);
            check(win, 6, 6);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testAdd(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            check(win, 0, 0);
            win.add(0, new Message()); // discarded, next expected is 1
            check(win, 0, 0);
            win.add(1, new Message());
            check(win, 1, 0);
            win.add(2, new Message());
            win.add(3, new Message());
            win.add(4, new Message());
            check(win, 4, 0);
            win.add(6, new Message());
            check(win, 6, 0);
            win.add(5, new Message());
            check(win, 6, 0);
            while(win.remove() != null) ;
            check(win, 6, 6);
            win.stable(4);
            check(win, 6, 6);
            win.stable(6);
            check(win, 6, 6);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test9(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
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
            check(win, 6, 4);
            win.add(5, new Message());
            check(win, 6, 4);
            while((win.remove()) != null) ;
            check(win, 6, 6);
            win.stable(6);
            check(win, 6, 6);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testHighestDelivered(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(2, new Message());
            win.add(3, new Message());
            win.add(4, new Message());
            check(win, 4, 0);
            win.add(10, new Message());
            check(win, 10, 0);
            System.out.println("win: " + win);
            win.add(9, new Message());
            win.add(7, new Message());
            win.add(8, new Message());
            win.add(6, new Message());
            win.add(5, new Message());
            System.out.println("win: " + win);
            check(win, 10, 0);
            while((win.remove()) != null) ;
            check(win, 10, 10);
            win.stable(5);
            System.out.println("win: " + win);
            check(win, 10, 10);
            win.stable(10);
            System.out.println("win: " + win);
            check(win, 10, 10);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testMissingMessages(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(5, new Message());
            check(win, 5, 0);
            win.add(6, new Message());
            check(win, 6, 0);
            System.out.println("win: " + win);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testMissingMessages2(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(5, new Message());
            check(win, 5, 0);
            win.add(8, new Message());
            check(win, 8, 0);
            win.add(9, new Message());
            check(win, 9, 0);
            System.out.println("win: " + win);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testMissingMessages3(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(5, new Message());
            check(win, 5, 0);
            win.add(8, new Message());
            check(win, 8, 0);
            win.add(9, new Message());
            check(win, 9, 0);
            System.out.println("win: " + win);
            win.add(2, new Message());
            check(win, 9, 0);
            win.add(3, new Message());
            win.add(4, new Message());
            check(win, 9, 0);
            win.add(7, new Message());
            check(win, 9, 0);
            win.add(6, new Message());
            check(win, 9, 0);
            win.add(10, new Message());
            check(win, 10, 0);
            win.add(11, new Message());
            check(win, 11, 0);
            System.out.println("win: " + win);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testMissingMessages4(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, timer);
            win.add(101, new Message());
            win.add(105, new Message());
            check(win, 105, 100);
            win.add(108, new Message());
            check(win, 108, 100);
            win.add(109, new Message());
            check(win, 109, 100);
            System.out.println("win: " + win);
            win.add(102, new Message());
            check(win, 109, 100);
            win.add(103, new Message());
            win.add(104, new Message());
            check(win, 109, 100);
            win.add(107, new Message());
            check(win, 109, 100);
            win.add(106, new Message());
            check(win, 109, 100);
            win.add(110, new Message());
            check(win, 110, 100);
            win.add(110, new Message());
            check(win, 110, 100);
            System.out.println("win: " + win);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testMissingMessages5(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 100, timer);
            win.add(101, new Message());
            check(win, 101, 100);
            win.add(108, new Message());
            check(win, 108, 100);
            win.remove();
            win.add(109, new Message());
            check(win, 109, 101);
            System.out.println("win: " + win);
            win.add(102, new Message());
            check(win, 109, 101);
            win.add(103, new Message());
            win.add(104, new Message());
            check(win, 109, 101);
            win.add(107, new Message());
            check(win, 109, 101);
            win.add(106, new Message());
            win.add(105, new Message());
            check(win, 109, 101);
            win.add(110, new Message());
            check(win, 110, 101);
            win.add(110, new Message());
            check(win, 110, 101);
            System.out.println("win: " + win);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test10(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(2, new Message());
            win.add(3, new Message());
            win.add(4, new Message());
            while((win.remove()) != null) ;
            check(win, 4, 4);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test10a(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(2, new Message());
            win.add(3, new Message());
            win.add(4, new Message());
            while((win.remove()) != null) ;
            win.stable(4);
            check(win, 4, 4);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test11(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(2, new Message());
            win.add(3, new Message());
            win.add(4, new Message());
            while((win.remove()) != null) ;
            win.destroy();
            check(win, 0, 0);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test12(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);

            win.add(1, new Message(null, null, new Integer(1)));
            win.add(2, new Message(null, null, new Integer(2)));
            win.add(3, new Message(null, null, new Integer(3)));

            Assert.assertEquals(1, ((Integer)win.remove().getObject()).intValue());
            Assert.assertEquals(2, ((Integer)win.remove().getObject()).intValue());
            Assert.assertEquals(3, ((Integer)win.remove().getObject()).intValue());
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test13(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, new Message());
            win.add(2, new Message());
            win.add(3, new Message());
            win.add(4, new Message());
            check(win, 4, 0);
            win.remove();
            win.remove();
            win.add(5, new Message());
            win.add(6, new Message());
            check(win, 6, 2);
            win.stable(2);
            check(win, 6, 2);
        }
        finally {
            timer.stop();
        }
    }



    @Test(dataProvider="createTimer")
    public void testAddOOBAtHead(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            boolean rc;
            rc=win.add(0, oob());
            assert !(rc);
            rc=win.add(1, oob());
            assert rc;
            rc=win.add(1, oob());
            assert !(rc);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testAddOOBAtTail(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            boolean rc;
            rc=win.add(1, oob());
            assert rc;
            rc=win.add(2, oob());
            assert rc;
            rc=win.add(2, oob());
            assert !(rc);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testAddOOBInTheMiddle(TimeScheduler timer) throws Exception {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
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
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testUpdateHighestSeen(TimeScheduler timer) {
        add(1000, timer);
        add(2000, timer);
        add(3000, timer);
        add(4000, timer);
        add(5000, timer);
        add(10000, timer);
        add(15000, timer);
        add(20000, timer);
        add(30000, timer);
    }


    @Test(dataProvider="createTimer")
    public void test1000(TimeScheduler timer) {
        add(1000, timer);
    }


    @Test(dataProvider="createTimer")
    public void test10000(TimeScheduler timer) {
        add(10000, timer);
    }





    @Test(dataProvider="createTimer")
    public void testRemoveRegularAndOOBMessages(TimeScheduler timer) {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, msg());
            System.out.println("win = " + win);
            win.remove();
            System.out.println("win = " + win);
            assert win.getHighestDelivered() == 1;

            win.add(3, msg());
            win.remove();
            System.out.println("win = " + win);
            assert win.getHighestDelivered() == 1;

            win.add(2, oob());
            System.out.println("win = " + win);

            assert win.getHighestDelivered() == 1 : "highest_delivered should be 2, but is " + win.getHighestDelivered();
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testRemoveMany(TimeScheduler timer) {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, msg());
            win.add(2, msg());
            win.add(3, msg());
            win.add(5, msg());
            win.add(6, msg());
            System.out.println("win = " + win);
            List<Message> msgs=win.removeMany(null);
            System.out.println("msgs = " + msgs);
            assert msgs.size() == 3;

            win.add(4, msg());
            msgs=win.removeMany(null);
            System.out.println("msgs = " + msgs);
            assert msgs.size() == 3;
        }
        finally {
            timer.stop();
        }
    }

    @Test(dataProvider="createTimer")
    public void testRemoveMany2(TimeScheduler timer) {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 4, timer);
            win.add(5, msg());
            win.add(6, msg());
            win.add(7, msg());
            win.add(9, msg());
            win.add(10, msg());
            System.out.println("win = " + win);
            List<Message> msgs=win.removeMany(null);
            System.out.println("msgs = " + msgs);
            assert msgs.size() == 3;

            win.add(8, msg());
            msgs=win.removeMany(null);
            System.out.println("msgs = " + msgs);
            assert msgs.size() == 3;
        }
        finally {
            timer.stop();
        }
    }




    @Test(dataProvider="createTimer")
    public void testRetransmitter(TimeScheduler timer) {
        try {
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 0, timer);
            win.add(1, msg());
            win.add(2, msg());
            win.add(3, msg());
            win.add(5, msg());
            win.add(6, msg());
            System.out.println("win = " + win);
            int num_pending_xmits=win.getPendingXmits();
            assert num_pending_xmits == 1;

            win.add(4, msg());
            num_pending_xmits=win.getPendingXmits();
            assert num_pending_xmits == 0;
        }
        finally {
            timer.stop();
        }
    }



    private static void add(int num_msgs, TimeScheduler timer) {
        try {
            long start, stop;
            double time_per_msg;
            NakReceiverWindow win=new NakReceiverWindow(sender, cmd, 1, timer);
            start=System.currentTimeMillis();
            for(int i=1; i < 1 + num_msgs; i++) {
                win.add(i, new Message());
            }
            stop=System.currentTimeMillis();
            time_per_msg=(stop-start) / (double)num_msgs;
            System.out.println("-- time for " + num_msgs + " msgs: " + (stop-start) + ", " + time_per_msg + " ms/msg");
        }
        finally {
            timer.stop();
        }
    }


    private static Message oob() {
        Message retval=new Message();
        retval.setFlag(Message.Flag.OOB);
        return retval;
    }

    private static Message msg() {
        return new Message();
    }


    private static void check(NakReceiverWindow win, long highest_received, long highest_delivered) {
        Assert.assertEquals(win.getHighestReceived(), highest_received, "highest_received=" + highest_received + ", win.highest_received=" + win.getHighestReceived());
        Assert.assertEquals(win.getHighestDelivered(), highest_delivered, "highest_delivered=" + highest_delivered + ", win.highest_delivered=" + win.getHighestDelivered());
    }


    private static class MyRetransmitCommand implements AbstractRetransmitter.RetransmitCommand {

        public void retransmit(long first_seqno, long last_seqno, Address sender) {
        }
    }


}
