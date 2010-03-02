package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.Util;
import org.jgroups.util.Tuple;
import org.jgroups.stack.AckReceiverWindow;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;

import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.util.LinkedList;


/**
 * @author Bela Ban
 * @version $Id: AckReceiverWindowTest.java,v 1.16 2010/03/02 08:17:48 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class AckReceiverWindowTest {
    AckReceiverWindow win;

    @BeforeMethod
    public void setUp() throws Exception {
        win=new AckReceiverWindow(1);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        win.reset();
    }


    public void testAdd() {
        Message m;
        win.reset(); // use a different window for this test method
        win=new AckReceiverWindow(10);
        Assert.assertEquals(0, win.size());
        win.add(9, msg());
        Assert.assertEquals(0, win.size());

        win.add(10, msg());
        Assert.assertEquals(1, win.size());

        win.add(13, msg());
        Assert.assertEquals(2, win.size());

        m=win.remove();
        assert m != null;
        Assert.assertEquals(1, win.size());

        m=win.remove();
        assert m == null;
        Assert.assertEquals(1, win.size());

        win.add(11, msg());
        win.add(12, msg());
        Assert.assertEquals(3, win.size());

        m=win.remove();
        assert m != null;
        m=win.remove();
        assert m != null;
        m=win.remove();
        assert m != null;
        Assert.assertEquals(0, win.size());
        m=win.remove();
        assert m == null;
    }

    public void testAddExisting() {
        win.add(1, msg());
        assert win.size() == 1;
        win.add(1, msg());
        assert win.size() == 1;
        win.add(2, msg());
        assert win.size() == 2;
    }

    public void testAddLowerThanNextToRemove() {
        win.add(1, msg());
        win.add(2, msg());
        win.remove(); // next_to_remove is 2
        win.add(1, msg());
        assert win.size() == 1;
    }

    public void testRemove() {
        win.add(2, msg());
        Message msg=win.remove();
        assert msg == null;
        assert win.size() == 1;
        win.add(1, msg());
        assert win.size() == 2;
        assert win.remove() != null;
        assert win.size() == 1;
        assert win.remove() != null;
        assert win.size() == 0;
        assert win.remove() == null;
    }



    public void testDuplicates() {
        Assert.assertEquals(0, win.size());
        assert win.add(9, msg());
        assert win.add(1, msg());
        assert win.add(2, msg());
        assert !win.add(2, msg());
        assert !win.add(0, msg());
        assert win.add(3, msg());
        assert win.add(4, msg());
        assert !win.add(4, msg());
    }



    public static void testRemoveRegularMessages() {
        AckReceiverWindow win=new AckReceiverWindow(1);
        win.add(1, msg());
        System.out.println("win = " + win);
        win.remove();
        System.out.println("win = " + win);
        assert win.size() == 0;

        win.add(3, msg());
        win.remove();
        System.out.println("win = " + win);
        assert win.size() == 1;

        win.add(2, msg(true));
        win.remove();
        System.out.println("win = " + win);

        assert win.size() == 1;
    }


    public static void testRemoveMany() {
        AckReceiverWindow win=new AckReceiverWindow(1);
        Tuple<List<Message>, Long> tuple=win.removeMany(100);
        assert tuple == null;

        win.add(2, msg());
        win.add(4, msg());
        tuple=win.removeMany(100);
        assert tuple == null;

        win.add(3, msg());
        win.add(1, msg());

        tuple=win.removeMany(10);
        List<Message> list=tuple.getVal1();
        assert list.size() == 4;
        assert tuple.getVal2() == 4;
    }


    public static void testRemoveMany2() {
        AckReceiverWindow win=new AckReceiverWindow(1);
        for(int i=1; i <=50; i++)
            win.add(i, msg());
        System.out.println("win = " + win);
        List<Message> list=win.removeManyAsList(25);

        System.out.println("win = " + win);
        assert list != null && list.size() == 25;
        assert win.size() == 25;

        list=win.removeManyAsList(30);
        System.out.println("win = " + win);
        assert list != null && list.size() == 25;
        assert win.size() == 0;
    }


    public static void testSmallerThanNextToRemove() {
        AckReceiverWindow win=new AckReceiverWindow(1);
        Message msg;
        for(long i=1; i <= 5; i++)
            win.add(i, msg());
        System.out.println("win = " + win);
        boolean added=win.add(3, msg());
        assert !added;
        for(;;) {
            msg=win.remove();
            if(msg == null)
                break;
        }
        added=win.add(3, msg());
        assert !added;
    }


    public static void testConcurrentAdds() throws InterruptedException {
        AckReceiverWindow win=new AckReceiverWindow(1);
        final int NUM=100;
        final int NUM_THREADS=10;
        final CountDownLatch latch=new CountDownLatch(1);

        final Adder[] adders=new Adder[NUM_THREADS];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(1, NUM, 10, win, latch);
        }
        for(Adder adder: adders)
            adder.start();

        latch.countDown();

        for(Adder adder: adders)
            adder.join();

        System.out.println("win = " + win);
        assert win.size() == NUM;
    }

    @Test(invocationCount=10)
    public static void testConcurrentAddsAndRemoves() throws InterruptedException {
        AckReceiverWindow win=new AckReceiverWindow(1);
        final int NUM=100;
        final int NUM_THREADS=10;
        final CountDownLatch latch=new CountDownLatch(1);

        final Adder[] adders=new Adder[NUM_THREADS];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(1, NUM, 10, win, latch);
            adders[i].start();
        }

        final Remover[] removers=new Remover[NUM_THREADS];
        for(int i=0; i < removers.length; i++) {
            removers[i]=new Remover(win, latch);
            removers[i].start();
        }

        latch.countDown();

        for(Adder adder: adders)
            adder.join();

        System.out.println("win = " + win);

        int total=0;
        int index=0;
        for(Remover remover: removers) {
            remover.join();
            List<Message> list=remover.getList();
            System.out.println("remover #" + index++ + ": " + list.size() + " msgs");
            total+=list.size();
        }

        System.out.println("total = " + total);
        if(total != NUM) {
            for(Remover remover: removers) {
                System.out.println(remover + ": " + print(remover.getList()));
            }
        }
        assert total == NUM;
    }

    private static String print(List<Message> list) {
        StringBuilder sb=new StringBuilder();
        for(Message msg: list) {
            if(msg == AckReceiverWindow.TOMBSTONE)
                sb.append("T ");
            else
                sb.append(msg.getObject() + " ");
        }
        return sb.toString();
    }

    private static Message msg() {
        return msg(false);
    }

    private static Message msg(long seqno) {
        return msg(false, seqno);
    }

    private static Message msg(boolean oob, long seqno) {
        Message retval=new Message(null, null, seqno);
        if(oob)
            retval.setFlag(Message.OOB);
        return retval;
    }

    private static Message msg(boolean oob) {
        Message retval=new Message();
        if(oob)
            retval.setFlag(Message.OOB);
        return retval;
    }


    private static class Adder extends Thread {
        private final long from, to, duplicates;
        private final AckReceiverWindow win;
        private final CountDownLatch latch;

        public Adder(long from, long to, long duplicates, AckReceiverWindow win, CountDownLatch latch) {
            this.from=from;
            this.to=to;
            this.duplicates=duplicates;
            this.win=win;
            this.latch=latch;
            setName("Adder");
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            for(long i=from; i <= to; i++) {
                for(int j=0; j < duplicates; j++) {
                    win.add(i, msg(true, i));
                }
            }
        }
    }

    private static class Remover extends Thread {
        private final AckReceiverWindow win;
        private final CountDownLatch latch;
        private final List<Message> list=new LinkedList<Message>();

        public Remover(AckReceiverWindow win, CountDownLatch latch) {
            this.win=win;
            this.latch=latch;
            setName("Remover");
        }

        public List<Message> getList() {
            return list;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }

            int cnt=5;
            while(true) {
                Message msg=win.remove();
                if(msg != null) {
                    list.add(msg);
                }
                else {
                    if(cnt-- <= 0)
                        break;
                    else
                        Util.sleep(100);
                }
            }
            
        }
    }

}
