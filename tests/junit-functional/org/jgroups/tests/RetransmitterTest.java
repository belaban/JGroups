// $Id: RetransmitterTest.java,v 1.8 2010/07/19 06:44:25 belaban Exp $

package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.DefaultRetransmitter;
import org.jgroups.stack.Retransmitter;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.DefaultTimeScheduler;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups=Global.FUNCTIONAL,sequential=true)
public class RetransmitterTest {
    private final Address sender=Util.createRandomAddress();
    private TimeScheduler timer;
    private Retransmitter xmitter;


    @BeforeMethod
    void initTimer() {
        timer=new DefaultTimeScheduler();
        xmitter=new DefaultRetransmitter(sender, new MyXmitter(), timer);
        xmitter.setRetransmitTimeouts(new StaticInterval(1000,2000,4000,8000));
        xmitter.reset();
    }

    @AfterMethod
    void destroyTimer() throws InterruptedException {
        timer.stop();
    }

    public void testNoEntry() {
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(0, size);
    }


    public void testSingleEntry() {
        xmitter.add(1, 1);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(1, size);
    }


    public void testEntry() {
        xmitter.add(1, 10);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(10, size);
    }


    public void testMultipleEntries() {
        xmitter.add(1, 10);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(10, size);

        xmitter.add(12,13);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(12, size);

        xmitter.remove(5);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(11, size);

        xmitter.remove(13);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(10, size);

        xmitter.remove(1);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(9, size);

        xmitter.remove(13);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(9, size);

        xmitter.remove(12);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(8, size);

        for(int i=8; i >= 0; i--)
            xmitter.remove(i);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(2, size);

        xmitter.remove(10);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(1, size);

        xmitter.remove(9);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(0, size);
    }


    static class MyXmitter implements Retransmitter.RetransmitCommand {

        public void retransmit(long first_seqno, long last_seqno, Address sender) {
        }
    }

}
