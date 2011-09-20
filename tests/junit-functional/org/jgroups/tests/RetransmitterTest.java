
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.DefaultRetransmitter;
import org.jgroups.stack.ExponentialInterval;
import org.jgroups.stack.RangeBasedRetransmitter;
import org.jgroups.stack.Retransmitter;
import org.jgroups.util.DefaultTimeScheduler;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.*;


@Test(groups=Global.FUNCTIONAL,sequential=true,dataProvider="createRetransmitter")
public class RetransmitterTest {
    private final Address sender=Util.createRandomAddress();
    private TimeScheduler timer;

    @BeforeClass
    void createTimer() {
        System.out.println("<< create timer");
        timer=new DefaultTimeScheduler();
    }


    @DataProvider(name="createRetransmitter")
    protected Retransmitter[][] createRetransmitter() {
        Retransmitter range_based_retransmitter=new RangeBasedRetransmitter(sender, new MyXmitter(), timer);
        Retransmitter old_retransmitter=new DefaultRetransmitter(sender, new MyXmitter(), timer);

        range_based_retransmitter.setRetransmitTimeouts(new ExponentialInterval(1000));
        range_based_retransmitter.reset();
        old_retransmitter.setRetransmitTimeouts(new ExponentialInterval(1000));
        old_retransmitter.reset();

        return new Retransmitter[][] {
          {range_based_retransmitter},
          {old_retransmitter}
        };
    }


    @AfterClass
    void destroyTimer() throws InterruptedException {
        System.out.println("<< destroy timer");
        timer.stop();
    }

    @Test(dataProvider="createRetransmitter")
    public void testNoEntry(Retransmitter xmitter) {
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(0, size);
    }


    @Test(dataProvider="createRetransmitter")
    public void testSingleEntry(Retransmitter xmitter) {
        xmitter.add(1, 1);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(1, size);
    }


    @Test(dataProvider="createRetransmitter")
    public void testEntry(Retransmitter xmitter) {
        xmitter.add(1, 10);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(10, size);
    }


    @Test(dataProvider="createRetransmitter")
    public void testMultipleEntries(Retransmitter xmitter) {
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


    @Test(dataProvider="createRetransmitter")
    public void testRanges(Retransmitter xmitter) {
        xmitter.add(200, 500);
        xmitter.add(100, 300);
        System.out.println("xmitter: " + xmitter);
        assert xmitter.size() == 401;
    }


    static class MyXmitter implements Retransmitter.RetransmitCommand {

        public void retransmit(long first_seqno, long last_seqno, Address sender) {
        }
    }

}
