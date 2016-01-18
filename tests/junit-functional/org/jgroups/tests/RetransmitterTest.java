
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.DefaultRetransmitter;
import org.jgroups.stack.ExponentialInterval;
import org.jgroups.stack.RangeBasedRetransmitter;
import org.jgroups.stack.AbstractRetransmitter;
import org.jgroups.util.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="createRetransmitter")
public class RetransmitterTest {
    private final Address sender=Util.createRandomAddress();
    private TimeScheduler timer;

    @BeforeClass
    void createTimer() {
        timer=new MockTimeScheduler();
    }

    @AfterClass
    void destroyTimer() {
        timer.stop();
    }


    @DataProvider(name="createRetransmitter")
    protected AbstractRetransmitter[][] createRetransmitter() {
        AbstractRetransmitter range_based_retransmitter=new RangeBasedRetransmitter(sender, new MyXmitter(), timer);
        AbstractRetransmitter old_retransmitter=new DefaultRetransmitter(sender, new MyXmitter(), timer);

        range_based_retransmitter.setRetransmitTimeouts(new ExponentialInterval(1000));
        range_based_retransmitter.reset();
        old_retransmitter.setRetransmitTimeouts(new ExponentialInterval(1000));
        old_retransmitter.reset();

        return new AbstractRetransmitter[][] {
          {old_retransmitter},
          {range_based_retransmitter}
        };
    }




    @Test(dataProvider="createRetransmitter")
    public void testNoEntry(AbstractRetransmitter xmitter) {
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(0, size);
    }


    @Test(dataProvider="createRetransmitter")
    public void testSingleEntry(AbstractRetransmitter xmitter) {
        xmitter.add(1, 1);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(1, size);
    }


    @Test(dataProvider="createRetransmitter")
    public void testEntry(AbstractRetransmitter xmitter) {
        xmitter.add(1, 10);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        Assert.assertEquals(10, size);
    }


    @Test(dataProvider="createRetransmitter")
    public void testMultipleEntries(AbstractRetransmitter xmitter) {
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

    /**
     * Note that we do not have overlapping ranges due to the way {@link org.jgroups.stack.NakReceiverWindow} adds
     * missing messages !
     * @param xmitter
     */
    @Test(dataProvider="createRetransmitter")
    public void testRanges(AbstractRetransmitter xmitter) {
        xmitter.add(100, 200);
        xmitter.add(300, 400);
        System.out.println("xmitter (" + xmitter.getClass().getCanonicalName() + "): " + xmitter);
        assert xmitter.size() == 202 : "size was " + xmitter.size();
    }

    @Test(dataProvider="createRetransmitter")
    public void testAddAndRemoveIndividualSeqnos(AbstractRetransmitter xmitter) {
        int NUM=100;
        List<Long> seqnos=new ArrayList<>(NUM);
        for(long i=1; i <= NUM; i++) {
            seqnos.add(i);
            xmitter.add(i, i);
        }
        
        System.out.println("xmitter = " + xmitter);
        assert xmitter.size() == NUM;
        Collections.shuffle(seqnos);
        while(!seqnos.isEmpty()) {
            long seqno=seqnos.remove(0);
            xmitter.remove(seqno);
        }
        System.out.println("xmitter = " + xmitter);
        assert xmitter.size() == 0 : "expected size of 0, but size is " + xmitter.size();
    }


    @Test(dataProvider="createRetransmitter")
    public void testAddAndRemoveRanges(AbstractRetransmitter xmitter) {
        int NUM=100;
        List<Long> seqnos=new ArrayList<>(NUM);
        for(long i=1; i <= NUM; i++)
            seqnos.add(i);

        xmitter.add(1, NUM);

        System.out.println("xmitter = " + xmitter);
        assert xmitter.size() == NUM;
        Collections.shuffle(seqnos);
        while(!seqnos.isEmpty()) {
            long seqno=seqnos.remove(0);
            xmitter.remove(seqno);
        }
        System.out.println("xmitter = " + xmitter);
        assert xmitter.size() == 0 : "expected size of 0, but size is " + xmitter.size();
    }


    static class MyXmitter implements AbstractRetransmitter.RetransmitCommand {

        public void retransmit(long first_seqno, long last_seqno, Address sender) {
        }
    }

}
