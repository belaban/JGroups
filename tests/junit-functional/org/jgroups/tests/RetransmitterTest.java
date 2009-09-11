// $Id: RetransmitterTest.java,v 1.2.2.1 2009/09/11 11:31:48 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.Address;
import org.jgroups.util.TimeScheduler;
import org.jgroups.stack.Retransmitter;
import org.jgroups.stack.StaticInterval;


public class RetransmitterTest extends TestCase {
    private final Address sender=new org.jgroups.stack.IpAddress(5555);
    Retransmitter xmitter;
    TimeScheduler timer=new TimeScheduler();

    protected void setUp() throws Exception {
        super.setUp();
        xmitter=new Retransmitter(sender, new MyXmitter(), timer);
        xmitter.setRetransmitTimeouts(new StaticInterval(1000,2000,4000,8000));
    }


    public static void main(String[] args) {
        String[] testCaseName={RetransmitterTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


    public void testNoEntry() {
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(0, size);
    }

    public void testSingleEntry() {
        xmitter.add(1, 1);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(1, size);
    }

    public void testEntry() {
        xmitter.add(1, 10);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(10, size);
    }

    public void testMultipleEntries() {
        xmitter.add(1, 10);
        int size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(10, size);

        xmitter.add(12,13);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(12, size);

        xmitter.remove(5);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(11, size);

        xmitter.remove(13);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(10, size);

        xmitter.remove(1);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(9, size);

        xmitter.remove(13);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(9, size);

        xmitter.remove(12);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(8, size);

        for(int i=8; i >= 0; i--)
            xmitter.remove(i);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(2, size);

        xmitter.remove(10);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(1, size);

        xmitter.remove(9);
        size=xmitter.size();
        System.out.println("xmitter: " + xmitter);
        assertEquals(0, size);
    }


    static class MyXmitter implements Retransmitter.RetransmitCommand {

        public void retransmit(long first_seqno, long last_seqno, Address sender) {
        }
    }

}
