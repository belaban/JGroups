

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;


/**
 * Tests a SEQUENCER based stack: A, B and C. B starts multicasting messages with a monotonically increasing
 * number. Then A is crashed. C and B should receive *all* numbers *without* a gap.
 * @author Bela Ban
 * @version $Id: SequencerFailoverTest.java,v 1.15 2009/10/14 09:41:48 belaban Exp $
 */
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class SequencerFailoverTest {
    JChannel a, b, c; // A is the coordinator
    static final String GROUP="demo-group";
    static final int NUM_MSGS=50;
    static final String props="sequencer.xml";


    @BeforeMethod
    public void setUp() throws Exception {
        a=new JChannel(props);
        a.setName("A");
        a.connect(GROUP);

        b=new JChannel(props);
        b.setName("B");
        b.connect(GROUP);

        c=new JChannel(props);
        c.setName("C");
        c.connect(GROUP);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Util.close(c, b, a);
    }

    @Test
    public void testBroadcastSequence() throws Exception {
        MyReceiver rb=new MyReceiver("B"), rc=new MyReceiver("C");
        b.setReceiver(rb); c.setReceiver(rc);

        View v2=b.getView(), v3=c.getView();
        System.out.println("ch2's view: " + v2 + "\nch3's view: " + v3);
        assert v2.equals(v3);

        new Thread() {
            public void run() {
                Util.sleep(3000);
                System.out.println("** killing A");
                try {
                    Util.shutdown(a);
                }
                catch(Exception e) {
                    System.err.println("failed shutting down channel " + a.getAddress() + ", exception=" + e);
                }
                System.out.println("** A killed");
                injectSuspectEvent(a.getAddress(), b, c);
                a=null;
            }
        }.start();

        for(int i=1; i <= NUM_MSGS; i++) {
            Util.sleep(300);
            b.send(new Message(null, null, new Integer(i)));
            System.out.print("-- messages sent: " + i + "/" + NUM_MSGS + "\r");
        }
        System.out.println("");
        v2=b.getView();
        v3=c.getView();
        System.out.println("B's view: " + v2 + "\nC's view: " + v3);
        assert v2.equals(v3);
        assert v2.size() == 2;
        int s2, s3;
        for(int i=15000; i > 0; i-=1000) {
            s2=rb.size(); s3=rc.size();
            if(s2 >= NUM_MSGS && s3 >= NUM_MSGS) {
                System.out.print("B: " + s2 + " msgs, C: " + s3 + " msgs\r");
                break;
            }
            Util.sleep(1000);
            System.out.print("sleeping for " + (i/1000) + " seconds (B: " + s2 + " msgs, C: " + s3 + " msgs)\r");
        }
        System.out.println("-- verifying messages on B and C");
        verifyNumberOfMessages(NUM_MSGS, rb);
        verifyNumberOfMessages(NUM_MSGS, rc);
    }

    private static void verifyNumberOfMessages(int num_msgs, MyReceiver receiver) throws Exception {
        List<Integer> msgs=receiver.getList();
        int size=msgs.size();
        assert num_msgs == size : "[" + receiver.name + "] list has " + size + " msgs (should have " + num_msgs + ")";
        System.out.println("[" + receiver.name + "] list has " + size + " msgs: OK");
        System.out.println("Verifying message order:");
        int i=1;
        for(Integer tmp: msgs) {
            if(tmp != i)
                throw new Exception("[" + receiver.name + "] expected " + i + ", but got " + tmp + ", msg list: " + msgs);
            i++;
        }
        System.out.println("[" + receiver.name + "] message order is OK");
    }



    /** Injects SUSPECT event(suspected_mbr) into channels */
    private static void injectSuspectEvent(Address suspected_mbr, JChannel ... channels) {
        Event evt=new Event(Event.SUSPECT, suspected_mbr);
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            if(gms != null)
                gms.up(evt);
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        private final List<Integer> list=new LinkedList<Integer>();
        private final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public List<Integer> getList() {return list;}

        public int size() {return list.size();}

        public void receive(Message msg) {
            list.add((Integer)msg.getObject());
        }

        void clear() {list.clear();}
    }


}
