
package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.UNICAST;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Vector;


/**
 * Tests the UNICAST protocol
 * @author Bela Ban
 * @version $id$
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class UNICAST_Test {
    IpAddress a1, a2;
    Vector members;
    View v;
    Simulator simulator;

    static final int SIZE=1000; // bytes
    static final int NUM_MSGS=10000;


    @AfterMethod
    void tearDown() throws Exception {
        if(simulator != null)
            simulator.stop();
    }


    public void testReceptionOfAllMessages() throws Throwable {
        UNICAST unicast=new UNICAST();
        unicast.setTimeout(new long[] {500,1000,2000,3000});
        Protocol[] stack=new Protocol[]{unicast};
        createStack(stack);
        _testReceptionOfAllMessages();
    }


    public void testReceptionOfAllMessagesWithDISCARD() throws Throwable {
        UNICAST unicast=new UNICAST();
        unicast.setTimeout(new long[] {500,1000,2000,3000});

        DISCARD discard=new DISCARD();
        discard.setDownDiscardRate(0.1); // discard all down message with 10% probability

        Protocol[] stack=new Protocol[]{unicast,discard};
        createStack(stack);
        _testReceptionOfAllMessages();
    }



    private static byte[] createPayload(int size, int seqno) {
        ByteBuffer buf=ByteBuffer.allocate(size);
        buf.putInt(seqno);
        return buf.array();
    }


    /** Checks that messages 1 - NUM_MSGS are received in order */
    static class Receiver implements Simulator.Receiver {
        int num_mgs_received=0, next=1;
        Throwable exception=null;
        boolean received_all=false;

        public void receive(Event evt) {
            if(evt.getType() == Event.MSG) {
                if(exception != null)
                return;
                Message msg=(Message)evt.getArg();
                ByteBuffer buf=ByteBuffer.wrap(msg.getRawBuffer());
                int seqno=buf.getInt();
                if(seqno != next) {
                    exception=new Exception("expected seqno was " + next + ", but received " + seqno);
                    return;
                }
                next++;
                num_mgs_received++;
                if(num_mgs_received % 1000 == 0)
                    System.out.println("<== " + num_mgs_received);
                if(num_mgs_received == NUM_MSGS) {
                    synchronized(this) {
                        received_all=true;
                        this.notifyAll();
                    }
                }
            }
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }

        public boolean receivedAll() {return received_all;}

        public Throwable getException() {
            return exception;
        }
    }


    private void _testReceptionOfAllMessages() throws Throwable {
        int num_received=0;
        Receiver r=new Receiver();
        simulator.setReceiver(r);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new Message(a1, null, createPayload(SIZE, i)); // unicast message
            Event evt=new Event(Event.MSG, msg);
            simulator.send(evt);
            if(i % 1000 == 0)
                System.out.println("==> " + i);
        }
        int num_tries=10;
        while((num_received=r.getNumberOfReceivedMessages()) != NUM_MSGS && num_tries > 0) {
            if(r.getException() != null)
                throw r.getException();
            synchronized(r) {
                try {r.wait(3000);}
                catch(InterruptedException e) {}
            }
            num_tries--;
        }
        printStats(num_received);
        Assert.assertEquals(num_received, NUM_MSGS);
    }

    private void createStack(Protocol[] stack) throws Exception {
        a1=new IpAddress(1111);
        members=new Vector();
        members.add(a1);
        v=new View(a1, 1, members);
        simulator=new Simulator();
        simulator.setLocalAddress(a1);
        simulator.setView(v);
        simulator.addMember(a1);
        simulator.setProtocolStack(stack);
        simulator.start();
    }

    private void printStats(int num_received) {
        System.out.println("-- num received=" + num_received + ", stats:\n" + simulator.dumpStats());
    }



}
