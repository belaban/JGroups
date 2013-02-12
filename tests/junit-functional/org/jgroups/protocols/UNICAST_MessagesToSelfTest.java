
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;


/**
 * Tests the UNICAST{2,3} protocols with messages sent by member A to itself
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class UNICAST_MessagesToSelfTest {
    protected JChannel ch;
    protected Address  a1;

    static final int SIZE=1000; // bytes
    static final int NUM_MSGS=10000;


    @AfterMethod void tearDown() throws Exception {Util.close(ch);}

    @DataProvider
    static Object[][] configProvider() {
        return new Object[][] {
          {new UNICAST()},
          {new UNICAST2()},
          {new UNICAST3()}
        };
    }

    @Test(dataProvider="configProvider")
    public void testReceptionOfAllMessages(Protocol prot) throws Throwable {
        System.out.println("prot=" + prot.getClass().getSimpleName());
        ch=createChannel(prot, null);
        ch.connect("UNICAST_Test.testReceptionOfAllMessages");
        _testReceptionOfAllMessages();
    }


    @Test(dataProvider="configProvider")
    public void testReceptionOfAllMessagesWithDISCARD(Protocol prot) throws Throwable {
        System.out.println("prot=" + prot.getClass().getSimpleName());
        DISCARD discard=new DISCARD();
        discard.setDownDiscardRate(0.1); // discard all down message with 10% probability
        ch=createChannel(prot, discard);
        ch.connect("UNICAST_Test.testReceptionOfAllMessagesWithDISCARD");
        _testReceptionOfAllMessages();
    }



    private static byte[] createPayload(int size, int seqno) {
        return ByteBuffer.allocate(size).putInt(seqno).array();
    }

    protected static JChannel createChannel(Protocol unicast, DISCARD discard) throws Exception {
        JChannel ch=new JChannel(false);
        ProtocolStack stack=new ProtocolStack();
        ch.setProtocolStack(stack);
        stack.addProtocol(new SHARED_LOOPBACK());

        if(discard != null)
            stack.addProtocol(discard);
        
        stack.addProtocol(new PING().setValue("timeout", 100))
          .addProtocol(new NAKACK2().setValue("use_mcast_xmit", false))
          .addProtocol(unicast)
          .addProtocol(new STABLE().setValue("max_bytes", 50000))
          .addProtocol(new GMS().setValue("print_local_addr", false));
        stack.init();
        return ch;
    }





    private void _testReceptionOfAllMessages() throws Throwable {
        final Receiver r=new Receiver();
        ch.setReceiver(r);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new Message(a1, null, createPayload(SIZE, i)); // unicast message
            ch.send(msg);
            if(i % 1000 == 0)
                System.out.println("==> " + i);
        }

        for(int i=0; i < 10; i++) {
            if(r.getException() != null)
                throw r.getException();
            if(r.getNumberOfReceivedMessages() >= NUM_MSGS)
                break;
            Util.sleep(1000);
        }
        int num_received=r.getNumberOfReceivedMessages();
        printStats(num_received);
        Assert.assertEquals(num_received, NUM_MSGS);
    }


    private static void printStats(int num_received) {
        System.out.println("-- num received=" + num_received);
    }

    /** Checks that messages 1 - NUM_MSGS are received in order */
    static class Receiver extends ReceiverAdapter {
        int num_mgs_received=0, next=1;
        Throwable exception=null;

        public void receive(Message msg) {
            if(exception != null)
                return;
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
        }

        public int       getNumberOfReceivedMessages() {return num_mgs_received;}
        public Throwable getException()                {return exception;}
    }



}
