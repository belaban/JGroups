
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Tests the UNICAST{3,4} protocols with messages sent by member A to itself
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="createUnicast")
public class UNICAST_MessagesToSelfTest {
    protected JChannel ch;
    protected Address  a1;
    static final int   SIZE=1000; // bytes
    static final int   NUM_MSGS=10_000;

    @AfterMethod void tearDown() throws Exception {Util.close(ch);}

    @DataProvider
    static Object[][] createUnicast() {
        return new Object[][] {
          {new UNICAST3()},
          {new UNICAST4()}
        };
    }

    public void testReceptionOfAllMessages(Protocol prot) throws Throwable {
        System.out.println("prot=" + prot.getClass().getSimpleName());
        ch=createChannel(prot, null).name("A");
        ch.connect("UNICAST_Test.testReceptionOfAllMessages");
        a1=ch.getAddress();
        assert a1 != null;
        _testReceptionOfAllMessages();
    }

    public void testReceptionOfAllMessagesWithDISCARD(Protocol prot) throws Throwable {
        System.out.println("prot=" + prot.getClass().getSimpleName());
        DISCARD discard=new DISCARD();
        discard.setDownDiscardRate(0.1); // discard all down message with 10% probability
        ch=createChannel(prot, discard).name("A");
        ch.connect("UNICAST_Test.testReceptionOfAllMessagesWithDISCARD");
        a1=ch.getAddress();
        assert a1 != null;
        _testReceptionOfAllMessages();
    }

    private static byte[] createPayload(int size, int seqno) {
        return ByteBuffer.allocate(size).putInt(seqno).array();
    }

    protected static JChannel createChannel(Protocol unicast, DISCARD discard) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new SHARED_LOOPBACK_PING(),
                                 new NAKACK2().useMcastXmit(false),
                                 unicast,
                                 new STABLE().setMaxBytes(50000),
                                 new GMS().printLocalAddress(false));
        if(discard != null)
            ch.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, SHARED_LOOPBACK.class);
        return ch;
    }





    private void _testReceptionOfAllMessages() throws Throwable {
        final MyReceiver r=new MyReceiver();
        ch.setReceiver(r);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new BytesMessage(a1, createPayload(SIZE, i)); // unicast message
            ch.send(msg);
            if(i % 1000 == 0)
                System.out.println("==> " + i);
        }

        for(int i=0; i < 20; i++) {
            if(r.getException() != null)
                throw r.getException();
            if(r.getNumberOfReceivedMessages() >= NUM_MSGS)
                break;
            Util.sleep(1000);
        }
        int num_received=r.getNumberOfReceivedMessages();
        printStats(num_received);

        assert num_received == NUM_MSGS : "list is " + printList(r.getList()) ;
    }


    protected static String printList(List<Integer> list) {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < 10; i++)
            sb.append(list.get(i) + " ");

        sb.append(" ... ");

        for(int i=list.size() - 10; i < list.size(); i++)
            sb.append(list.get(i) + " ");

        return sb.toString();
    }


    private static void printStats(int num_received) {
        System.out.println("-- num received=" + num_received);
    }

    /** Checks that messages 1 - NUM_MSGS are received in order */
    protected static class MyReceiver implements Receiver {
        int num_mgs_received=0, next=1;
        Throwable exception=null;
        protected final List<Integer> list=new ArrayList<>(NUM_MSGS);

        public void receive(Message msg) {
            if(exception != null)
                return;
            ByteBuffer buf=ByteBuffer.wrap(msg.getArray(), msg.getOffset(), msg.getLength());
            int seqno=buf.getInt();
            if(seqno != next) {
                exception=new Exception("expected seqno was " + next + ", but received " + seqno);
                return;
            }
            list.add(seqno);
            next++;
            num_mgs_received++;
            if(num_mgs_received % 1000 == 0)
                System.out.println("<== " + num_mgs_received);
        }

        public int       getNumberOfReceivedMessages() {return num_mgs_received;}
        public Throwable getException()                {return exception;}
        public List<Integer> getList() {return list;}
    }



}
