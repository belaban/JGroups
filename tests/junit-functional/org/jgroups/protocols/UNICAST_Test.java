package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="createUnicast")
public class UNICAST_Test {
    protected JChannel           a, b;
    protected Protocol           uni_a, uni_b;
    protected static final short UNICAST3_ID=ClassConfigurator.getProtocolId(UNICAST3.class);
    protected static final short UNICAST4_ID=ClassConfigurator.getProtocolId(UNICAST4.class);
    protected static final int   CONN_CLOSE_TIMEOUT=60_000; // change to a low value (e.g. 1000) to make this test fail

    @DataProvider
    static Object[][] createUnicast() {
        return new Object[][]{
          {UNICAST3.class},
          {UNICAST4.class}
        };
    }

    protected void setup(Class<? extends Protocol> unicast_class) throws Exception {
        a=create("A", unicast_class).connect(getClass().getSimpleName());
        b=create("B", unicast_class).connect(getClass().getSimpleName());
        uni_a=a.getProtocolStack().findProtocol(UNICAST3.class,UNICAST4.class);
        uni_b=b.getProtocolStack().findProtocol(UNICAST3.class,UNICAST4.class);
        DropUnicastAck drop_ack=new DropUnicastAck((short)499);
        b.getProtocolStack().insertProtocol(drop_ack, ProtocolStack.Position.BELOW, UNICAST3.class,ReliableUnicast.class);
    }

    @AfterMethod protected void destroy() {Util.close(b, a);}

    /**
     - A and B exchanging unicast messages
     - Seqno 499 is sent from A to B
     - B adds it to its table for A, and sends the ACK, then delivers the message
     - The ack for 499 from B to A is dropped
     - B excludes A (but A doesn't exclude B) and removes A's table
     * This happens only if conn_close_timeout is small (default: 10s)
     * If conn_close_timeout == 0, connections will not be removed
     - A retransmits 499 to B
     - B receives A:499, but asks for the first seqno
     - A has its highest seqno acked at 498, so resends 499 with first==true
     - B creates a receiver window for A at 499, receives 499 and delivers it (again)

     The issue is fixed by setting CONN_CLOSE_TIMEOUT to a highher value, or to 0
     */
    public void testDuplicateMessageDelivery(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        Address a_addr=a.getAddress();
        Address b_addr=b.getAddress();
        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);

        for(int i=1; i < 500; i++)
            a.send(b_addr, i);

        Util.waitUntilTrue(500, 50, () -> receiver.count >= 499);
        System.out.printf("B: received %d messages from A\n", receiver.count);
        assert receiver.count == 499;

        // remove A's receive window in B:
        System.out.printf("-- closing the receive-window for %s:\n", a_addr);
        // e.g. caused by an asymmetric network split: B excludes A, but not vice versa
        Util.invoke(uni_b, "closeReceiveConnection", a_addr);
        uni_a.setLevel("trace");
        uni_b.setLevel("trace");

        Util.invoke(uni_b, "setConnCloseTimeout", CONN_CLOSE_TIMEOUT);
        // wait until B closes the receive window for A:
        for(int i=0; i < 10; i++) {
            int num_recv_conns=(int)Util.invoke(uni_b, "getNumReceiveConnections");
            if(num_recv_conns == 0)
                break;
            Util.sleep(500);
        }

        // remove the DropUnicastAck protocol:
        System.out.printf("-- removing the %s protocol\n", DropUnicastAck.class.getSimpleName());
        b.getProtocolStack().removeProtocol(DropUnicastAck.class);

        Util.waitUntilTrue(2000, 200, () -> receiver.count >= 500);
        System.out.printf("B: received %d messages from A\n", receiver.count);
        assert receiver.count == 499 : String.format("received %d messages, but should only have received 499", receiver.count);
    }

    protected static JChannel create(String name, Class<? extends Protocol> unicast_class) throws Exception {
        Protocol ucast=unicast_class.getDeclaredConstructor().newInstance();
        return new JChannel(new SHARED_LOOPBACK(), new SHARED_LOOPBACK_PING(), ucast).name(name);
    }

    /**
     * Inserted only into B (under UNICAST3). Drops ACK for 499 from B to A, only acks the previous message (498).
     * Then drops all traffic from A and closes A's receive window in B.
     * When removed, A:499 should get retransmitted and cause duplicate delivery in B.
     */
    protected static class DropUnicastAck extends Protocol {
        protected final short      start_drop_ack;
        protected volatile boolean discarding;

        public DropUnicastAck(short start_drop_ack) {
            this.start_drop_ack=start_drop_ack;
        }

        public Object down(Message msg) {
            UnicastHeader3 hdr3=msg.getHeader(UNICAST3_ID);
            UnicastHeader  hdr4=msg.getHeader(UNICAST4_ID);

            if(hdr3 != null) {
                if(hdr3.type() == UnicastHeader3.ACK && hdr3.seqno() == start_drop_ack) {
                    discarding=true;
                    hdr3.seqno=start_drop_ack-1; // change 499 to 489, so A nevers gets the 499 ACK
                }
            }
            else {
                if(hdr4.type() == UnicastHeader.ACK && hdr4.seqno() == start_drop_ack) {
                    discarding=true;
                    hdr4.seqno=start_drop_ack-1; // change 499 to 489, so A nevers gets the 499 ACK
                }
            }
            return down_prot.down(msg);
        }

        public Object up(Message msg) {
            if(discarding)
                return null;
            return up_prot.up(msg);
        }

        public void up(MessageBatch batch) {
            if(discarding)
                return;
            up_prot.up(batch);
        }
    }


    protected static class MyReceiver implements Receiver {
        protected int count=0;

        public void receive(Message msg) {
            // System.out.printf("single msg from %s: %s, hdrs: %s\n", msg.src(), msg.getObject(), msg.printHeaders());
            count++;
        }

        public void receive(MessageBatch batch) {
            // System.out.printf("batch from %s: %d msgs:\n", batch.sender(), batch.size());
            for(Message ignored: batch)
                count++;
        }
    }
}
