package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.MPING;
import org.jgroups.protocols.TCP_NIO2;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests sending of multicast messages via TCP_NIO2 from A to {A,B}
 * @author Bela Ban
 * @since  3.6.7
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class NioServerTest2 {
    protected static final int     NUM_MSGS=10000;
    protected static final int     MSG_SIZE=1000;
    protected static final int     recv_buf_size=50000, send_buf_size=10000;
    protected JChannel             a, b;
    protected MyReceiver           ra, rb;

    @BeforeMethod protected void init() throws Exception {
        a=create("A");
        a.setReceiver(ra=new MyReceiver());
        b=create("B");
        b.setReceiver(rb=new MyReceiver());
        a.connect("NioServerTest2");
        b.connect("NioServerTest2");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
    }

    @AfterMethod protected void destroy() {Util.close(b,a);}

    public void testMulticasting() throws Exception {
        for(int i=1; i<= NUM_MSGS; i++) {
            Message msg=new Message(null, new byte[MSG_SIZE], 0, MSG_SIZE);
            a.send(msg);
        }

        for(int i=0; i < 20; i++) {
            if(ra.total() >= NUM_MSGS && rb.total() >= NUM_MSGS)
                break;
            System.out.printf("A.good=%d | bad=%d, B.good=%d | bad=%d\n", ra.good(), ra.bad(), rb.good(), rb.bad());
            Util.sleep(500);
        }

        TCP_NIO2 ta=(TCP_NIO2)a.getProtocolStack().getTransport(), tb=(TCP_NIO2)b.getProtocolStack().getTransport();
        System.out.printf("A.partial_writes=%d, B.partial_writes=%d\n", ta.numPartialWrites(), tb.numPartialWrites());
        NAKACK2 na=a.getProtocolStack().findProtocol(NAKACK2.class), nb=b.getProtocolStack().findProtocol(NAKACK2.class);
        System.out.printf("A.xmit_reqs_sent|received=%d|%d, B.xmit_reqs_sent|received=%d|%d\n",
                          na.getXmitRequestsSent(), na.getXmitRequestsReceived(), nb.getXmitRequestsSent(), nb.getXmitRequestsReceived());

        System.out.printf("A.good=%d | bad=%d, B.good=%d | bad=%d\n", ra.good(), ra.bad(), rb.good(), rb.bad());
        check(ra, "A");
        check(rb, "B");
    }

    protected static void check(MyReceiver r, String name) {
        if(r.bad() > 0) {
            List<byte[]> bad_msgs=r.badMsgs();
            for(byte[] arr: bad_msgs)
                System.out.printf("bad buffer for %s: length=%d\n", name, arr.length);
            assert r.bad() == 0 : String.format("%s.good=%d | bad=%d\n", name, r.good(), r.bad());
        }
        assert r.total() == NUM_MSGS : String.format("%s.good=%d | bad=%d\n", name, r.good(), r.bad());
    }


    protected static JChannel create(String name) throws Exception {
        return new JChannel(
          new TCP_NIO2()
            .setValue("bind_addr", Util.getLoopback())
            .setValue("recv_buf_size", recv_buf_size).setValue("send_buf_size", send_buf_size),
          new MPING(),
          new NAKACK2().setValue("use_mcast_xmit", false),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(1000),
          new FRAG2()).name(name);
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected int good, bad;
        protected List<byte[]> bad_msgs=new ArrayList<>(1000);

        public int          good()    {return good;}
        public int          bad()     {return bad;}
        public int          total()   {return good+bad;}
        public List<byte[]> badMsgs() {return bad_msgs;}

        public void receive(Message msg) {
            if(msg.getLength() != MSG_SIZE) {
                bad++;
                byte[] copy=new byte[msg.getLength()];
                byte[] buf=null;
                buf=msg.getRawBuffer();
                System.arraycopy(buf, msg.getOffset(), copy, 0, copy.length);
                bad_msgs.add(copy);
            }
            else
                good++;
        }
    }
}
