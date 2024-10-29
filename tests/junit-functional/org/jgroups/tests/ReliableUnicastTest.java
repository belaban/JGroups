package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.protocols.ReliableUnicast;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST4;
import org.jgroups.protocols.UnicastHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.jgroups.util.MessageBatch.Mode.OOB;
import static org.jgroups.util.MessageBatch.Mode.REG;

/**
 * Tests {@link ReliableUnicast}, ie. methods
 * {@link ReliableUnicast#_getReceiverEntry(Address, long, boolean, short, Address)} and
 * {@link ReliableUnicast#up(MessageBatch)}
 * @author Bela Ban
 * @since  5.4
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="createUnicast")
public class ReliableUnicastTest {
    protected ReliableUnicast          unicast;
    protected UpProtocol<Integer>      up_prot;
    protected DownProtocol             down_prot;
    protected TP                       transport;
    protected TimeScheduler            timer;
    protected static final Address     DEST=Util.createRandomAddress("A");
    protected static final Address     SRC=Util.createRandomAddress("B");
    protected static final AsciiString CLUSTER=new AsciiString("cluster");

    @DataProvider
    static Object[][] createUnicast() {
        return new Object[][]{
          {UNICAST4.class}
        };
    }

    @BeforeClass
    protected void setupTimer() {
        timer=new TimeScheduler3();
    }

    @AfterClass
    protected void stopTimer() {
        timer.stop();
    }

    protected void setup(Class<? extends ReliableUnicast> unicast_cl) throws Exception {
        unicast=unicast_cl.getConstructor().newInstance();
        unicast.addr(DEST);
        up_prot=new UpProtocol<Integer>();
        down_prot=new DownProtocol();
        transport=new MockTransport().cluster(CLUSTER).addr(DEST);
        up_prot.setDownProtocol(unicast);
        unicast.setUpProtocol(up_prot);
        unicast.setDownProtocol(down_prot);
        down_prot.setUpProtocol(unicast);
        down_prot.setDownProtocol(transport);
        transport.setUpProtocol(down_prot);
        TimeService time_service=new TimeService(timer);
        unicast.timeService(time_service);
        unicast.lastSync(new ExpiryCache<>(5000));
        transport.init();
    }

    @AfterMethod
    protected void destroy() {
        unicast.stop();
        transport.stop();
        transport.destroy();
    }

    public void testGetReceiverEntryFirst(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        ReliableUnicast.ReceiverEntry entry=unicast._getReceiverEntry(DEST, 1L, true, (short)0, null);
        assert entry != null && entry.connId() == 0;
        entry=unicast._getReceiverEntry(DEST, 1L, true, (short)0, null);
        assert entry != null && entry.connId() == 0;
        assert unicast.getNumReceiveConnections() == 1;
    }

    public void testGetReceiverEntryNotFirst(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        ReliableUnicast.ReceiverEntry entry=unicast._getReceiverEntry(DEST, 2L, false, (short)0, null);
        assert entry == null;
        assert down_prot.numSendFirstReqs() == 1;
    }

    public void testGetReceiverEntryExists(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        ReliableUnicast.ReceiverEntry entry=unicast._getReceiverEntry(DEST, 1L, true, (short)1, null);
        ReliableUnicast.ReceiverEntry old=entry;
        assert entry != null && entry.connId() == 1;

        // entry exists, but this conn-ID is smaller
        entry=unicast._getReceiverEntry(DEST, 1L, true, (short)0, null);
        assert entry == null;

        // entry exists and conn-IDs match
        ReliableUnicast.ReceiverEntry e=unicast._getReceiverEntry(DEST, 2L, true, (short)1, null);
        assert e != null && e == old;

        // entry exists, but is replaced by higher conn_id
        entry=unicast._getReceiverEntry(DEST, 5L, true, (short)2, null);
        assert entry.connId() == 2;
        assert entry.buf().high() == 4;

        entry=unicast._getReceiverEntry(DEST, 10L, false, (short)3, null);
        assert entry == null;
        assert down_prot.numSendFirstReqs() == 1;
    }

    public void testBatch(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatch(false);
    }

    public void testBatchOOB(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatch(true);
    }

    public void testBatchWithFirstMissing(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithFirstMissing(false);
    }

    public void testBatchWithFirstMissingOOB(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithFirstMissing(true);
    }

    public void testBatchWithFirstMissingAndExistingMessages(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithFirstMissingAndExistingMessages(false);
    }

    public void testBatchWithFirstMissingAndExistingMessagesOOB(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithFirstMissingAndExistingMessages(true);
    }

    public void testBatchWithFirstMissingAndEmptyBatch(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithFirstMissingAndEmptyBatch(false);
    }

    public void testBatchWithFirstMissingAndEmptyBatchOOB(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithFirstMissingAndEmptyBatch(true);
    }

    public void testBatchWithDifferentConnIds(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithDifferentConnIds(false);
    }

    public void testBatchWithDifferentConnIdsOOB(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithDifferentConnIds(true);
    }

    public void testBatchWithDifferentConnIds2(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithDifferentConnIds2(false);
    }

    public void testBatchWithDifferentConnIds2OOB(Class<? extends ReliableUnicast> cl) throws Exception {
        setup(cl);
        testBatchWithDifferentConnIds2(true);
    }

    protected void testBatch(boolean oob) throws Exception {
        MessageBatch mb=create(DEST, SRC, oob, 1, 10, (short)0);
        unicast.up(mb);
        List<Integer> list=up_prot.list();
        Util.waitUntilTrue(2000, 200, () -> list.size() == 10);
        assert list.size() == 10;
        List<Integer> expected=IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList());
        assert list.equals(expected);
    }

    protected void testBatchWithFirstMissing(boolean oob) throws Exception {
        MessageBatch mb=create(DEST, SRC, oob, 1, 10, (short)0);
        mb.array().set(0, null);
        unicast.up(mb);
        List<Integer> list=up_prot.list();
        Util.waitUntilTrue(1000, 200, () -> list.size() == 9);
        assert list.isEmpty();
        // Now send the first seqno:
        mb=create(DEST, SRC, oob, 11, 10, (short)0);
        Message msg=new ObjectMessage(DEST, 1).src(SRC)
          .putHeader(unicast.getId(), UnicastHeader.createDataHeader(1L, (short)0, true));
        if(oob)
            msg.setFlag(Message.Flag.OOB);
        mb.add(msg);
        unicast.up(mb);
        Util.waitUntil(2000, 200, () -> list.size() == 20);
        List<Integer> expected=IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList());
        if(oob) {
            expected.remove(0);
            expected.add(1);
        }
        assert list.equals(expected);
    }

    protected void testBatchWithFirstMissingAndExistingMessages(boolean oob) throws Exception {
        MessageBatch mb=create(DEST, SRC, oob, 1, 10, (short)0);
        mb.array().set(0, null);
        unicast.up(mb);
        List<Integer> list=up_prot.list();
        Util.waitUntilTrue(1000, 200, () -> list.size() == 9);
        assert list.isEmpty();

        // Now send the first seqno, but also existing messages 1-10 (and new messages 11-20)
        mb=create(DEST, SRC, oob, 1, 20, (short)0);
        unicast.up(mb);
        Util.waitUntil(2000, 200, () -> list.size() == 20);
        List<Integer> expected=IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList());
        if(oob) {
            expected.remove((Object)1);
            expected.add(9, 1);
        }
        assert list.equals(expected);
    }

    protected void testBatchWithFirstMissingAndEmptyBatch(boolean oob) throws Exception {
        MessageBatch mb=create(DEST, SRC, oob, 1, 10, (short)0);
        mb.array().set(0, null);
        unicast.up(mb);
        List<Integer> list=up_prot.list();
        Util.waitUntilTrue(1000, 200, () -> list.size() == 9);
        assert list.isEmpty();

        // Now send the first seqno, but also existing messages 1-10 (and new messages 11-20)
        mb=create(DEST, SRC, oob, 1, 1, (short)0);
        unicast.up(mb);
        Util.waitUntil(2000, 200, () -> list.size() == 10);
        List<Integer> expected=IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList());
        if(oob) {
            expected.remove((Object)1);
            expected.add(9, 1);
        }
        assert list.equals(expected);
    }

    protected void testBatchWithDifferentConnIds(boolean oob) throws TimeoutException {
        MessageBatch mb=create(DEST, SRC, oob, 1, 20, (short)0);
        List<Message> buf=mb.array();
        for(int i=0; i < buf.size(); i++) {
            short conn_id=(short)Math.min(i, 10);
            ((UnicastHeader)buf.get(i).getHeader(unicast.getId())).connId(conn_id);
        }
        List<Integer> list=up_prot.list();
        unicast.up(mb);
        Util.waitUntilTrue(1000, 200, () -> list.size() == 10);
        assert list.isEmpty();

        Message msg=new ObjectMessage(DEST, 10).src(SRC)
          .putHeader(unicast.getId(), UnicastHeader.createDataHeader(10, (short)10, true));
        if(oob)
            msg.setFlag(Message.Flag.OOB);
        unicast.up(msg);
        Util.waitUntil(2000, 200, () -> list.size() == 11);
        List<Integer> expected=IntStream.rangeClosed(10, 20).boxed().collect(Collectors.toList());
        if(oob) {
            expected.remove(0);
            expected.add(10);
        }
        assert list.equals(expected) : String.format("expected %s, but got: %s", expected, list);
    }

    protected void testBatchWithDifferentConnIds2(boolean oob) throws TimeoutException {
        MessageBatch mb=new MessageBatch(20).dest(DEST).sender(SRC).setMode(oob? OOB : REG);
        short conn_id=5;
        for(int i=20; i > 0; i--) {
            if(i % 5 == 0)
                conn_id--;
            Message msg=new ObjectMessage(DEST, i).src(SRC)
              .putHeader(unicast.getId(), UnicastHeader.createDataHeader(i, conn_id, false));
            if(oob)
                msg.setFlag(Message.Flag.OOB);
            mb.add(msg);
        }
        List<Integer> list=up_prot.list();
        unicast.up(mb);
        Util.waitUntilTrue(1000, 200, () -> list.size() == 5);
        assert list.isEmpty();
        Message msg=new ObjectMessage(DEST, 16).src(SRC)
          .putHeader(unicast.getId(), UnicastHeader.createDataHeader(16, (short)4, true));
        if(oob)
            msg.setFlag(Message.Flag.OOB);
        unicast.up(msg);
        Util.waitUntilTrue(2000, 200, () -> list.size() == 5);
        List<Integer> expected=IntStream.rangeClosed(16, 20).boxed().collect(Collectors.toList());
        if(oob)
            Collections.reverse(expected);
        assert list.equals(expected) : String.format("expected %s, but got: %s", expected, list);
    }

    protected MessageBatch create(Address dest, Address sender, boolean oob, int start_seqno, int num_msgs, short conn_id) {
        MessageBatch mb=new MessageBatch(dest, sender, CLUSTER, false, oob? OOB : REG, 16);
        for(int i=start_seqno; i < start_seqno+num_msgs; i++) {
            Message msg=new ObjectMessage(dest, i).src(sender)
              .putHeader(unicast.getId(), UnicastHeader.createDataHeader(i, conn_id, i == 1));
            if(oob)
                msg.setFlag(Message.Flag.OOB);
            mb.add(msg);
        }
        return mb;
    }

    protected static class DownProtocol extends Protocol {
        protected final LongAdder num_send_first_reqs=new LongAdder();

        protected long         numSendFirstReqs() {return num_send_first_reqs.sum();}
        protected DownProtocol clear() {num_send_first_reqs.reset(); return this;}

        @Override
        public Object down(Message msg) {
            UnicastHeader hdr=msg.getHeader(up_prot.getId());
            if(hdr != null &&  hdr.type() == UnicastHeader.SEND_FIRST_SEQNO)
                num_send_first_reqs.increment();
            return null;
        }
    }

    protected static class UpProtocol<T> extends Protocol {
        protected final List<T> list=new ArrayList<>();
        protected boolean       raw_msgs;

        protected List<T>       list()                {return list;}
        protected UpProtocol<T> clear()               {list.clear(); return this;}
        public    UpProtocol<T> rawMsgs(boolean flag) {this.raw_msgs=flag; return this;}

        @Override
        public Object up(Message msg) {
            T obj=raw_msgs? (T)msg : (T)msg.getObject();
            synchronized(list) {
                list.add(obj);
            }
            return null;
        }

        @Override
        public void up(MessageBatch batch) {
            synchronized(list) {
                for(Message m: batch) {
                    T obj=raw_msgs? (T)m : (T)m.getObject();
                    list.add(obj);
                }
            }
        }
    }
}
