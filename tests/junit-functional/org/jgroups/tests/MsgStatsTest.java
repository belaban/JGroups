package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.MsgStats;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Tests {@link org.jgroups.protocols.MsgStats}
 * @author Bela Ban
 * @since  5.3.5
 */
@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
public class MsgStatsTest {
    protected JChannel            a, b;
    protected MyReceiver          ra, rb;
    protected MsgStats            stats_a, stats_b;
    protected static final int    NUM_MSGS=1000, SIZE=1000, TOTAL_BYTES=NUM_MSGS*SIZE;
    protected static final String CLUSTER=MsgStatsTest.class.getSimpleName();

    @BeforeMethod
    protected void setup() throws Exception {
        a=new JChannel(Util.getTestStack()).name("A").connect(CLUSTER).setReceiver(ra=new MyReceiver());
        b=new JChannel(Util.getTestStack()).name("B").connect(CLUSTER).setReceiver(rb=new MyReceiver());
        Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b);
        stats_a=a.stack().getTransport().getMessageStats();
        stats_b=b.stack().getTransport().getMessageStats();
    }

    @AfterMethod
    protected void destroy() {
        Util.close(b,a);
    }

    public void testStatsMcasts() throws Exception {
        byte[] payload=new byte[SIZE];
        for(int i=0; i < NUM_MSGS; i++) {
            Message msg=new BytesMessage(null, payload);
            a.send(msg);
        }
        Util.waitUntil(2000, 100, () -> Stream.of(ra,rb).map(MyReceiver::mcasts).allMatch(n -> n >= NUM_MSGS));
        assertTrue(NUM_MSGS, TOTAL_BYTES, NUM_MSGS, TOTAL_BYTES, stats_a);
        assertTrue(0, 0, NUM_MSGS, TOTAL_BYTES, stats_b);
        assert stats_a.getNumMcastsSent() >= NUM_MSGS;
        assert stats_a.getNumMcastBytesSent() >= TOTAL_BYTES;
        assert stats_b.getNumMcastsReceived() >= NUM_MSGS;
        assert stats_b.getNumMcastBytesReceived() >= TOTAL_BYTES;
        assert stats_b.getNumBatchesReceived() > 0;
        AverageMinMax avg=stats_b.avgBatchSize();
        assert avg.getAverage() > 0;
        assert stats_a.getNumSingleMsgsSent() + stats_a.getNumBatchesSent() > 0;
    }

    public void testStatsUcasts() throws Exception {
        byte[] payload=new byte[SIZE];
        final Address dest=b.getAddress();
        for(int i=0; i < NUM_MSGS; i++) {
            Message msg=new BytesMessage(dest, payload);
            a.send(msg);
        }
        Util.waitUntil(2000, 100, () -> Stream.of(rb).map(MyReceiver::ucasts).allMatch(n -> n >= NUM_MSGS));
        assertTrue(NUM_MSGS, TOTAL_BYTES, 0, 0, stats_a);
        assertTrue(0, 0, NUM_MSGS, TOTAL_BYTES, stats_b);
        assert stats_a.getNumUcastsSent() >= NUM_MSGS;
        assert stats_a.getNumUcastBytesSent() >= TOTAL_BYTES;
        assert stats_b.getNumUcastsReceived() >= NUM_MSGS;
        assert stats_b.getNumUcastBytesReceived() >= TOTAL_BYTES;
        assert stats_b.getNumBatchesReceived() > 0;
        AverageMinMax avg=stats_b.avgBatchSize();
        assert avg.getAverage() > 0;
        assert stats_a.getNumSingleMsgsSent() + stats_a.getNumBatchesSent() > 0;
    }


    protected static void assertTrue(int sent, int sent_bytes, int received, int received_bytes, MsgStats... s) {
        for(MsgStats ms: s) {
            assert sent <= 0 || ms.getNumMsgsSent() >= sent
              : String.format("sent msgs expected: %d, actual: %d", sent, ms.getNumBytesSent());
            assert sent_bytes <= 0 || ms.getNumBytesSent() >= sent_bytes
              : String.format("sent bytes expected: %d, actual: %d", sent_bytes, ms.getNumBytesSent());
            assert received <= 0 || ms.getNumMsgsReceived() >= received
              : String.format("num received expected: %d, actual: %d", received, ms.getNumMsgsReceived());
            assert received_bytes <= 0 || ms.getNumBytesReceived() >= received_bytes
              : String.format("msgs bytes expected: %d, actual: %d", received_bytes, ms.getNumBytesReceived());
        }
    }

    protected static class MyReceiver implements Receiver {
        protected final LongAdder num_ucasts=new LongAdder(), num_mcasts=new LongAdder();
        protected final LongAdder mcast_bytes=new LongAdder(), ucast_bytes=new LongAdder();

        public int ucasts() {return (int)num_ucasts.sum();}
        public int mcasts() {return (int)num_mcasts.sum();}

        @Override
        public void receive(Message msg) {
            (msg.dest() == null? num_mcasts : num_ucasts).increment();
            (msg.dest() == null? mcast_bytes : ucast_bytes).add(msg.getLength());
        }

        @Override
        public void receive(MessageBatch batch) {
            (batch.dest() == null? num_mcasts : num_ucasts).add(batch.size());
            (batch.dest() == null? mcast_bytes : ucast_bytes).add(batch.length());
        }

        public void reset() {Stream.of(num_mcasts, num_ucasts, mcast_bytes, ucast_bytes).forEach(LongAdder::reset);}

        @Override
        public String toString() {
            return String.format("%,d received (%s)", ucasts()+mcasts(), Util.printBytes(mcast_bytes.sum() + ucast_bytes.sum()));
        }
    }
}
