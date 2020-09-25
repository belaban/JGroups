package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Tests {@link org.jgroups.RefcountedNioMessage}
 * @author Bela Ban
 * @since  5.1.0
 */
@Test(groups=Global.FUNCTIONAL)
public class RefcountedNioMessageTest {
    protected static final int                 NUM_MSGS=50, NUM_SENDERS=10, MSG_SIZE=100, POOL_SIZE=10;
    protected static final boolean             DIRECT=false;
    protected static final Collection<Message> MSGS=new ConcurrentLinkedQueue<>();
    protected final BlockingQueue<ByteBuffer>  pool=new ArrayBlockingQueue<>(POOL_SIZE);



    @BeforeMethod protected void setup() {MSGS.clear();}
    @AfterMethod protected void cleanup() {MSGS.clear(); pool.clear();}

    public void testSimpleCreation() {
        RefcountedNioMessage m=new RefcountedNioMessage(null, ByteBuffer.wrap("hello".getBytes()));
        assert m.getRefcount() == 0;
        m.incr().incr();
        assert m.getRefcount() == 2;
        AtomicReference<Message> ref=new AtomicReference<>();
        m.onRelease(ref::set);
        assert ref.get() == null;
        m.decr().decr();
        assert ref.get() == m;
        assert m.getRefcount() == 0;
        m.decr();
        assert m.getRefcount() == 0;
    }



    public void testUnicastRefcounting() throws Exception {
        try(JChannel a=new JChannel(Util.getTestStack()).name("A");
            JChannel b=new JChannel(Util.getTestStack()).name("B")) {

            UNICAST3 u=b.getProtocolStack().findProtocol(UNICAST3.class);
            u.setAckThreshold(1);

            a.connect("testUnicastRefcounting");
            b.connect("testUnicastRefcounting");
            Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b);
            Address dest=b.getAddress();

            // populate a fixed pool of POOL_SIZE elements
            for(int i=0; i < POOL_SIZE; i++)
                pool.put(DIRECT? ByteBuffer.allocateDirect(MSG_SIZE) : ByteBuffer.allocate(MSG_SIZE));

            final AtomicInteger received=new AtomicInteger();
            Receiver r=new Receiver() {
                @Override public void receive(Message msg)        {received.incrementAndGet();}
                @Override public void receive(MessageBatch batch) {received.addAndGet(batch.size());}
            };
            b.setReceiver(r);

            Sender[] senders=new Sender[NUM_SENDERS];
            for(int i=0;i < senders.length; i++) {
                senders[i]=new Sender(a, pool, dest);
                new Thread(senders[i], "sender-" + (i+i)).start();
            }
            Util.waitUntil(30000, 500,
                           () -> received.get() == NUM_MSGS * NUM_SENDERS,
                           () -> String.format("received=%d (expected=%d)", received.get(), NUM_MSGS * NUM_SENDERS));
            assert MSGS.size() == NUM_MSGS * NUM_SENDERS;
            assert MSGS.stream().allMatch(m -> m instanceof RefcountedNioMessage);

            Util.waitUntil(10000, 500,
                           () -> MSGS.stream().allMatch(m -> ((RefcountedNioMessage)m).getRefcount() == 0));
            assert MSGS.stream().allMatch(m -> ((RefcountedNioMessage)m).getRefcount() == 0);
            System.out.printf("\n*** pool size: %d, %d msgs\n", pool.size(), MSGS.size());
            assert pool.size() == POOL_SIZE;
        }
    }

    protected static final class Sender implements Runnable {
        protected final JChannel                   ch;
        protected final BlockingQueue<ByteBuffer>  pool;
        protected final Address                    dest;

        public Sender(JChannel ch, BlockingQueue<ByteBuffer> pool, Address dest) {
            this.ch=ch;
            this.pool=pool;
            this.dest=dest;
        }

        @Override public void run() {
            try {
                _run();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        protected void _run() throws InterruptedException {
            Consumer<Message> reclaimer=m -> pool.add(((NioMessage)m).getBuf());

            for(int i=0; i < NUM_MSGS; i++) {
                ByteBuffer buf=pool.take();
                Message msg=new RefcountedNioMessage(dest, buf).onRelease(reclaimer);
                MSGS.add(msg);
                try {
                    ch.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
