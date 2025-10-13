package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests {@link org.jgroups.RefcountedNioMessage}
 * @author Bela Ban
 * @since  5.5.1
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RefcountedNioMessageTest {
    protected static final int                 NUM_MSGS=50, NUM_SENDERS=10, MSG_SIZE=100, POOL_SIZE=10;
    protected final Collection<Message>        msgs=new ConcurrentLinkedQueue<>();
    protected final BlockingQueue<ByteBuffer>  pool=new ArrayBlockingQueue<>(POOL_SIZE);
    protected final static String              CLUSTER=RefcountedNioMessageTest.class.getSimpleName();
    protected JChannel                         a,b,c;
    protected Receiver                         ra,rb,rc;


    @DataProvider
    static Object[][] createUnicastProtocol() {
        return new Object[][] {
          {UNICAST3.class},
          {UNICAST4.class}
        };
    }

    @DataProvider
    static Object[][] createMulticastProtocol() {
        return new Object[][]{
          {NAKACK2.class},
          {NAKACK3.class},
          {NAKACK4.class}
        };
    }

    @BeforeMethod protected void setup() throws Exception {
        msgs.clear(); pool.clear();
        ra=new Receiver("A"); rb=new Receiver("B"); rc=new Receiver("C");
        // populate a fixed pool of POOL_SIZE elements
        for(int i=0; i < POOL_SIZE; i++)
            pool.offer(ByteBuffer.allocate(MSG_SIZE));
    }

    @AfterMethod  protected void cleanup() {
        Util.close(c,b,a);
    }

    public void testSimpleCreation() {
        RefcountedNioMessage m=new RefcountedNioMessage(null, ByteBuffer.wrap("hello".getBytes()));
        assert m.refCount() == 0;
        m.incr(); m.incr();
        assert m.refCount() == 2;
        AtomicReference<Message> ref=new AtomicReference<>();
        m.onRelease(ref::set);
        assert ref.get() == null;
        m.decr();
        m.decr();
        assert ref.get() == m;
        assert m.refCount() == 0;
        m.decr();
        assert m.refCount() == 0;
    }

    @Test(dataProvider="createUnicastProtocol")
    public void testUnicastRefcounting_A_B(Class<? extends Protocol> cl) throws Exception {
        a=createUnicast("A", cl);
        a.setReceiver(ra);
        b=createUnicast("B", cl);
        b.setReceiver(rb);
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);
        testUnicastRefcounting(a,b,rb);
    }

    @Test(dataProvider="createUnicastProtocol")
    public void testUnicastRefcounting_A_A(Class<? extends Protocol> cl) throws Exception {
        a=createUnicast("A", cl);
        a.setReceiver(ra);
        b=createUnicast("B", cl);
        b.setReceiver(rb);
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);
        testUnicastRefcounting(a,a,ra);
    }

    @Test(dataProvider="createMulticastProtocol")
    public void testMulticastRefcounting(Class<? extends Protocol> cl) throws Exception {
        a=createMulticast("A", cl);
        b=createMulticast("B", cl);
        c=createMulticast("C", cl);
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b,c);
        a.setReceiver(ra); b.setReceiver(rb); c.setReceiver(rc);

        Sender[] senders=new Sender[NUM_SENDERS];
        for(int i=0;i < senders.length; i++) {
            senders[i]=new Sender(a, pool, null, msgs);
            new Thread(senders[i], "sender-" + (i+i)).start();
        }
        Util.waitUntil(10000, 500,
                       () -> sum(ra,rb,rc) == NUM_MSGS * NUM_SENDERS * 3,
                       () -> String.format("received=%d (expected=%d)", sum(ra,rb,rc), NUM_MSGS * NUM_SENDERS * 3));
        assert msgs.size() == NUM_MSGS * NUM_SENDERS;
        assert msgs.stream().allMatch(m -> m instanceof RefcountedNioMessage);

        Util.waitUntil(10000, 500,
                       () -> msgs.stream().allMatch(m -> m.refCount() == 0));
        System.out.printf("\n*** pool size: %d, %d msgs\n", pool.size(), msgs.size());
        assert pool.size() == POOL_SIZE;
    }

    protected void testUnicastRefcounting(JChannel from, JChannel to, Receiver r) throws Exception {
        Address dest=to.getAddress();
        to.setReceiver(r);

        Sender[] senders=new Sender[NUM_SENDERS];
        for(int i=0;i < senders.length; i++) {
            senders[i]=new Sender(from, pool, dest, msgs);
            new Thread(senders[i], "sender-" + (i+i)).start();
        }
        Util.waitUntil(10000, 500,
                       () -> r.count() == NUM_MSGS * NUM_SENDERS,
                       () -> String.format("received=%d (expected=%d)", r.count(), NUM_MSGS * NUM_SENDERS));
        assert msgs.size() == NUM_MSGS * NUM_SENDERS;
        assert msgs.stream().allMatch(m -> m instanceof RefcountedNioMessage);

        Util.waitUntil(10000, 500,
                       () -> msgs.stream().allMatch(m -> m.refCount() == 0));
        assert msgs.stream().allMatch(m -> m.refCount() == 0);
        System.out.printf("\n*** pool size: %d, %d msgs\n", pool.size(), msgs.size());
        assert pool.size() == POOL_SIZE;
    }

    protected static void setAckThreshold(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            Protocol p=ch.stack().findProtocol(UNICAST3.class, ReliableUnicast.class);
            Util.invoke(p, "ackThreshold", 1);
        }
    }

    protected static JChannel createUnicast(String name, Class<? extends Protocol> unicast_cl) throws Exception {
        Protocol[] protocols=Util.getTestStack();
        for(int i=0; i < protocols.length; i++) {
            Protocol prot=protocols[i];
            if(prot instanceof UNICAST3 || prot instanceof UNICAST4) {
                if(prot.getClass().isAssignableFrom(unicast_cl))
                    break;
                protocols[i]=null;
                protocols[i]=unicast_cl.getConstructor().newInstance();
                break;
            }
        }
        JChannel ch=new JChannel(protocols).name(name);
        setAckThreshold(ch);
        return ch.connect(CLUSTER);
    }

    protected static JChannel createMulticast(String name, Class<? extends Protocol> mcast_cl) throws Exception {
        Protocol[] protocols=Util.getTestStack();
        for(int i=0; i < protocols.length; i++) {
            Protocol prot=protocols[i];
            if(prot instanceof NAKACK2 || prot instanceof ReliableMulticast) {
                if(!prot.getClass().isAssignableFrom(mcast_cl)) {
                    protocols[i]=null;
                    protocols[i]=mcast_cl.getConstructor().newInstance();
                }
                if(protocols[i] instanceof NAKACK2 || protocols[i] instanceof NAKACK3) {
                    STABLE stable=find(STABLE.class, protocols);
                    stable.setMaxBytes(500).setDesiredAverageGossip(500);
                }
                if(protocols[i] instanceof NAKACK4)
                    ((NAKACK4)protocols[i]).ackThreshold(1);
                break;
            }
        }
        JChannel ch=new JChannel(protocols).name(name);
        return ch.connect(CLUSTER);
    }

    protected static <T extends Protocol> T find(Class<? extends Protocol> cl, Protocol[] prots) {
        for(Protocol p: prots) {
            if(p.getClass().equals(cl))
                return (T)p;
        }
        throw new IllegalStateException(String.format("protocol of type %s not found", cl.getSimpleName()));
    }

    protected static String print(Receiver ... receivers) {
        return Stream.of(receivers).map(Receiver::toString).collect(Collectors.joining(", "));
    }

    protected static int sum(Receiver ... receivers) {
        return Stream.of(receivers).map(Receiver::count).mapToInt(i -> i).sum();
    }

    protected record Sender(JChannel ch, BlockingQueue<ByteBuffer> pool, Address dest,
                            Collection<Message> msgs) implements Runnable {
        @Override public void run() {
            try {
                _run();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        protected void _run() throws InterruptedException {
            Consumer<Message> reclaimer=m -> pool.offer(((NioMessage)m).getBuf());

            for(int i=0; i < NUM_MSGS; i++) {
                ByteBuffer buf=pool.take();
                Message msg=new RefcountedNioMessage(dest, buf).onRelease(reclaimer);
                msgs.add(msg);
                try {
                    ch.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected static class Receiver implements org.jgroups.Receiver {
        protected final AtomicInteger count=new AtomicInteger();

        protected Receiver(String name) {
            this.name=name;
        }

        protected int count() {return count.get();}
        protected final String name;


        @Override public void receive(MessageBatch batch) {
            count.addAndGet(batch.size());
        }

        @Override public void receive(Message msg) {
            count.incrementAndGet();
        }

        @Override
        public String toString() {
            return String.format("%s: %,d", name, count());
        }
    }
}
