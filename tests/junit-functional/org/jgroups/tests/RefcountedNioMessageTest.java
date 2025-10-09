package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.ReliableMulticast;
import org.jgroups.protocols.ReliableUnicast;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.UNICAST4;
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
    protected final AtomicInteger              received=new AtomicInteger();

    protected final Receiver                   r=new Receiver() {
        @Override public void receive(Message msg)        {received.incrementAndGet();}
        @Override public void receive(MessageBatch batch) {received.addAndGet(batch.size());}
    };

    @DataProvider
    static Object[][] createUnicastProtocol() {
        return new Object[][]{
          {UNICAST3.class},
          {UNICAST4.class}
        };
    }

    @DataProvider
    static Object[][] createMulticastProtocol() {
        return new Object[][]{
          {NAKACK2.class},
          //{NAKACK3.class},
          //{NAKACK4.class}
        };
    }

    @BeforeMethod protected void setup() throws Exception {
        msgs.clear(); pool.clear();
        received.set(0);
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
        b=createUnicast("B", cl);
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);
        testUnicastRefcounting(a,b);
    }

    @Test(dataProvider="createUnicastProtocol")
    public void testUnicastRefcounting_A_A(Class<? extends Protocol> cl) throws Exception {
        a=createUnicast("A", cl);
        b=createUnicast("B", cl);
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);
        testUnicastRefcounting(a,a);
    }

    @Test(dataProvider="createMulticastProtocol")
    public void testMulticastRefcounting(Class<? extends Protocol> cl) throws Exception {
        a=createMulticast("A", cl);
        b=createMulticast("B", cl);
        c=createMulticast("C", cl);
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b,c);
        a.setReceiver(r); b.setReceiver(r); c.setReceiver(r);

        Sender[] senders=new Sender[NUM_SENDERS];
        for(int i=0;i < senders.length; i++) {
            senders[i]=new Sender(a, pool, null, msgs);
            new Thread(senders[i], "sender-" + (i+i)).start();
        }
        Util.waitUntil(10000, 500,
                       () -> received.get() == NUM_MSGS * NUM_SENDERS * 3,
                       () -> String.format("received=%d (expected=%d)", received.get(), NUM_MSGS * NUM_SENDERS * 3));
        assert msgs.size() == NUM_MSGS * NUM_SENDERS;
        assert msgs.stream().allMatch(m -> m instanceof RefcountedNioMessage);

        Util.waitUntil(10000, 500,
                       () -> msgs.stream().allMatch(m -> m.refCount() == 0));
        assert msgs.stream().allMatch(m -> m.refCount() == 0);
        System.out.printf("\n*** pool size: %d, %d msgs\n", pool.size(), msgs.size());
        assert pool.size() == POOL_SIZE;
    }

    protected void testUnicastRefcounting(JChannel from, JChannel to) throws Exception {
        Address dest=to.getAddress();
        to.setReceiver(r);

        Sender[] senders=new Sender[NUM_SENDERS];
        for(int i=0;i < senders.length; i++) {
            senders[i]=new Sender(from, pool, dest, msgs);
            new Thread(senders[i], "sender-" + (i+i)).start();
        }
        Util.waitUntil(10000, 500,
                       () -> received.get() == NUM_MSGS * NUM_SENDERS,
                       () -> String.format("received=%d (expected=%d)", received.get(), NUM_MSGS * NUM_SENDERS));
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
                if(protocols[i] instanceof NAKACK2) {
                    STABLE stable=find(STABLE.class, protocols);
                    stable.setDesiredAverageGossip(1000);
                }
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
                Message msg=new RefcountedNioMessage(dest, buf).onRelease(reclaimer); //.setFlag(Message.Flag.NO_RELIABILITY);
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
}
