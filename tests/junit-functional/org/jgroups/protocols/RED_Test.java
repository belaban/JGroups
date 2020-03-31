package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Tests {@link RED}
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RED_Test {
    protected JChannel             ch;
    protected DelayBundler         bundler;
    protected RED                  red;
    protected TP                   transport;
    protected static final Address TARGET=Util.createRandomAddress("B");
    protected static final int     NUM_SENDERS=10, NUM_MSGS=1000, TOT_MSGS=NUM_SENDERS*NUM_MSGS;

    @BeforeMethod protected void setup() throws Exception {
        ch=create("A").connect(RED_Test.class.getSimpleName());
    }

    @AfterMethod protected void destroy() {Util.close(ch);}

    public void testNoMessageDrops() throws Exception {
        for(int i=1; i <= 10; i++)
            ch.send(TARGET, i);
        System.out.printf("red: %s\nbundler: %s\n", red, bundler);
        Util.waitUntil(10000, 500, () -> bundler.getSentMessages() + red.getDroppedMessages() >= 10,
                       () -> String.format("sent msgs (%d) and dropped msgs (%d) need to be >= %d",
                                           bundler.getSentMessages(), red.getDroppedMessages(), 10));
    }

    public void testMessageDrops() throws TimeoutException {
        final Thread[] senders=new Thread[NUM_SENDERS];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Thread(() -> {
                long start=System.currentTimeMillis();
                for(int j=0; j < NUM_MSGS; j++) {
                    try {
                        ch.send(TARGET, Thread.currentThread().getId() + "-" + j);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
                long time=System.currentTimeMillis()-start;
                System.out.printf("%s: sent %d messages in %d ms\n", Thread.currentThread(), NUM_MSGS, time);
            });
        }
        Stream.of(senders).parallel().forEach(Thread::start);

        Stream.of(senders).forEach(t -> {
            try {
                t.join(30000);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        });

        assert Stream.of(senders).noneMatch(Thread::isAlive);
        Util.waitUntil(10000, 500, () -> bundler.getSentMessages() + red.getDroppedMessages() >= TOT_MSGS,
                       () -> String.format("sent msgs (%d) and dropped msgs (%d) need to be >= %d",
                                           bundler.getSentMessages(), red.getDroppedMessages(), TOT_MSGS));
        System.out.printf("red: %s\nbundler: %s\n", red, bundler);
        assert red.getDroppedMessages() > 0;
    }


    protected JChannel create(String name) throws Exception {
        JChannel retval=new JChannel(Util.getTestStack()).name(name);
        red=new RED();
        transport=retval.getProtocolStack().getTransport();
        transport.setBundlerCapacity(1024);
        transport.getProtocolStack().removeProtocol(UNICAST3.class);
        retval.getProtocolStack().insertProtocolInStack(red, transport, ProtocolStack.Position.ABOVE);
        bundler=new DelayBundler();
        bundler.init(transport);
        transport.setBundler(bundler);
        ((GMS)retval.getProtocolStack().findProtocol(GMS.class)).setJoinTimeout(5);
        return retval;
    }

    protected static class DelayBundler extends TransferQueueBundler {
        protected final LongAdder     sent=new LongAdder(), single=new LongAdder(), batches=new LongAdder();
        protected final AverageMinMax avg_batch_size=new AverageMinMax();

        protected long   getSentMessages() {return sent.sum();}
        protected long   getSingle()       {return single.sum();}
        protected long   getBatches()      {return batches.sum();}
        protected double getAvgBatchSize() {return avg_batch_size.getAverage();}

        protected void sendSingleMessage(Message msg) {
            sent.increment();
            single.increment();
            Util.sleepRandom(5, 100);
        }

        protected void sendMessageList(Address dest, Address src, List<Message> list) {
            if(list != null) {
                int size=list.size();
                batches.increment();
                sent.add(size);
                avg_batch_size.add(size);
            }
            Util.sleepRandom(2, 100);
        }

        public String toString() {
            return String.format("sent=%d (single=%d, batches=%d) avg-batch=%s",
                                 getSentMessages(), getSingle(), getBatches(), avg_batch_size);
        }
    }
}
