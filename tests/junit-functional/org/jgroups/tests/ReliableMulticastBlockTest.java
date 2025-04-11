package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.ThreadPool;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests {@link org.jgroups.protocols.NAKACK4} and other subclasses of {@link org.jgroups.protocols.ReliableMulticast}
 * for (sender-)blocking operations
 * @author Bela Ban
 * @since  5.4
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ReliableMulticastBlockTest {
    protected static final int      NUM=4;
    protected JChannel[]            channels=new JChannel[NUM];
    protected MyReceiver<Integer>[] receivers=new MyReceiver[NUM];
    protected static short          NAKACK4_ID=ClassConfigurator.getProtocolId(NAKACK4.class);

    @BeforeMethod
    protected void setup() throws Exception {
        for(int i=0; i < channels.length; i++) {
            JChannel ch=new JChannel(Util.getTestStackNew());
            ((ReliableMulticast)ch.stack().findProtocol(NAKACK4.class)).setXmitInterval(500);
            String name=String.valueOf((char)('A' + i));
            receivers[i]=new MyReceiver<Integer>().name(name);
            if(i == 0) {
                NAKACK4 nak=ch.stack().findProtocol(ReliableMulticast.class);
                nak.capacity(5); // A can send 5 messages before it blocks
            }
            ch.stack().getTransport().enableDiagnostics();
            channels[i]=ch.name(name).connect("ReliableMulticastBlockTest");
            ch.receiver(receivers[i]);
        }
        Util.waitUntilAllChannelsHaveSameView(2000, 100, channels);
    }

    @AfterMethod
    protected void destroy() {
        Util.closeReverse(channels);
    }

    /** Tests A sending and blocking on waiting for ACKs from D, then D leaves -> this should unblock A */
    public void testSenderBlockingAndViewChange() throws Exception {
        DISCARD discard=new DISCARD().discardAll(true);
        channels[NUM-1].stack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
        Util.shutdown(channels[NUM-1]);
        channels[NUM-1]=null;

        Thread sender=new Thread(() -> {
            System.out.printf("A sending %d messages to all\n", 10);
            for(int i=1; i <= 10; i++) {
                try {
                    channels[0].send(null, i);
                }
                catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        sender.start(); // will block

        // inject view change excluding D
        View view=View.create(channels[0].address(), 10L, channels[0].address(), channels[1].address(), channels[2].address());
        System.out.printf("-- installing view %s\n", view);
        for(int i=0; i < NUM-1; i++) {
            GMS gms=channels[i].stack().findProtocol(GMS.class);
            gms.installView(view); // this should unblock the sender thread above
        }
        sender.join(500);

        List<Integer> expected=IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList());
        Util.waitUntil(5000, 100,
                       () -> Stream.of(receivers[0], receivers[1], receivers[2])
                         .map(MyReceiver::size).allMatch(n -> n == 10), () -> print(receivers));
        assert receivers[NUM-1].size() == 0;
        for(int i=0; i < NUM-1; i++) {
            List<Integer> actual=receivers[i].list();
            assert expected.equals(actual);
        }
        System.out.printf("received msgs:\n%s\n", print(receivers));
    }

    /** A sends messages and blocks, then A's channel is closed */
    public void testSenderBlockingAndChannelClose() throws Exception {
        for(int i=1; i < NUM; i++) {
            DISCARD discard=new DISCARD().discardAll(true);
            channels[i].stack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
            Util.shutdown(channels[i]);
        }

        Thread sender=new Thread(() -> {
            System.out.printf("A sending %d messages to all\n", 10);
            for(int i=1; i <= 10; i++) {
                try {
                    channels[0].send(null, i);
                }
                catch(Exception ex) {
                    System.out.printf("-- exception because channel was closed: %s\n", ex);
                    break;
                }
            }
        });
        sender.start(); // will block
        sender.join(1000);

        channels[0].disconnect();
        Util.waitUntilTrue(2000, 100, () -> !sender.isAlive());
        assert !sender.isAlive() : "sender should have been unblocked";
    }

    /**
      * Tests https://issues.redhat.com/browse/JGRP-2873
      */
    public void testNonBlockingMulticastSends() throws Exception {
        final Class<? extends Protocol> CLAZZ=NAKACK4.class;
        final Address target=null;
        // 3 messages were sent before (3 view changes): we need 3+13 capacity
        changeCapacity(CLAZZ, 14, channels[0]);
        insertAckDropper(CLAZZ, channels);
        // A already sent a JOIN-RSP to B, so we can only send 10 more unicasts to B before we block (capacity: 11)
        for(int i=1; i <= 10; i++)
            channels[0].send(target, i);
        sendMessages(target, 11,15);
        removeAckDropper(channels);
        assertSize(expected(1,15), receivers);
        NAKACK4 n=channels[0].stack().findProtocol(NAKACK4.class);
        long num_blockings=n.getNumBlockings();
        System.out.printf("-- num_blockings=%d\n", num_blockings);
        assert num_blockings > 0;
    }

    protected static class AckDropper extends Protocol {
        @Override
        public Object down(Message msg) {
            NakAckHeader hdr=msg.getHeader(NAKACK4_ID);
            if(hdr != null && hdr.type() == NakAckHeader.ACK)
                return null;
            return down_prot.down(msg);
        }
    }

    // messages are sent in *any order*, so we need to sort when comparing (see below in assertSize())
    protected void sendMessages(Address target, int from, int to) {
        ThreadPool thread_pool=channels[0].stack().getTransport().getThreadPool();
        for(int i=from; i <= to; i++) {
            Message msg=new ObjectMessage(target, i);
            if(i % 2 != 0) // set odd numbers to DONT_BLOCK
                msg.setFlag(Message.TransientFlag.DONT_BLOCK);
            thread_pool.execute(() -> send(channels[0], msg));
        }
    }

    protected static List<Integer> expected(int from, int to) {
        return IntStream.rangeClosed(from,to).boxed().collect(Collectors.toList());
    }

    protected static void send(JChannel ch, Message msg) {
        try {
            ch.send(msg);
        }
        catch(Exception ex) {
            System.err.printf("sending of %s failed: %s\n", msg, ex);
        }
    }

    protected static void changeCapacity(Class<? extends Protocol> cl, int new_capacity, JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            Protocol prot=ch.stack().findProtocol(cl);
            Util.invoke(prot, "changeCapacity", new_capacity);
        }
    }

    protected static void insertAckDropper(Class<? extends Protocol> cl, JChannel ... channels) throws Exception {
        for(JChannel ch: channels)
            ch.stack().insertProtocol(new AckDropper(), ProtocolStack.Position.BELOW, cl);
    }

    protected static void removeAckDropper(JChannel ... channels) {
        for(JChannel ch: channels)
            ch.stack().removeProtocol(AckDropper.class);
    }

    @SafeVarargs
    protected static void assertSize(List<Integer> expected, MyReceiver<Integer> ... receivers) throws TimeoutException {
        for(MyReceiver<Integer> r: receivers) {
            int expected_size=expected.size();
            Util.waitUntil(3000, 200,
                           () -> r.size() == expected_size && sort(r).equals(expected),
                           () -> String.format("%s: expected %s (size: %d) but got %s (size: %d)",
                                               r.name(), expected, expected_size, sort(r), r.size()));
            System.out.printf("%s: expected: %s, actual: %s\n", r.name(), expected, sort(r));
        }
    }

    protected static List<Integer> sort(MyReceiver<Integer> r) {
        List<Integer> list=new ArrayList<>(r.list());
        Collections.sort(list);
        return list;
    }

    @SafeVarargs
    protected static String print(MyReceiver<Integer> ... receivers) {
        return Stream.of(receivers).map(r -> String.format("%s: %s", r.name(), r.list()))
          .collect(Collectors.joining("\n"));
    }
}
