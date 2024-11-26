package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.NAKACK4;
import org.jgroups.protocols.ReliableMulticast;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
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

    @SafeVarargs
    protected static String print(MyReceiver<Integer> ... receivers) {
        return Stream.of(receivers).map(r -> String.format("%s: %s", r.name(), r.list())).collect(Collectors.joining("\n"));
    }
}
