package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.ReliableUnicast;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST4;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests {@link org.jgroups.protocols.UNICAST4} and other subclasses of {@link org.jgroups.protocols.ReliableUnicast}
 * for (sender-)blocking operations
 * @author Bela Ban
 * @since  5.4
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ReliableUnicastBlockTest {
    protected JChannel                   a,b,c;
    protected MyReceiver<Integer>        rb, rc;
    protected static long                CONN_CLOSE_TIMEOUT=2000;
    protected static final List<Integer> EXPECTED=IntStream.rangeClosed(1,10).boxed().collect(Collectors.toList());

    @BeforeMethod
    protected void setup() throws Exception {
        a=new JChannel(Util.getTestStackNew()).name("A");
        ((UNICAST4)a.stack().findProtocol(UNICAST4.class)).capacity(5);
        ReliableUnicast u=a.stack().findProtocol(ReliableUnicast.class);
        u.setConnCloseTimeout(CONN_CLOSE_TIMEOUT);
        b=new JChannel(Util.getTestStackNew()).name("B").receiver(rb=new MyReceiver<Integer>().name("B"));
        c=new JChannel(Util.getTestStackNew()).name("C").receiver(rc=new MyReceiver<Integer>().name("C"));
        Stream.of(a,b,c).map(ch -> ch.stack().getTransport().getDiagnosticsHandler()).forEach(d -> d.setEnabled(true));
        a.connect("ReliableUnicastBlockTest");
        b.connect("ReliableUnicastBlockTest");
        c.connect("ReliableUnicastBlockTest");
        Util.waitUntilAllChannelsHaveSameView(2000, 100, a,b,c);
    }

    @AfterMethod
    protected void destroy() {
        Util.close(c,b,a);
    }

    /** Tests A sending to B and C and blocking on waiting for ACKs from B, then B leaves -> this should unblock A */
    public void testSenderBlockingAndViewChange() throws Exception {
        final Address target_b=b.address(), target_c=c.address();
        Util.shutdown(b);
        Sender sender=new Sender(a, target_b, target_c);
        sender.start(); // will block after sending 5 unicasts to B
        // Wait until sender blocks: seqno >= 5
        Util.waitUntilTrue(2000, 100, () -> sender.seqno() >= 5);

        // inject view change excluding B
        View view=View.create(a.address(), 10L, a.address(), c.address());
        System.out.printf("-- installing view %s\n", view);
        GMS gms=a.stack().findProtocol(GMS.class);
        // This closes the conn to B (state: CLOSING). When the entry to B is removed (state: CLOSED), the sender to B
        // will be unblocked and the messages to C can be sent
        gms.installView(view);

        // B dropped all messages:
        Util.waitUntilTrue(1000, 100, () -> rb.size() > 0);
        assert rb.size() == 0;

        // Waits until conn_close_timeout (2s in this test) kicks in and removes the conn to B, unblocking the sender
        Util.waitUntil(2000, 100, () -> !sender.isAlive());

        // C received all 10 messages:
        Util.waitUntil(2000, 100, () -> rc.size() == 10, () -> print(rb,rc));
        System.out.printf("-- received msgs:\n%s\n", print(rb,rc));
        assert rc.list().equals(EXPECTED);
    }

    /** A blocks sending message to B, then A is closed */
    public void testSenderBlockingAndChannelCloseA() throws Exception {
        Util.close(c);
        final Address target_b=b.address();
        Util.shutdown(b);
        Sender sender=new Sender(a, target_b);
        sender.start(); // will block
        // Wait until sender blocks: seqno >= 5
        Util.waitUntilTrue(2000, 100, () -> sender.seqno() >= 5);
        Util.close(a);
        Util.waitUntil(2000, 100, () -> !sender.isAlive());
    }

    /** A blocks sending messages to B, then B is closed */
    public void testSenderBlockingAndChannelCloseB() throws Exception {
        Util.close(c);
        final Address target_b=b.address();
        DISCARD discard=new DISCARD().discardAll(true);
        b.stack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
        Sender sender=new Sender(a, target_b);
        sender.start(); // will block
        // Wait until sender blocks: seqno >= 5
        Util.waitUntilTrue(2000, 100, () -> sender.seqno() >= 5);

        // inject view change excluding B
        View view=View.create(a.address(), 10L, a.address());
        System.out.printf("-- installing view %s\n", view);
        GMS gms=a.stack().findProtocol(GMS.class);
        gms.installView(view); // this should unblock the sender thread above

        // B dropped all messages:
        Util.waitUntilTrue(1000, 100, () -> rb.size() > 0);
        assert rb.size() == 0;
        Util.waitUntil(2000, 100, () -> !sender.isAlive());
    }

    /** A blocks sending to B. Then A's connection to B is closed (state: CLOSING), the reopened (state: OPEN).
     * A should now again be able to send messages to B (as soon as B's DISCARD has been removed). This mimicks
     * a network partition which subsequently heals */
    public void testConnectionCloseThenReopen() throws Exception {
        Util.close(c);
        ReliableUnicast u=a.stack().findProtocol(ReliableUnicast.class);
        u.setConnCloseTimeout(60_000); // to give the MergeView a chance to re-open the connection to B
        final Address target_b=b.address();
        DISCARD discard=new DISCARD().discardAll(true);
        b.stack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
        Sender sender=new Sender(a, target_b);
        sender.start(); // will block
        // Wait until sender blocks: seqno >= 5
        Util.waitUntilTrue(2000, 100, () -> sender.seqno() >= 5);

        // inject view change excluding B
        View view=View.create(a.address(), 10L, a.address());
        System.out.printf("-- installing view %s\n", view);
        GMS gms=a.stack().findProtocol(GMS.class);
        gms.installView(view);

        Util.waitUntilTrue(2000, 100, () -> !sender.isAlive());
        assert sender.isAlive();

        View view_b=View.create(b.address(), 10L, b.address());
        ViewId vid=new ViewId(a.address(), 12L);
        MergeView mv=new MergeView(vid, List.of(a.address(), b.address()), List.of(view, view_b));
        System.out.printf("-- Installing view %s\n", mv);
        gms.installView(mv);
        discard.discardAll(false);
        GMS gms_b=b.stack().findProtocol(GMS.class);
        gms_b.installView(mv);

        Util.waitUntil(2000, 100, () -> rb.size() == 10);
        assert rb.list().equals(EXPECTED);
        System.out.printf("-- rb: %s\n", print(rb));
    }

    protected static class Sender extends Thread {
        protected final JChannel  ch;
        protected final Address[] targets;
        protected int             seqno;

        protected Sender(JChannel ch, Address ... targets) {
            this.ch=ch;
            this.targets=targets;
            setName("sender-thread");
        }

        public int seqno() {
            return seqno;
        }

        public void run() {
            System.out.printf("A sending %d messages to %s\n", 10, Arrays.toString(targets));
            for(seqno=1; seqno <= 10; seqno++) {
                try {
                    for(Address target: targets) {
                        ch.send(target, seqno);
                        System.out.printf("-- sent msg #%d to %s\n", seqno, target);
                    }
                }
                catch(Exception ex) {
                    System.out.printf("-- received exception as expected: %s\n", ex);
                    break;
                }
            }
        }
    }

    @SafeVarargs
    protected static String print(MyReceiver<Integer> ... receivers) {
        return Stream.of(receivers).map(r -> String.format("%s: %s", r.name(), r.list())).collect(Collectors.joining("\n"));
    }
}
