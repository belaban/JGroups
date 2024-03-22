package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jgroups.stack.ProtocolStack.Position.ABOVE;

/**
 * Tests {@link MFC} and {@link MFC_NB}
 * @author Bela Ban
 * @since  4.2.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="create")
public class MFC_Test {
    protected static final int MAX_CREDITS=10000;
    protected static final int MSG_SIZE=1000;
    protected JChannel         a,b,c,d;


    @BeforeMethod protected void setup() throws Exception {
        a=new JChannel(Util.getTestStack()).name("A").connect("MFC_Test");
        b=new JChannel(Util.getTestStack()).name("B").connect("MFC_Test");
        c=new JChannel(Util.getTestStack()).name("C").connect("MFC_Test");
        d=new JChannel(Util.getTestStack()).name("D").connect("MFC_Test");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c,d);
    }

    @AfterMethod protected void destroy() throws Exception {
        Util.closeReverse(a,b,c,d);
    }

    @DataProvider
    protected static Object[][] create() {
        return new Object[][] {
          {MFC.class},
          {MFC_NB.class}
        };
    }


    /** A blocks threads on sending messages to C. When C is removed from the view, the threads should unblock */
    public void testBlockingAndViewChange(Class<MFC> clazz) throws Exception {
        inject(clazz, a,b,d); // C has no MFC, won't send any credits -> this will make threads in A (sending to C) block

        for(JChannel ch: Arrays.asList(a,b,d))
            send(ch, null, 9950); // uses up the 10'000 initial credits, so the next threads block on C
        if(clazz.equals(MFC_NB.class)) {
            // we send another 9K to fill the queue
            for(JChannel ch: Arrays.asList(a,b,d))
                send(ch, null, 9950); // uses up the 10'000 initial credits, so the next threads block on C
        }

        Thread[] threads=new Thread[10];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Thread(() -> send(a, null, MSG_SIZE));
            threads[i].start();
        }

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads)
                         .allMatch(t -> t.getState() == Thread.State.WAITING || t.getState() == Thread.State.TIMED_WAITING),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        // install a new view in A excluding C (this should unblock the senders):
        View v=View.create(a.getAddress(), a.getView().getViewId().getId() +1, a.getAddress(), b.getAddress(), d.getAddress());

        for(JChannel ch: Arrays.asList(a,b,d)) {
            ((GMS)ch.getProtocolStack().findProtocol(GMS.class)).installView(v);
        }

        System.out.printf("view:\n%s\n", Stream.of(a, b, c, d).map(ch -> ch.getAddress() + ": " + ch.getView()).collect(Collectors.joining("\n")));

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
    }


    /** A blocks threads on sending messages to C. When A is stopped, the threads should unblock */
    public void testBlockingAndStop(Class<MFC> clazz) throws Exception {
        inject(clazz, a,b,d); // C has no MFC, won't send any credits -> this will make threads in A (sending to C) block

        for(JChannel ch: Arrays.asList(a,b,d))
            send(ch, null, 9950); // uses up the 10'000 initial credits, so the next threads block on C
        if(clazz.equals(MFC_NB.class)) {
            // we send another 9K to fill the queue
            for(JChannel ch: Arrays.asList(a,b,d))
                send(ch, null, 9950); // uses up the 10'000 initial credits, so the next threads block on C
        }

        Thread[] threads=new Thread[10];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Thread(() -> send(a, null, MSG_SIZE));
            threads[i].start();
        }

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads)
                         .allMatch(t -> t.getState() == Thread.State.WAITING || t.getState() == Thread.State.TIMED_WAITING),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        a.close();

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        View v=View.create(b.getAddress(), b.getView().getViewId().getId() +1, b.getAddress(), d.getAddress());

        for(JChannel ch: Arrays.asList(b,d)) {
            ((GMS)ch.getProtocolStack().findProtocol(GMS.class)).installView(v);
        }
    }

    /**
     * Mimicks the following scenario: MFC is _below_ NAKACK2. Members have 2MB of credits. Every member sends 1000 1K
     * messages and then blocks on flow control.
     * </br>
     * The switch (DROP) drops messages 1-999, only message #1000 is received. This leads to xmit requests for
     * messages 1-999.
     * </br>
     * MFC drops retransmission messages (DONT_BLOCK flag set) as no credits are available. When the switch is back to
     * normal (DROP is removed), the original senders should send a CREDIT_REQUEST to the receivers, which send a
     * REPLENISH message to the senders. As a result, the senders have enough credits to send messages 1-999, which
     * stops retransmission.
     */
    public void testSwitchDiscardingMessages(Class<MFC> clazz) throws Exception {
        if(!clazz.equals(MFC.class))
            return;
        final int NUM_MSGS=1000;
        final short NAKACK_ID=ClassConfigurator.getProtocolId(NAKACK2.class);
        final short FC_ID=ClassConfigurator.getProtocolId(clazz);
        inject(clazz, NUM_MSGS * 1000, a,b,c,d);
        MyReceiver<Integer> ra=new MyReceiver<Integer>().name("A"), rb=new MyReceiver<Integer>().name("B"),
          rc=new MyReceiver<Integer>().name("C"), rd=new MyReceiver<Integer>().name("D");
        a.setReceiver(ra); b.setReceiver(rb); c.setReceiver(rc); d.setReceiver(rd);

        Predicate<Message> drop_under_1000=msg -> {
            NakAckHeader2 hdr=msg.getHeader(NAKACK_ID);
            return hdr != null && hdr.getType() == NakAckHeader2.MSG && hdr.getSeqno() < NUM_MSGS-10;
        };
        Predicate<Message> drop_credit_rsp=msg -> {
            FcHeader hdr=msg.getHeader(FC_ID);
            return hdr != null && hdr.type == FcHeader.REPLENISH;
        };

        for(JChannel ch: List.of(a, b, c, d)) {
            ProtocolStack st=ch.getProtocolStack();
            st.insertProtocol(new DROP().addDownFilter(drop_under_1000).addDownFilter(drop_credit_rsp), ABOVE, TP.class);
        }

        new Thread(() -> { // removes DROP: this needs to be done async, as one of the sends below will block on 0 credits
            Util.waitUntilTrue(3000, 500, () -> Stream.of(ra,rb,rc,rd).allMatch(r -> r.size() >= NUM_MSGS * 4));
            System.out.printf("-- receivers:\n%s\n", print(ra,rb,rc,rd));
            System.out.println("-- clearing DROP filters:");
            for(JChannel ch: List.of(a,b,c,d)) {
                DROP drop=ch.stack().findProtocol(DROP.class);
                drop.clearAllFilters();
            }
            System.out.println("-- done: waiting for messages to be received by all members:");
        }).start();

        byte[] payload=new byte[1000];
        System.out.printf("-- sending %d messages:\n", NUM_MSGS);
        for(int i=1; i <= NUM_MSGS; i++) {
            a.send(new ObjectMessage(null, payload));
            b.send(new ObjectMessage(null, payload));
            c.send(new ObjectMessage(null, payload));
            d.send(new ObjectMessage(null, payload));
        }

        Util.waitUntil(10000, 500, () -> Stream.of(ra,rb,rc,rd).allMatch(r -> r.size() >= NUM_MSGS * 4),
                       () -> print(ra,rb,rc,rd));
        System.out.printf("-- receivers:\n%s\n", print(ra,rb,rc,rd));
    }


    @SafeVarargs
    protected static String print(MyReceiver<Integer>... receivers) {
        return Stream.of(receivers).map(r -> String.format("%s: %d msgs", r.name(), r.size()))
          .collect(Collectors.joining("\n"));
    }

    protected static void inject(Class<MFC> clazz, JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            MFC mfc=clazz.getConstructor().newInstance();
            mfc.setMaxCredits(MAX_CREDITS).setMaxBlockTime(60000);
            if(mfc instanceof MFC_NB)
                ((MFC_NB)mfc).setMaxQueueSize(MAX_CREDITS);
            mfc.init();
            ProtocolStack stack=ch.getProtocolStack();
            stack.removeProtocol(MFC.class); // just in case we already have an MFC protocol
            stack.insertProtocol(mfc, ABOVE, GMS.class);
            View v=ch.getView();
            mfc.handleViewChange(v.getMembers());
        }
    }

    /** Inserts the flow control protocol *below* NAKACK2 */
    protected static void inject(Class<MFC> clazz, long max_credits, JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            MFC mfc=clazz.getConstructor().newInstance();
            mfc.setMaxCredits(max_credits).setMaxBlockTime(2000);
            if(mfc instanceof MFC_NB)
                ((MFC_NB)mfc).setMaxQueueSize((int)max_credits);
            mfc.init();
            ProtocolStack stack=ch.getProtocolStack();
            stack.removeProtocol(MFC.class); // just in case we already have an MFC protocol
            stack.insertProtocol(mfc, ProtocolStack.Position.BELOW, NAKACK2.class);
            View v=ch.getView();
            mfc.handleViewChange(v.getMembers());
        }
    }


    protected static void send(JChannel ch, Address target, int length) {
        try {
            ch.send(new BytesMessage(target, new byte[length]));
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}