package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.DISCARD_PAYLOAD;
import org.jgroups.protocols.MAKE_BATCH;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests the NAKACK protocol for OOB and regular msgs, tests https://issues.redhat.com/browse/JGRP-379
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class NAKACK_Test extends ChannelTestBase {
    protected JChannel a, b, c;

    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        c=createChannel().name("C");
        makeUnique(a, b, c);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c, b, a);
    }


    /**
     * Tests https://issues.redhat.com/browse/JGRP-379: we send 1, 2, 3, 4(OOB) and 5 to the cluster.
     * Message with seqno 3 is discarded two times, so retransmission will make the receivers receive it *after* 4.
     * Note that OOB messages *destroys* FIFO ordering (or whatever ordering properties are set) !
     */
    public void testOutOfBandMessages() throws Exception {
        NAKACK_Test.MyReceiver receiver1=new NAKACK_Test.MyReceiver("A");
        NAKACK_Test.MyReceiver receiver2=new NAKACK_Test.MyReceiver("B");
        NAKACK_Test.MyReceiver receiver3=new NAKACK_Test.MyReceiver("C");
        a.setReceiver(receiver1);
        b.setReceiver(receiver2);
        c.setReceiver(receiver3);

        a.getProtocolStack().insertProtocol(new DISCARD_PAYLOAD(), ProtocolStack.Position.BELOW, NAKACK2.class);

        a.connect("NAKACK_Test");
        b.connect("NAKACK_Test");
        c.connect("NAKACK_Test");
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b,c);

        for(int i=1; i <=5; i++) {
            Message msg=new BytesMessage(null, i);
            if(i == 4)
                msg.setFlag(Message.Flag.OOB);
            System.out.println("-- sending message #" + i);
            a.send(msg);
            Util.sleep(100);
        }

        Collection<Integer> seqnos1=receiver1.getSeqnos();
        Collection<Integer> seqnos2=receiver2.getSeqnos();
        Collection<Integer> seqnos3=receiver3.getSeqnos();

        // wait until retransmission of seqno #3 happens, so that 4 and 5 are received as well
        long target_time=System.currentTimeMillis() + 20000;
        do {
            if(seqnos1.size() >= 5 && seqnos2.size() >= 5 && seqnos3.size() >= 5)
                break;
            Util.sleep(500);
        }
        while(target_time > System.currentTimeMillis());

        System.out.println("sequence numbers:");
        System.out.println("c1: " + seqnos1);
        System.out.println("c2: " + seqnos2);
        System.out.println("c3: " + seqnos3);
        checkOrder(seqnos1, seqnos2, seqnos3);
    }

    public void testOobBatch() throws Exception {
        NAKACK_Test.MyReceiver receiver2=new NAKACK_Test.MyReceiver("B");
        b.setReceiver(receiver2);

        MAKE_BATCH m=new MAKE_BATCH().multicasts(true).unicasts(false).skipOOB(false);
        b.getProtocolStack().insertProtocol(m, ProtocolStack.Position.BELOW, NAKACK2.class);
        m.start();

        a.connect("NAKACK_Test");
        b.connect("NAKACK_Test");
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b);

        for(int i=1; i <= 5; i++) {
            Message msg=new BytesMessage(null, i).setFlag(Message.Flag.OOB);
            a.send(msg);
        }
        Util.waitUntil(5000, 500, () -> receiver2.size() == 5);
    }

    /** Sends multicast messages on A, the disconnects and reconnects A and sends more messages. B and C should
     * receive all of A's messages. https://issues.redhat.com/browse/JGRP-2720
     */
    public void testReconnect() throws Exception {
        NAKACK_Test.MyReceiver r1=new NAKACK_Test.MyReceiver("A");
        NAKACK_Test.MyReceiver r2=new NAKACK_Test.MyReceiver("B");
        NAKACK_Test.MyReceiver r3=new NAKACK_Test.MyReceiver("C");
        a.connect("NAKACK_Test").setReceiver(r1);
        b.connect("NAKACK_Test").setReceiver(r2);
        c.connect("NAKACK_Test").setReceiver(r3);
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b,c);

        for(int i=1; i <= 10; i++)
            a.send(null, i);
        Util.waitUntil(5000, 200, () -> Stream.of(r1,r2,r3).allMatch(r -> r.size() == 10));

        System.out.println("-- disconnecting and reconnecting A:");
        a.disconnect();
        Util.sleep(1000);
        a.connect("NAKACK_Test").setReceiver(r1);

        Stream.of(r1,r2,r3).forEach(MyReceiver::clear);
        for(int i=11; i <= 20; i++)
            a.send(null, i);
        Util.waitUntil(5000, 200,
                       () -> Stream.of(r1,r2,r3).allMatch(r -> r.size() == 10),
                       () -> Stream.of(r1,r2,r3)
                         .map(r -> String.format("%s: %s", r.name(), r.getSeqnos())).collect(Collectors.joining("\n")));
        System.out.printf("-- receivers:\n%s\n",
                          Stream.of(r1,r2,r3).map(r -> String.format("%s: %s", r.name, r.seqnos))
                            .collect(Collectors.joining("\n")));
    }


    /**
     * Checks whether the numbers are in order *after* removing 4: the latter is OOB and can therefore appear anywhere
     * in the sequence
     * @param lists
     */
    @SafeVarargs
    private static void checkOrder(Collection<Integer> ... lists) throws Exception {
        for(Collection<Integer> list: lists) {
            list.remove(4);
            long prev_val=0;
            for(int val: list) {
                if(val <= prev_val)
                    throw new Exception("elements are not ordered in list: " + list);
                prev_val=val;
            }
        }
    }



    public static class MyReceiver implements Receiver {
        protected final Collection<Integer> seqnos=new ConcurrentLinkedQueue<>();
        protected final String              name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public String              name()      {return name;}
        public Collection<Integer> getSeqnos() {return seqnos;}
        public MyReceiver          clear()     {seqnos.clear(); return this;}

        public void receive(Message msg) {
            if(msg != null) {
                Integer num=msg.getObject();
                seqnos.add(num);
            }
        }

        public int size() {return seqnos.size();}
    }

}
