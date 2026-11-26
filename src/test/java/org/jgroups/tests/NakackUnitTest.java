package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

/**
 * Tests NAKACK2 functionality, especially flag {@link Message.TransientFlag#DONT_LOOPBACK}.
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="create")
public class NakackUnitTest {
    protected JChannel   a, b;
    protected MyReceiver ra, rb;


    @AfterMethod  protected void tearDown() throws Exception {Util.close(b, a);}

    @DataProvider
    static Object[][] create() {
        return new Object[][]{
          {NAKACK2.class},
          {NAKACK3.class},
          {NAKACK4.class}
        };
    }


    // @Test(invocationCount=10)
    public void testMessagesToAllWithDontLoopback(Class<Protocol> cl) throws Exception {
        a=create(cl, "A", false); b=create(cl, "B", false);
        createReceivers();
        _testMessagesToAllWithDontLoopback();
    }

    /**
     * Tests NAKACK4: when more messages than send-buf.capacity() are sent; senders block when DONT_LOOPBACK is
     * set, probably because the local member doesn't ACK them.
     */
    public void testMessagesToAllWithDontLoopbackDontBlock(Class<Protocol> cl) throws Exception {
        if(!cl.equals(NAKACK4.class))
            return;
        int NUM_MSGS=200;
        a=createNAKACK4("A", 128);
        b=createNAKACK4("B", 128);
        createReceivers();
        connect();
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new ObjectMessage(null, i).setFlag(DONT_LOOPBACK);
            a.send(msg);
        }
        // A should receive 0 messages
        // B should receive 200 messages (from A)
        Util.waitUntil(1000, 500, () -> ra.size() == 0 && rb.size() == NUM_MSGS);
        System.out.printf("A: %d, B: %d\n", ra.size(), rb.size());
        assert ra.size() == 0 && rb.size() == NUM_MSGS;
        List<Integer> expected=IntStream.rangeClosed(1, NUM_MSGS).boxed().collect(Collectors.toList());
        assert expected.equals(rb.list);
    }

    // @Test(invocationCount=10)
    public void testMessagesToOtherBatching(Class<Protocol> cl) throws Exception {
        a=create(cl, "A", true); b=create(cl, "B", true);
        createReceivers();
        _testMessagesToAllWithDontLoopback();
    }

    protected void _testMessagesToAllWithDontLoopback() throws Exception {
        connect();
        Message[] msgs={
          msg(),
          msg().setFlag(Message.Flag.OOB),
          msg().setFlag(Message.Flag.OOB),
          msg().setFlag(Message.Flag.OOB).setFlag(DONT_LOOPBACK),
          msg().setFlag(DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB),
          msg().setFlag(Message.Flag.OOB),
          msg().setFlag(Message.Flag.OOB).setFlag(DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB).setFlag(DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB).setFlag(DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB).setFlag(DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB)
        };

        send(a, msgs);
        checkReception(ra, 1,2,3,6,7,12);
        checkReception(rb, 1,2,3,4,5,6,7,8,9,10,11,12);
    }


    protected static void send(JChannel ch, Message... msgs) throws Exception {
        int cnt=1;
        for(Message msg: msgs) {
            msg.setObject(cnt++);
            ch.send(msg);
        }
    }

    protected static void checkReception(MyReceiver r, int... num) {
        List<Integer> received=r.list();
        for(int i=0; i < 10; i++) {
            if(received.size() == num.length)
                break;
            Util.sleep(500);
        }
        List<Integer> expected=new ArrayList<>(num.length);
        for(int n: num) expected.add(n);
        System.out.println("received=" + received + ", expected=" + expected);
        assert received.size() == expected.size() : "list=" + received + ", expected=" + expected;
        assert received.containsAll(expected) : "list=" + received + ", expected=" + expected;
    }

    protected void createReceivers() {
        a.setReceiver(ra=new MyReceiver());
        b.setReceiver(rb=new MyReceiver());
    }

    protected static Message msg() {return new BytesMessage(null);}

    protected static JChannel create(Class<Protocol> cl, String name, boolean use_batching) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new MAKE_BATCH().sleepTime(100).multicasts(use_batching),
          cl.getConstructor().newInstance(),
          new UNICAST3(),
          new STABLE(),
          new GMS(),
          new FRAG2().setFragSize(8000),
        };
        return new JChannel(protocols).name(name);
    }

    protected static JChannel createNAKACK4(String name, int capacity) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new NAKACK4().capacity(capacity).sendAtomically(false),
          new UNICAST3(),
          new GMS(),
        };
        return new JChannel(protocols).name(name);
    }

    protected void connect() throws Exception {
        a.connect("UnicastUnitTest");
        b.connect("UnicastUnitTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
    }


    protected static class MyReceiver implements Receiver {
        protected final List<Integer> list=new ArrayList<>();

        public List<Integer> list() {return list;}
        public int           size() {return list.size();}

        public void receive(Message msg) {
            Integer num=msg.getObject();
            synchronized(list) {
                list.add(num);
            }
        }
    }
}