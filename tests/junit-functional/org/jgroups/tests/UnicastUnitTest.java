package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests unicast functionality
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="create")
public class UnicastUnitTest {
    protected JChannel a, b, c, d;

    @DataProvider
    static Object[][] create() {
        return new Object[][]{
          {UNICAST3.class},
          {UNICAST4.class}
        };
    }

    @AfterMethod  protected void tearDown() throws Exception {Util.closeReverse(a,b,c,d);}


    public void testUnicastMessageInCallbackExistingMember(Class<? extends Protocol> cl) throws Throwable {
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", false, mcast_addr);
        b=create(cl, "B", false, mcast_addr);
        a.connect("UnicastUnitTest");
        MyReceiver<?> receiver=new MyReceiver<>();
        a.setReceiver(receiver);
        b.connect("UnicastUnitTest");
        a.setReceiver(null); // so the receiver doesn't get a view change
    }

    /** Tests sending msgs from A to B */
    // @Test(invocationCount=10)
    public void testMessagesToOther(Class<? extends Protocol> cl) throws Exception {
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", false, mcast_addr);
        b=create(cl, "B", false, mcast_addr);
        _testMessagesToOther();
    }

    public void testMessagesToOtherBatching(Class<? extends Protocol> cl) throws Exception {
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", true, mcast_addr);
        b=create(cl, "B", true, mcast_addr);
        _testMessagesToOther();
    }

    public void testMessagesToEverybodyElse(Class<? extends Protocol> cl) throws Exception {
        MyReceiver<String> r1=new MyReceiver<>(), r2=new MyReceiver<>(), r3=new MyReceiver<>(), r4=new MyReceiver<>();
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", false, mcast_addr);
        b=create(cl, "B", false, mcast_addr);
        c=create(cl, "C", false, mcast_addr);
        d=create(cl, "D", false, mcast_addr);
        connect(a,b,c,d);

        a.setReceiver(r1);
        b.setReceiver(r2);
        c.setReceiver(r3);
        d.setReceiver(r4);

        for(JChannel sender: Arrays.asList(a,b,c,d)) {
            for(JChannel receiver: Arrays.asList(a,b,c,d)) {
                for(int i=1; i <= 5; i++) {
                    Message msg=new BytesMessage(receiver.getAddress(), String.format("%s%d", sender.getAddress(), i));
                    sender.send(msg);
                }
            }
        }

        for(int i=0; i < 10; i++) {
            if(Stream.of(r1,r2,r3,r4).allMatch(r -> r.list().size() == 20))
                break;
            Util.sleep(500);
        }

        Stream.of(r1,r2,r3,r4).forEach(r -> System.out.printf("%s\n", r.list()));
        List<Integer> expected_list=Arrays.asList(1,2,3,4,5);
        System.out.print("Checking (per-sender) FIFO ordering of messages: ");
        Stream.of(r1,r2,r3,r4).forEach(r -> Stream.of(a, b, c, d).forEach(ch -> {
            String name=ch.getName();
            List<String> list=r.list();
            List<String> l=list.stream().filter(el -> el.startsWith(name)).collect(Collectors.toList());
            List<Integer> nl=l.stream().map(s -> s.substring(1)).map(Integer::valueOf).collect(Collectors.toList());
            assert nl.equals(expected_list) : String.format("%s: expected: %s, actual: %s", name, expected_list, nl);
        }));
        System.out.println("OK");
    }

    public void testPartition(Class<? extends Protocol> cl) throws Exception {
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", false, mcast_addr);
        b=create(cl, "B", false, mcast_addr);
        connect(a,b);
        System.out.println("-- Creating network partition");
        Stream.of(a,b).forEach(ch -> {
            DISCARD discard=new DISCARD().discardAll(true);
            try {
                ch.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        });

        for(int i=0; i < 10; i++) {
            if(Stream.of(a,b).allMatch(ch -> ch.getView().size() == 1))
                break;
            Util.sleep(1000);
        }
        Stream.of(a,b).forEach(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()));

        System.out.println("-- Removing network partition; waiting for merge");
        Stream.of(a,b).forEach(ch -> ch.getProtocolStack().removeProtocol(DISCARD.class));

        for(int i=0; i < 10; i++) {
            if(Stream.of(a,b).allMatch(ch -> ch.getView().size() == 2))
                break;
            Util.sleep(1000);
        }
        Stream.of(a,b).forEach(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()));
    }

    protected void _testMessagesToOther() throws Exception {
        connect(a,b);
        Address dest=b.getAddress();
        Message[] msgs={
          msg(dest), // reg
          msg(dest),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest)
        };

        MyReceiver<Integer> receiver=new MyReceiver<>();
        b.setReceiver(receiver);
        send(a, msgs);
        checkReception(receiver, false, 1,2,3,4,5);
        checkUnackedMessages(0, a);
    }


    // @Test(invocationCount=10)
    public void testMessagesToSelf(Class<? extends Protocol> cl) throws Exception {
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", false, mcast_addr);
        b=create(cl, "B", false, mcast_addr);
        _testMessagesToSelf();
    }

    public void testMessagesToSelfBatching(Class<? extends Protocol> cl) throws Exception {
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", true, mcast_addr);
        b=create(cl, "B", true, mcast_addr);
        _testMessagesToSelf();
    }

    protected void _testMessagesToSelf() throws Exception {
        connect(a,b);
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);
        Address dest=a.getAddress();
        Message[] msgs={
          msg(dest),
          msg(dest),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest),
          msg(dest).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest),
          msg(dest)
        };

        MyReceiver<Integer> receiver=new MyReceiver<>();
        a.setReceiver(receiver);
        send(a, msgs);
        checkReception(receiver, false, 1,2,3,5,8,9);
        checkUnackedMessages(0, a);
    }

    public void testMessagesToSelf2(Class<? extends Protocol> cl) throws Exception {
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", false, mcast_addr);
        b=create(cl, "B", false, mcast_addr);
        _testMessagesToSelf2();
    }

    public void testMessagesToSelf2Batching(Class<? extends Protocol> cl) throws Exception {
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(cl, "A", true, mcast_addr);
        b=create(cl, "B", true, mcast_addr);
        _testMessagesToSelf2();
    }

    protected void _testMessagesToSelf2() throws Exception {
        connect(a,b);
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);
        Address dest=a.getAddress();
        Message[] msgs={
          msg(dest).setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest),
          msg(dest).setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.DONT_LOOPBACK),
        };

        MyReceiver<Integer> receiver=new MyReceiver<>();
        a.setReceiver(receiver);
        send(a, msgs);
        checkReception(receiver, false, 2,5,6,10);
        checkUnackedMessages(0, a);
    }

    protected static void send(JChannel ch, Message... msgs) throws Exception {
        int cnt=1;
        for(Message msg: msgs) {
            assert msg.getDest() != null;
            msg.setObject(cnt++);
            ch.send(msg);
        }
    }

    protected static void checkReception(MyReceiver<Integer> r, boolean check_order, int... num) {
        List<Integer> received=r.list();
        Util.waitUntilTrue(3000, 500, () -> received.size() == num.length);
        List<Integer> expected=new ArrayList<>(num.length);
        for(int n: num) expected.add(n);
        System.out.println("received=" + received + ", expected=" + expected);
        assert received.size() == expected.size() : "list=" + received + ", expected=" + expected;
        assert received.containsAll(expected) : "list=" + received + ", expected=" + expected;
        if(check_order)
            for(int i=0; i < num.length; i++)
                assert num[i] == received.get(i);
    }

    protected static void checkUnackedMessages(int expected, JChannel ... channels) throws TimeoutException {
        Util.waitUntil(3000, 100,
                       () -> Stream.of(channels).map(ch -> ch.stack().findProtocol(UNICAST3.class, UNICAST4.class))
                         .map(rp -> rp instanceof UNICAST4? ((UNICAST4)rp).getNumUnackedMessages()
                           : ((UNICAST3)rp).getNumUnackedMessages())
                         .allMatch(num -> num == expected));
    }

    protected static Message msg(Address dest) {return new ObjectMessage(dest);}

    protected static JChannel create(Class<? extends Protocol> cl, String name, boolean use_batching, String mcast_addr) throws Exception {
        Protocol[] protocols={
          new UDP().setMcastGroupAddr(InetAddress.getByName(mcast_addr)).setBindAddress(Util.getLoopback()),
          new LOCAL_PING(),
          new MERGE3().setMinInterval(500).setMaxInterval(3000).setCheckInterval(4000),
          new FD_ALL3().setTimeout(2000).setInterval(500),
          new NAKACK2(),
          new MAKE_BATCH().sleepTime(100).unicasts(use_batching),
          //new UNBATCH(),
          cl.getConstructor().newInstance(),
          new STABLE(),
          new GMS().setJoinTimeout(1000),
          new FRAG2().setFragSize(8000),
        };
        return new JChannel(protocols).name(name);
    }

    protected static void connect(JChannel... channels) throws Exception {
        for(JChannel ch: channels)
            ch.connect("UnicastUnitTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
    }

}