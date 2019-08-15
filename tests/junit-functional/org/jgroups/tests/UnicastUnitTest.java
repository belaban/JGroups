package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests unicast functionality
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UnicastUnitTest {
    protected JChannel a, b, c, d;

    @AfterMethod  protected void tearDown() throws Exception {Util.closeReverse(a,b,c,d);}


    public void testUnicastMessageInCallbackExistingMember() throws Throwable {
        a=create("A", false); b=create("B", false);
        a.connect("UnicastUnitTest");
        MyReceiver receiver=new MyReceiver(a);
        a.setReceiver(receiver);
        b.connect("UnicastUnitTest");
        a.setReceiver(null); // so the receiver doesn't get a view change
        Throwable ex=receiver.getEx();
        if(ex != null)
            throw ex;
    }

    /** Tests sending msgs from A to B */
    // @Test(invocationCount=10)
    public void testMessagesToOther() throws Exception {
        a=create("A", false); b=create("B", false);
        _testMessagesToOther();
    }

    public void testMessagesToOtherBatching() throws Exception {
        a=create("A", true); b=create("B", true);
        _testMessagesToOther();
    }

    public void testMessagesToEverybodyElse() throws Exception {
        MyReceiver<String> r1=new MyReceiver(), r2=new MyReceiver(), r3=new MyReceiver(), r4=new MyReceiver();
        a=create("A", false);
        b=create("B", false);
        c=create("C", false);
        d=create("D", false);
        connect(a,b,c,d);

        a.setReceiver(r1);
        b.setReceiver(r2);
        c.setReceiver(r3);
        d.setReceiver(r4);

        for(JChannel sender: Arrays.asList(a,b,c,d)) {
            for(JChannel receiver: Arrays.asList(a,b,c,d)) {
                for(int i=1; i <= 5; i++) {
                    Message msg=new Message(receiver.getAddress(), String.format("%s%d", sender.getAddress(), i));
                    sender.send(msg);
                }
            }
        }

        for(int i=0; i < 10; i++) {
            if(Stream.of(r1,r2,r3,r4).allMatch(r -> r.list().size() == 20))
                break;
            Util.sleep(500);
        }

        Stream.of(r1,r2,r3,r4).forEach(r -> System.out.printf("%s\n", r.list));

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

    public void testPartition() throws Exception {
        a=create("A", false);
        b=create("B", false);
        connect(a,b);
        System.out.println("-- Creating network partition");
        Stream.of(a,b).forEach(ch -> {
            DISCARD discard=new DISCARD().setDiscardAll(true);
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
          msg(dest).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest)
        };

        MyReceiver<Integer> receiver=new MyReceiver();
        b.setReceiver(receiver);
        send(a, msgs);
        checkReception(receiver, false, 1,2,3,4,5);
    }


    // @Test(invocationCount=10)
    public void testMessagesToSelf() throws Exception {
        a=create("A", false); b=create("B", false);
        _testMessagesToSelf();
    }

    public void testMessagesToSelfBatching() throws Exception {
        a=create("A", true); b=create("B", true);
        _testMessagesToSelf();
    }

    protected void _testMessagesToSelf() throws Exception {
        connect(a,b);
        Address dest=a.getAddress();
        Message[] msgs={
          msg(dest),
          msg(dest),
          msg(dest).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL),
          msg(dest).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest),
          msg(dest).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest),
          msg(dest)
        };

        MyReceiver<Integer> receiver=new MyReceiver();
        a.setReceiver(receiver);
        send(a, msgs);
        checkReception(receiver, false, 1,2,3,5,8,9);
    }

    public void testMessagesToSelf2() throws Exception {
        a=create("A", false); b=create("B", false);
        _testMessagesToSelf2();
    }

    public void testMessagesToSelf2Batching() throws Exception {
        a=create("A", true); b=create("B", true);
        _testMessagesToSelf2();
    }

    protected void _testMessagesToSelf2() throws Exception {
        connect(a,b);
        Address dest=a.getAddress();
        Message[] msgs={
          msg(dest).setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest).setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest),
          msg(dest).setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
        };

        MyReceiver<Integer> receiver=new MyReceiver<>();
        a.setReceiver(receiver);
        send(a, msgs);
        checkReception(receiver, false, 2,5,6,10);
    }


    protected static void send(JChannel ch, Message... msgs) throws Exception {
        int cnt=1;
        for(Message msg: msgs) {
            assert msg.dest() != null;
            msg.setObject(cnt++);
            ch.send(msg);
        }
    }

    protected static void checkReception(MyReceiver<Integer> r, boolean check_order, int... num) {
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
        if(check_order)
            for(int i=0; i < num.length; i++)
                assert num[i] == received.get(i);
    }

    protected static Message msg(Address dest) {return new Message(dest);}

    protected static JChannel create(String name, boolean use_batching) throws Exception {
        Protocol[] protocols={
          new UDP().setBindAddress(Util.getLoopback()),
          new LOCAL_PING(),
          // new TEST_PING(),
          new MERGE3().setMinInterval(500).setMaxInterval(3000).setCheckInterval(4000),
          new FD_ALL().timeout(2000).interval(500).timeoutCheckInterval(700),
          new NAKACK2(),
          new MAKE_BATCH().sleepTime(100).unicasts(use_batching),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(1000),
          new FRAG2().fragSize(8000),
        };
        return new JChannel(protocols).name(name);
    }

    protected static void connect(JChannel... channels) throws Exception {
        for(JChannel ch: channels)
            ch.connect("UnicastUnitTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
    }


    protected static class MyReceiver<T> extends ReceiverAdapter {
        protected JChannel      channel;
        protected Throwable     ex;
        protected final List<T> list=new ArrayList<>();

        public           MyReceiver()            {this(null);}
        public           MyReceiver(JChannel ch) {this.channel=ch;}
        public Throwable getEx()                 {return ex;}
        public List<T>   list()                  {return list;}
        public void      clear()                 {list.clear();}

        public void receive(Message msg) {
            T obj=msg.getObject();
            synchronized(list) {
                list.add(obj);
            }
        }

      /*  public void viewAccepted(View new_view) {
            if(channel == null) return;
            Address local_addr=channel.getAddress();
            assert local_addr != null;
            System.out.println("[" + local_addr + "]: " + new_view);
            List<Address> members=new LinkedList<>(new_view.getMembers());
            assert 2 == members.size() : "members=" + members + ", local_addr=" + local_addr + ", view=" + new_view;
            Address dest=members.get(0);
            Message unicast_msg=new Message(dest);
            try {
                channel.send(unicast_msg);
            }
            catch(Throwable e) {
                ex=e;
                throw new RuntimeException(e);
            }
        }*/
    }
}