package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests unicast functionality
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UnicastUnitTest {
    protected JChannel a, b;

    @AfterMethod  protected void tearDown() throws Exception {Util.close(b, a);}


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

    protected void _testMessagesToOther() throws Exception {
        connect();
        Address dest=b.getAddress();
        Message[] msgs={
          msg(dest), // reg
          msg(dest),
          msg(dest).setFlag(Message.Flag.OOB),
          msg(dest).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg(dest)
        };

        MyReceiver receiver=new MyReceiver();
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
        connect();
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

        MyReceiver receiver=new MyReceiver();
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
        connect();
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

        MyReceiver receiver=new MyReceiver();
        a.setReceiver(receiver);
        send(a, msgs);
        checkReception(receiver, false, 2,5,6,10);
    }


    protected void send(JChannel ch, Message ... msgs) throws Exception {
        int cnt=1;
        for(Message msg: msgs) {
            assert msg.dest() != null;
            msg.setObject(cnt++);
            ch.send(msg);
        }
    }

    protected void checkReception(MyReceiver r, boolean check_order, int ... num) {
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

    protected Message msg(Address dest) {return new Message(dest);}

    protected JChannel create(String name, boolean use_batching) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING().timeout(1000),
          new NAKACK2(),
          new MAKE_BATCH().sleepTime(100).unicasts(use_batching),
          new UNICAST3(),
          new STABLE(),
          new GMS(),
          new FRAG2().fragSize(8000),
        };
        return new JChannel(protocols).name(name);
    }

    protected void connect() throws Exception {
        a.connect("UnicastUnitTest");
        b.connect("UnicastUnitTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected Channel             channel;
        protected Throwable           ex;
        protected final List<Integer> list=new ArrayList<>();

        public               MyReceiver()           {this(null);}
        public               MyReceiver(Channel ch) {this.channel=ch;}
        public Throwable     getEx()                {return ex;}
        public List<Integer> list()                 {return list;}
        public void          clear()                {list.clear();}

        public void receive(Message msg) {
            Integer num=(Integer)msg.getObject();
            synchronized(list) {
                list.add(num);
            }
            // System.out.println("<-- " + num);
        }

        public void viewAccepted(View new_view) {
            if(channel == null) return;
            Address local_addr=channel.getAddress();
            assert local_addr != null;
            System.out.println("[" + local_addr + "]: " + new_view);
            List<Address> members=new LinkedList<>(new_view.getMembers());
            assert 2 == members.size() : "members=" + members + ", local_addr=" + local_addr + ", view=" + new_view;
            Address dest=members.get(0);
            Message unicast_msg=new Message(dest, null, null);
            try {
                channel.send(unicast_msg);
            }
            catch(Throwable e) {
                ex=e;
                throw new RuntimeException(e);
            }
        }
    }
}