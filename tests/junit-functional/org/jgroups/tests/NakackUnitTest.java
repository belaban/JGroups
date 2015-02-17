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
import java.util.List;

/**
 * Tests NAKACK2 functionality, especially flag {@link org.jgroups.Message.TransientFlag#DONT_LOOPBACK}.
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=false)
public class NakackUnitTest {
    protected JChannel   a, b;
    protected MyReceiver ra, rb;


    @AfterMethod  protected void tearDown() throws Exception {Util.close(b, a);}

    // @Test(invocationCount=10)
    public void testMessagesToAllWithDontLoopback() throws Exception {
        a=create("A", false); b=create("B", false);
        createReceivers();
        _testMessagesToAllWithDontLoopback();
    }

    // @Test(invocationCount=10)
    public void testMessagesToOtherBatching() throws Exception {
        a=create("A", true); b=create("B", true);
        createReceivers();
        _testMessagesToAllWithDontLoopback();
    }

    protected void _testMessagesToAllWithDontLoopback() throws Exception {
        connect();
        Message[] msgs={
          msg(),
          msg().setFlag(Message.Flag.OOB),
          msg().setFlag(Message.Flag.OOB),
          msg().setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg().setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB),
          msg().setFlag(Message.Flag.OOB),
          msg().setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK),
          msg().setFlag(Message.Flag.OOB)
        };

        send(a, msgs);
        checkReception(ra, 1,2,3,6,7,12);
        checkReception(rb, 1,2,3,4,5,6,7,8,9,10,11,12);
    }


    protected void send(JChannel ch, Message ... msgs) throws Exception {
        int cnt=1;
        for(Message msg: msgs) {
            msg.setObject(cnt++);
            ch.send(msg);
        }
    }

    protected void checkReception(MyReceiver r, int ... num) {
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

    protected Message msg() {return new Message(null);}

    protected JChannel create(String name, boolean use_batching) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING().timeout(1000),
          new MAKE_BATCH().sleepTime(100).multicasts(use_batching),
          new NAKACK2(),
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
        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, a, b);
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected final List<Integer> list=new ArrayList<>();

        public List<Integer> list()       {return list;}

        public void receive(Message msg) {
            Integer num=(Integer)msg.getObject();
            synchronized(list) {
                list.add(num);
            }
        }
    }
}