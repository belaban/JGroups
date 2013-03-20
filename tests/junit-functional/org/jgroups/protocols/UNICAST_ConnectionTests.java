package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

/**
 * Tests unilateral closings of UNICAST connections. The test scenarios are described in doc/design/UNICAST2.txt.
 * Some of the tests may fail occasionally until https://issues.jboss.org/browse/JGRP-1594 is fixed
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class UNICAST_ConnectionTests {
    protected JChannel   a, b;
    protected Address    a_addr, b_addr;
    protected MyReceiver r1, r2;
    protected Protocol   u1, u2;
    protected static final String CLUSTER="UNICAST_ConnectionTests";

    
    @DataProvider
    static Object[][] configProvider() {
        return new Object[][]{
          {UNICAST.class},
          {UNICAST2.class},
          {UNICAST3.class}
        };
    }

    protected void setup(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class, null);
    }

    protected void setup(Class<? extends Protocol> unicast_class, Map<String,Object> props) throws Exception {
        r1=new MyReceiver("A");
        r2=new MyReceiver("B");
        a=createChannel(unicast_class, "A");
        if(props != null) {
            Protocol prot=a.getProtocolStack().findProtocol(unicast_class);
            for(Map.Entry<String,Object> entry: props.entrySet())
                prot.setValue(entry.getKey(), entry.getValue());
        }
        a.connect(CLUSTER);
        a_addr=a.getAddress();
        a.setReceiver(r1);
        u1=a.getProtocolStack().findProtocol(unicast_class);
        b=createChannel(unicast_class, "B");
        if(props != null) {
            Protocol prot=b.getProtocolStack().findProtocol(unicast_class);
            for(Map.Entry<String,Object> entry: props.entrySet())
                prot.setValue(entry.getKey(), entry.getValue());
        }
        b.connect(CLUSTER);
        b_addr=b.getAddress();
        b.setReceiver(r2);
        u2=b.getProtocolStack().findProtocol(unicast_class);
    }


    @AfterMethod void stop() {Util.close(b, a);}


    /**
     * Tests cases #1 and #2 of UNICAST.new.txt
     * @throws Exception
     */
    @Test(dataProvider="configProvider")
    public void testRegularMessageReception(Class<? extends Protocol> unicast) throws Exception {
        setup(unicast);
        sendAndCheck(a, b_addr, 100, r2);
        sendAndCheck(b,a_addr,50,r1);
    }


    /**
     * Tests case #3 of UNICAST.new.txt
     */
    @Test(dataProvider="configProvider")
    public void testBothChannelsClosing(Class<? extends Protocol> unicast) throws Exception {
        setup(unicast);
        sendToEachOtherAndCheck(10);
        
        // now close the connections to each other
        System.out.println("==== Closing the connections on both sides");
        removeConnection(u1, b_addr);
        removeConnection(u2, a_addr);
        r1.clear(); r2.clear();

        // causes new connection establishment
        sendToEachOtherAndCheck(10);
    }


    /**
     * Scenario #4 (A closes the connection unilaterally (B keeps it open), then reopens it and sends messages)
     */
    @Test(dataProvider="configProvider")
    public void testAClosingUnilaterally(Class<? extends Protocol> unicast) throws Exception {
        setup(unicast);
        sendToEachOtherAndCheck(10);

        // now close connection on A unilaterally
        System.out.println("==== Closing the connection on A");
        removeConnection(u1, b_addr);

        // then send messages from A to B
        sendAndCheck(a, b_addr, 10, r2);
    }

    /**
     * Scenario #5 (B closes the connection unilaterally (A keeps it open), then A sends messages to B)
     */
    @Test(dataProvider="configProvider")
    public void testBClosingUnilaterally(Class<? extends Protocol> unicast) throws Exception {
        setup(unicast);
        sendToEachOtherAndCheck(10);

        // now close connection on A unilaterally
        System.out.println("==== Closing the connection on B");
        removeConnection(u2, a_addr);

        // then send messages from A to B
        sendAndCheck(a, b_addr, 10, r2);
    }


    /**
     * Scenario #6 (A closes the connection unilaterally (B keeps it open), then reopens it and sends messages,
     * but loses the first message
     */
    @Test(dataProvider="configProvider")
    public void testAClosingUnilaterallyButLosingFirstMessage(Class<? extends Protocol> unicast) throws Exception {
        setup(unicast);
        sendAndCheck(a, b_addr, 10, r2);

        // now close connection on A unilaterally
        System.out.println("==== Closing the connection on A");
        removeConnection(u1, b_addr);

        // add a Drop protocol to drop the first unicast message
        Drop drop=new Drop(true);
        a.getProtocolStack().insertProtocol(drop, ProtocolStack.BELOW,(Class<? extends Protocol>[])Util.getUnicastProtocols());

        // then send messages from A to B
        sendAndCheck(a, b_addr, 10, r2);
    }

    /** Tests concurrent reception of multiple messages with a different conn_id (https://issues.jboss.org/browse/JGRP-1347) */
    @Test(dataProvider="configProvider")
    public void testMultipleConcurrentResets(Class<? extends Protocol> unicast) throws Exception {
        setup(unicast);
        sendAndCheck(a, b_addr, 1, r2);

        // now close connection on A unilaterally
        System.out.println("==== Closing the connection on A");
        removeConnection(u1, b_addr);

        r2.clear();

        final Protocol ucast=b.getProtocolStack().findProtocol(Util.getUnicastProtocols());

        int NUM=10;

        final List<Message> msgs=new ArrayList<Message>(NUM);

        for(int i=1; i <= NUM; i++) {
            Message msg=new Message(b_addr, a_addr, i);
            Header hdr=createDataHeader(ucast, 1, (short)2, true);
            msg.putHeader(ucast.getId(), hdr);
            msgs.add(msg);
        }


        Thread[] threads=new Thread[NUM];
        final CyclicBarrier barrier=new CyclicBarrier(NUM+1);
        for(int i=0; i < NUM; i++) {
            final int index=i;
            threads[i]=new Thread() {
                public void run() {
                    try {
                        barrier.await();
                        ucast.up(new Event(Event.MSG,msgs.get(index)));
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            threads[i].start();
        }

        barrier.await();
        for(Thread thread: threads)
            thread.join();

        List<Integer> list=r2.getMessages();
        System.out.println("list = " + print(list));

        assert list.size() == 1 : "list must have 1 element but has " + list.size() + ": " + print(list);
    }

    @Test(dataProvider="configProvider")
    public void testMessageToNonExistingMember(Class<? extends Protocol> unicast) throws Exception {
        Map<String,Object> props=new HashMap<String,Object>(1);
        props.put("max_retransmit_time",5000);
        setup(unicast,props);
        Address target=Util.createRandomAddress("FakeAddress");
        a.send(target, "hello");
        Protocol prot=a.getProtocolStack().findProtocol(unicast);
        Method hasSendConnectionTo=unicast.getMethod("hasSendConnectionTo", Address.class);
        for(int i=0; i < 10; i++) {
            boolean result=(Boolean)hasSendConnectionTo.invoke(prot, target);
            if(!result)
                break;
            Util.sleep(1000);
        }
        assert !(Boolean)hasSendConnectionTo.invoke(prot, target);
    }


    protected Header createDataHeader(Protocol unicast, long seqno, short conn_id, boolean first) {
        if(unicast instanceof UNICAST)
            return UNICAST.UnicastHeader.createDataHeader(seqno,conn_id, first);
        if(unicast instanceof UNICAST2)
            return UNICAST2.Unicast2Header.createDataHeader(seqno, conn_id, first);
        if(unicast instanceof UNICAST3)
            return UNICAST3.Header.createDataHeader(seqno, conn_id, first);
        throw new IllegalArgumentException("protocol " + unicast.getClass().getSimpleName() + " needs to be " +
                                             "UNICAST, UNICAST2 or UNICAST3");
    }


    /**
     * Send num unicasts on both channels and verify the other end received them
     * @param num
     * @throws Exception
     */
    protected void sendToEachOtherAndCheck(int num) throws Exception {
        for(int i=1; i <= num; i++) {
            a.send(b_addr, i);
            b.send(a_addr, i);
        }
        List<Integer> l1=r1.getMessages();
        List<Integer> l2=r2.getMessages();
        for(int i=0; i < 10; i++) {
            if(l1.size()  == num && l2.size() == num)
                break;
            Util.sleep(500);
        }
        System.out.println("l1 = " + print(l1));
        System.out.println("l2 = " + print(l2));
        assert l1.size() == num;
        assert l2.size() == num;
    }

    protected static void sendAndCheck(JChannel channel, Address dest, int num, MyReceiver receiver) throws Exception {
        receiver.clear();
        for(int i=1; i <= num; i++)
            channel.send(dest, i);
        List<Integer> list=receiver.getMessages();
        for(int i=0; i < 20; i++) {
            if(list.size() == num)
                break;
            Util.sleep(500);
        }
        System.out.println("list = " + print(list));
        int size=list.size();
        assert size == num : "list has " + size + " elements: " + list;
    }

    protected void removeConnection(Protocol prot, Address target) {
        if(prot instanceof UNICAST) {
            UNICAST unicast=(UNICAST)prot;
            unicast.removeConnection(target);
        }
        else if(prot instanceof UNICAST2) {
            UNICAST2 unicast=(UNICAST2)prot;
            unicast.removeConnection(target);
        }
        else if(prot instanceof UNICAST3) {
            UNICAST3 unicast=(UNICAST3)prot;
            unicast.closeConnection(target);
        }
        else
            throw new IllegalArgumentException("prot (" + prot + ") needs to be UNICAST, UNICAST2 or UNICAST3");
    }


    protected static String print(List<Integer> list) {
        return Util.printListWithDelimiter(list, " ");
    }


    protected JChannel createChannel(Class<? extends Protocol> unicast_class, String name) throws Exception {
        Protocol unicast=unicast_class.newInstance();
        if(unicast instanceof UNICAST2)
            unicast.setValue("stable_interval", 1000);
        return new JChannel(new SHARED_LOOPBACK(), unicast).name(name);
    }

    protected static class MyReceiver extends ReceiverAdapter {
        final String        name;
        final List<Integer> msgs=new ArrayList<Integer>(20);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            msgs.add((Integer)msg.getObject());
        }

        public List<Integer> getMessages() { return msgs; }
        public void          clear()       {msgs.clear();}
        public int           size()        {return msgs.size();}

        public String toString() {
            return name;
        }
    }

    protected static class Drop extends Protocol {
        protected volatile boolean drop_next=false;

        protected Drop(boolean drop_next) {
            this.drop_next=drop_next;
        }

        public String getName() {
            return "Drop";
        }

        public void dropNext() {
            drop_next=true;
        }

        public Object down(Event evt) {
            if(drop_next && evt.getType() == Event.MSG) {
                drop_next=false;
                return null;
            }
            return super.down(evt);
        }
    }
}
