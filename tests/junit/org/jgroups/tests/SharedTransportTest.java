package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.protocols.AbstractBasicTCP;
import org.jgroups.protocols.TCPPING;
import org.jgroups.protocols.AbstractTP;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.AbstractProtocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Tests which test the shared transport
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class SharedTransportTest extends ChannelTestBase {
    private JChannel a, b, c;
    private MyReceiver r1, r2, r3;
    static final String SINGLETON_1="singleton-1", SINGLETON_2="singleton-2";


    @AfterMethod protected void tearDown() throws Exception {Util.close(c,b,a);}


    public void testCreationNonSharedTransport() throws Exception {
        a=createChannel(true, 1, "A");
        changeGMSTimeout(500, a);
        a.connect("SharedTransportTest.testCreationNonSharedTransport");
        View view=a.getView();
        System.out.println("view = " + view);
        assert view.size() == 1;
    }


    public void testCreationOfDuplicateCluster() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        b=createSharedChannel(SINGLETON_1, "B");
        changeGMSTimeout(1000,a,b);
        a.connect("x");
        try {
            b.connect("x");
            assert false : "b should not be able to join cluster 'x' as a has already joined it";
        }
        catch(Exception ex) {
            System.out.println("b was not able to join the same cluster (\"x\") as expected");
        }
    }


    public void testView() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        b=createSharedChannel(SINGLETON_2, "B");
        a.setReceiver(new MyReceiver(SINGLETON_1));
        b.setReceiver(new MyReceiver(SINGLETON_2));
        changeGMSTimeout(1000,a,b);
        a.connect("x");
        b.connect("x");
        Util.waitUntilAllChannelsHaveSameSize(30000, 1000, a,b);
    }


    public void testView2() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        b=createSharedChannel(SINGLETON_1, "B");
        a.setReceiver(new MyReceiver("first-channel"));
        b.setReceiver(new MyReceiver("second-channel"));
        changeGMSTimeout(1000,a,b);
        a.connect("x");
        b.connect("y");

        View view=a.getView();
        assert view.size() == 1;
        view=b.getView();
        assert view.size() == 1;
    }

    /**
     * Tests http://jira.jboss.com/jira/browse/JGRP-689: with TCP or UDP.ip_mcast=false, the transport iterates through
     * the 'members' instance variable to send a group message. However, the 'members' var is the value of the last
     * view change received. If we receive multiple view changes, this leads to incorrect membership.
     * @throws Exception
     */

    public void testView3() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        b=createSharedChannel(SINGLETON_1, "B");
        c=createSharedChannel(SINGLETON_2, "C");
        r1=new MyReceiver("A::" + SINGLETON_1);
        r2=new MyReceiver("B::" + SINGLETON_1);
        r3=new MyReceiver("C::" + SINGLETON_2);
        a.setReceiver(r1);
        b.setReceiver(r2);
        c.setReceiver(r3);
        changeGMSTimeout(1000,a,b,c);
        a.connect("cluster-1");
        c.connect("cluster-1");
        Util.waitUntilAllChannelsHaveSameSize(30000, 1000, a,c);

        a.send(new Message(null, null, "msg-1"));
        c.send(new Message(null, null, "msg-2"));

        // async sending - wait until the message has been received by A and C
        for(int i=0; i < 20; i++) {
            if(r1.getList().size() == 2 && r3.getList().size() == 3)
                break;
            Util.sleep(500);
        }

        List<Message> list=r1.getList();
        assert list.size() == 2;
        list=r3.getList();
        assert list.size() == 2;

        r1.clear();
        r2.clear();
        r3.clear();
        b.connect("cluster-2");

        a.send(new Message(null, null, "msg-3"));
        b.send(new Message(null, null, "msg-4"));
        c.send(new Message(null, null, "msg-5"));
        Util.sleep(1000); // async sending - wait a little

        // printLists(r1, r2, r3);
        list=r1.getList();
        assert list.size() == 2;
        list=r2.getList();
        assert list.size() == 1;
        list=r3.getList();
        assert list.size() == 2;
    }


    public void testView4() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        r1=new MyReceiver("A::" + SINGLETON_1);
        a.setReceiver(r1);
        changeGMSTimeout(1000,a);
        a.connect("cluster-X");
        a.send(new Message(null, null, "msg-1"));

        Util.sleep(1000); // async sending - wait a little
        List<Message> list=r1.getList();
        assert list.size() == 1;

        a.send(new Message(null, null, "msg-2"));
        a.send(new Message(null, null, "msg-3"));
        a.send(new Message(null, null, "msg-4"));
        Util.sleep(1000); // async sending - wait a little

        list=r1.getList();
        assert list.size() == 4;
    }


    public void testSharedTransportAndNonsharedTransport() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        b=createChannel();
        a.setReceiver(new MyReceiver("first-channel"));
        b.setReceiver(new MyReceiver("second-channel"));
        changeGMSTimeout(1000,a,b);
        a.connect("x");
        b.connect("x");
        Util.waitUntilAllChannelsHaveSameSize(30000, 1000, a,b);
    }


    public void testCreationOfDifferentCluster() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        b=createSharedChannel(SINGLETON_2, "B");
        changeGMSTimeout(1000,a,b);
        a.connect("x");
        b.connect("x");
        Util.waitUntilAllChannelsHaveSameSize(30000, 1000, a,b);
    }



    public void testReferenceCounting() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        r1=new MyReceiver("a");
        a.setReceiver(r1);

        b=createSharedChannel(SINGLETON_1, "B");
        r2=new MyReceiver("b");
        b.setReceiver(r2);

        c=createSharedChannel(SINGLETON_1, "C");
        r3=new MyReceiver("c");
        c.setReceiver(r3);
        changeGMSTimeout(1000,a,b,c);
        a.connect("A");
        b.connect("B");
        c.connect("C");

        a.send(null, "message from A");
        b.send(null, "message from B");
        c.send(null, "message from C");

        for(int i=0; i < 60; i++) {
            if(r1.size() == 1 && r2.size() == 1 && r3.size() == 1)
                break;
            Util.sleep(500);
        }

        assert r1.size() == 1;
        assert r2.size() == 1;
        assert r3.size() == 1;
        r1.clear();
        r2.clear();
        r3.clear();

        b.disconnect();
        System.out.println("\n");
        a.send(null, "message from A");
        c.send(null, "message from C");
        for(int i=0; i < 60; i++) {
            if(r1.size() == 1 && r3.size() == 1)
                break;
            Util.sleep(500);
        }

        assert r1.size() == 1 : "size should be 1 but is " + r1.size();
        assert r3.size() == 1 : "size should be 1 but is " + r3.size();
        r1.clear();
        r3.clear();

        c.disconnect();
        System.out.println("\n");
        a.send(null, "message from a");
        for(int i=0; i < 60; i++) {
            if(r1.size() == 1)
                break;
            Util.sleep(500);
        }
        assert r1.size() == 1;
    }

    /**
     * Tests that a second channel with the same group name can be
     * created and connected once the first channel is disconnected.
     * @throws Exception
     */

    public void testSimpleReCreation() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        a.setReceiver(new MyReceiver("A"));
        changeGMSTimeout(1000,a);
        a.connect("A");
        a.disconnect();
        b=createSharedChannel(SINGLETON_1, "B");
        b.setReceiver(new MyReceiver("A'"));
        changeGMSTimeout(1000,b);
        b.connect("A");
    }

    /**
     * Tests that a second channel with the same group name can be
     * created and connected once the first channel is disconnected even
     * if 3rd channel with a different group name is still using the shared
     * transport.
     * @throws Exception
     */

    public void testCreationFollowedByDeletion() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        a.setReceiver(new MyReceiver("A"));
        a.connect("A");
        changeGMSTimeout(1000,a);
        b=createSharedChannel(SINGLETON_1, "B");
        b.setReceiver(new MyReceiver("B"));
        b.connect("B");
        changeGMSTimeout(1000,b);
        b.close();
        a.close();
    }



    public void test2ChannelsCreationFollowedByDeletion() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        a.setReceiver(new MyReceiver("A"));
        changeGMSTimeout(1000,a);
        a.connect("A");

        b=createSharedChannel(SINGLETON_2, "B");
        b.setReceiver(new MyReceiver("B"));
        changeGMSTimeout(1000,b);
        b.connect("A");

        c=createSharedChannel(SINGLETON_2, "C");
        c.setReceiver(new MyReceiver("C"));
        changeGMSTimeout(1000,c);
        c.connect("B");

        c.send(null, "hello world from C");
    }


    /**
     * Test case for https://issues.jboss.org/browse/JGRP-1356
     * @throws Exception
     */
    public void testFailedFirstChannel() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");

        AbstractTP transport=a.getProtocolStack().getTransport();
        transport.setBindPort(128); // set the bind_port to an incorrect value (< 1024), this will fail on connect()
        a.setReceiver(new MyReceiver("A"));
        try {
            a.connect("A");
        }
        catch(Exception ex) {
            System.out.println("caught exception - as expected: " + ex);
        }

        b=createSharedChannel(SINGLETON_1, "B");
        b.setReceiver(new MyReceiver("B"));

        try {
            b.connect("B");
        }
        catch(Exception ex) {
            System.out.println("caught exception - as expected: " + ex);
        }

        transport.setBindPort(0); // fix the problem by picking an ephemeral port > 1024
        b.connect("B");
    }


    public void testMulticasts() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        a.setReceiver(r1=new MyReceiver("A"));

        b=createSharedChannel(SINGLETON_1, "B");
        b.setReceiver(r2=new MyReceiver("B"));

        c=createSharedChannel(SINGLETON_2, "C");
        c.setReceiver(r3=new MyReceiver("C"));
        changeGMSTimeout(1000,a,b,c);
        a.connect("one");
        b.connect("two");
        c.connect("one");

        Util.waitUntilAllChannelsHaveSameSize(10000, 500, b);
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a,c);

        b.send(null, "hello world");
        assertSize(1, r2);
        assertSize(0, r1, r3);

        a.send(null, "first msg");
        c.send(null, "second");
        assertSize(2, r1, r3);
        assertSize(1, r2);
    }

    
    public void testReCreationWithSurvivingChannel() throws Exception {

        // Create 2 channels sharing a transport
        System.out.println("-- creating A");
        a=createSharedChannel(SINGLETON_1, "A");
        a.setReceiver(new MyReceiver("A"));
        changeGMSTimeout(1000,a);
        a.connect("A");

        System.out.println("-- creating B");
        b=createSharedChannel(SINGLETON_1, "B");
        b.setReceiver(new MyReceiver("B"));
        changeGMSTimeout(1000,b);
        b.connect("B");

        System.out.println("-- disconnecting A");
        a.disconnect();

        // a is disconnected so we should be able to create a new channel with group "A"
        System.out.println("-- creating A'");
        c=createSharedChannel(SINGLETON_1, "C");
        c.setReceiver(new MyReceiver("A'"));
        c.connect("A");
    }

    /**
     * Tests http://jira.jboss.com/jira/browse/JGRP-737
     * @throws Exception
     */
    public void testShutdownOfTimer() throws Exception {
        a=createSharedChannel(SINGLETON_1, "A");
        b=createSharedChannel(SINGLETON_1, "B");
        changeGMSTimeout(1000,a,b);
        a.connect("x");
        b.connect("y");
        TimeScheduler timer1=a.getProtocolStack().getTransport().getTimer();
        TimeScheduler timer2=b.getProtocolStack().getTransport().getTimer();

        assert timer1 == timer2;

        assert !timer1.isShutdown();
        assert !timer2.isShutdown();

        Util.sleep(500);
        b.close();

        assert !timer2.isShutdown();
        assert !timer1.isShutdown();

        a.close(); // now, reference counting reaches 0, so the timer thread pool is stopped
        assert timer2.isShutdown();
        assert timer1.isShutdown();
    }


    /** Create channels A, B and C. Close A. This will close the timer and transports threads (!), so B will
     * not be able to send messages anymore, so C will not receive any messages
     * Tests http://jira.jboss.com/jira/browse/JGRP-737 */
    public void testSendingOfMessagesAfterChannelClose() throws Exception {
        MyReceiver rec_a=new MyReceiver("A"), rec_b=new MyReceiver("B"), rec_c=new MyReceiver("C");
        a=createSharedChannel(SINGLETON_1, "A");
        a.setName("A");
        a.setReceiver(rec_a);
        changeGMSTimeout(1000,a);
        a.connect("one");

        b=createSharedChannel(SINGLETON_1, "B");
        b.setName("B");
        b.setReceiver(rec_b);
        b.connect("two");

        c=createSharedChannel(SINGLETON_2, "C");
        c.setName("C");
        c.setReceiver(rec_c);
        c.connect("two");

        b.send(null, "first");

        assertSize(1, rec_b, rec_c);
        assertSize(0, rec_a);
        a.close();

        b.send(null, "second");
        assertSize(0, rec_a);
        assertSize(2, rec_b, rec_c);
    }
    
    /**
     * Use a CountDownLatch to concurrently connect 3 channels; confirms
     * the channels connect
     * 
     * @throws Exception
     */
    public void testConcurrentCreation() throws Exception
    {
       a=createSharedChannel(SINGLETON_1, "A");
       r1=new MyReceiver("a");
       a.setReceiver(r1);

       b=createSharedChannel(SINGLETON_1, "B");
       r2=new MyReceiver("b");
       b.setReceiver(r2);

       c=createSharedChannel(SINGLETON_1, "C");
       r3=new MyReceiver("c");
       c.setReceiver(r3);
        changeGMSTimeout(1000,a,b,c);
       CountDownLatch startLatch = new CountDownLatch(1);
       CountDownLatch finishLatch = new CountDownLatch(3);
       
       ConnectTask connectA = new ConnectTask(a, "a", startLatch, finishLatch);
       Thread threadA = new Thread(connectA);
       threadA.setDaemon(true);
       threadA.start();
       
       ConnectTask connectB = new ConnectTask(b, "b", startLatch, finishLatch);
       Thread threadB = new Thread(connectB);
       threadB.setDaemon(true);
       threadB.start();
       
       ConnectTask connectC = new ConnectTask(c, "c", startLatch, finishLatch);
       Thread threadC = new Thread(connectC);
       threadC.setDaemon(true);
       threadC.start();
       
       startLatch.countDown();
       
       try
       {
          boolean finished = finishLatch.await(20, TimeUnit.SECONDS);
          
          if (connectA.exception != null)
          {
             AssertJUnit.fail("connectA threw exception " + connectA.exception);
          }
          if (connectB.exception != null)
          {
             AssertJUnit.fail("connectB threw exception " + connectB.exception);
          }
          if (connectC.exception != null)
          {
             AssertJUnit.fail("connectC threw exception " + connectC.exception);
          }
          
          if (!finished) {
             if (threadA.isAlive())
                AssertJUnit.fail("threadA did not finish");
             if (threadB.isAlive())
                AssertJUnit.fail("threadB did not finish");
             if (threadC.isAlive())
                AssertJUnit.fail("threadC did not finish");
          }
       }
       finally
       {
          if (threadA.isAlive())
             threadA.interrupt();
          if (threadB.isAlive())
             threadB.interrupt();
          if (threadC.isAlive())
             threadC.interrupt();
       }
    }

    private static void assertSize(int expected, MyReceiver... receivers) {
        for(int i=0; i < 10; i++) {
            boolean all_ok=true;
            for(MyReceiver receiver: receivers) {
                if(receiver.size() != expected) {
                    all_ok=false;
                    break;
                }
            }
            if(all_ok)
                break;
            else
                Util.sleep(500);
        }

        for(MyReceiver recv: receivers)
            assertEquals(expected, recv.size());
    }

    private JChannel createSharedChannel(String singleton_name, String name) throws Exception {
        ProtocolStackConfigurator config=ConfiguratorFactory.getStackConfigurator(channel_conf);
        List<ProtocolConfiguration> protocols=config.getProtocolStack();
        ProtocolConfiguration transport=protocols.get(0);
        transport.getProperties().put(Global.SINGLETON_NAME, singleton_name);
        JChannel ch=new JChannel(config);
        if(name != null)
            ch.setName(name);
        return ch;
    }


    protected static void makeUnique(AbstractChannel channel, int num) throws Exception {
        ProtocolStack stack=channel.getProtocolStack();
        AbstractTP transport=stack.getTransport();
        InetAddress bind_addr=transport.getBindAddress();

        if(transport instanceof UDP) {
            String mcast_addr=ResourceManager.getNextMulticastAddress();
            short mcast_port=ResourceManager.getNextMulticastPort(bind_addr);
            ((UDP)transport).setMulticastAddress(InetAddress.getByName(mcast_addr));
            ((UDP)transport).setMulticastPort(mcast_port);
        }
        else if(transport instanceof AbstractBasicTCP) {
            List<Short> ports=ResourceManager.getNextTcpPorts(bind_addr, num);
            transport.setBindPort(ports.get(0));
            transport.setPortRange(num);

            AbstractProtocol ping=stack.findProtocol(TCPPING.class);
            if(ping == null)
                throw new IllegalStateException("TCP stack must consist of TCP:TCPPING - other config are not supported");

            List<String> initial_hosts=new LinkedList<>();
            for(short port: ports) {
                initial_hosts.add(bind_addr + "[" + port + "]");
            }
            String tmp=Util.printListWithDelimiter(initial_hosts, ",");
            List<PhysicalAddress> init_hosts = Util.parseCommaDelimitedHosts(tmp, 1) ;
            ((TCPPING)ping).setInitialHosts(init_hosts) ;
        }
        else {
            throw new IllegalStateException("Only UDP and TCP are supported as transport protocols");
        }
    }


    protected static void changeGMSTimeout(long timeout, JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.joinTimeout(timeout);
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        final List<Message> list=new LinkedList<>();
        final String name;

        private MyReceiver(String name) {
            this.name=name;
        }

        public List<Message> getList() {
            return list;
        }

        public int size() {
            return list.size();
        }

        public void clear() {
            list.clear();
        }

        public void receive(Message msg) {
            System.out.println("[" + name + "]: received message from " + msg.getSrc() + ": " + msg.getObject());
            list.add(msg);
        }

        public void viewAccepted(View new_view) {
            System.out.println("[" + name + "]: view = " + new_view);
        }

        public String toString() {
            return super.toString() + " (size=" + list.size() + ")";
        }
    }
    
    private static class ConnectTask implements Runnable
    {
       private final AbstractChannel channel;
       private final String clusterName;
       private final CountDownLatch startLatch;
       private final CountDownLatch finishLatch;
       private Exception exception;
       
       ConnectTask(AbstractChannel channel, String clusterName, CountDownLatch startLatch, CountDownLatch finishLatch)
       {
          this.channel = channel;
          this.clusterName = clusterName;
          this.startLatch = startLatch;
          this.finishLatch = finishLatch;
       }
       
       public void run()
       {          
          try
          {
             startLatch.await();
             channel.connect(clusterName);
          }
          catch (Exception e)
          {
             e.printStackTrace(System.out);
             this.exception = e;
          }
          finally
          {
             finishLatch.countDown();
          }
         
       }
       
    }

}
