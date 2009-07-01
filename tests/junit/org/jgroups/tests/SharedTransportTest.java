package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolData;
import org.jgroups.conf.ProtocolParameter;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.util.Util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Tests which test the shared transport
 * @author Bela Ban
 * @version $Id: SharedTransportTest.java,v 1.5.2.10 2009/07/01 13:43:10 vlada Exp $
 */
public class SharedTransportTest extends ChannelTestBase {
    private JChannel a, b, c;
    private MyReceiver r1, r2, r3;
    static final String SINGLETON_1="singleton-1", SINGLETON_2="singleton-2";


    protected void tearDown() throws Exception {
        if(c != null)
            c.close();
        if(b != null)
            b.close();
        if(a != null)
            a.close();
        r1=r2=r3=null;
        super.tearDown();
    }


    public void testCreationNonSharedTransport() throws Exception {
        a=createChannel();
        a.connect("x");
        View view=a.getView();
        System.out.println("view = " + view);
        assertEquals(1, view.size());
    }

    public void testCreationOfDuplicateCluster() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_1);
        a.connect("x");
        try {
            b.connect("x");
            fail("b should not be able to join cluster 'x' as a has already joined it");
        }
        catch(Exception ex) {
            System.out.println("b was not able to join the same cluster (\"x\") as expected");
        }
    }
    
    /** 
     * https://jira.jboss.org/jira/browse/JGRP-1006
     * @throws Exception
     */
   public void testShunReconnect() throws Exception {
      int size = 10;
      JChannel chs[] = new JChannel[size];
      for (int i = 0; i < size; i++) {
         chs[i] = createSharedChannel(SINGLETON_1);
         chs[i].connect("" + i);
      }

      for (JChannel c : chs) {
         c.up(new Event(Event.EXIT));
      }

      Util.sleep(5000);
      for (JChannel c : chs) {
         assertEquals(1, c.getView().size());
      }
   }

    public void testView() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_2);
        a.setReceiver(new MyReceiver(SINGLETON_1));
        b.setReceiver(new MyReceiver(SINGLETON_2));

        a.connect("x");
        b.connect("x");

        View view=a.getView();
        assertEquals(2, view.size());
        view=b.getView();
        assertEquals(2, view.size());
    }

    public void testView2() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_1);
        a.setReceiver(new MyReceiver("first-channel"));
        b.setReceiver(new MyReceiver("second-channel"));

        a.connect("x");
        b.connect("y");

        View view=a.getView();
        assertEquals(1, view.size());
        view=b.getView();
        assertEquals(1, view.size());
    }

    /** Tests http://jira.jboss.com/jira/browse/JGRP-689: with TCP or UDP.ip_mcast=false, the transport iterates through
     * the 'members' instance variable to send a group message. However, the 'members' var is the value of the last
     * view change received. If we receive multiple view changes, this leads to incorrect membership.
     *
     * @throws Exception
     */
    public void testView3() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_1);
        c=createSharedChannel(SINGLETON_2);
        r1=new MyReceiver("A::" + SINGLETON_1); r2=new MyReceiver("B::" + SINGLETON_1); r3=new MyReceiver("C::" + SINGLETON_2);
        a.setReceiver(r1);
        b.setReceiver(r2);
        c.setReceiver(r3);

        a.connect("a");
        c.connect("a");

        View view=a.getView();
        assertEquals(2, view.size());
        view=c.getView();
        assertEquals(2, view.size());

        a.send(new Message(null, null, "msg-1"));
        c.send(new Message(null, null, "msg-2"));

        Util.sleep(1000); // async sending - wait a little
        List<Message> list=r1.getList();
        assertEquals(2, list.size());
        list=r3.getList();
        assertEquals(2, list.size());

        r1.clear(); r2.clear(); r3.clear();
        b.connect("b");

        a.send(new Message(null, null, "msg-3"));
        b.send(new Message(null, null, "msg-4"));
        c.send(new Message(null, null, "msg-5"));
        Util.sleep(1000); // async sending - wait a little

        // printLists(r1, r2, r3);
        list=r1.getList();
        assertEquals(2, list.size());
        list=r2.getList();
        assertEquals(1, list.size());
        list=r3.getList();
        assertEquals(2, list.size());
    }

    private static void  printLists(MyReceiver... receivers) {
        StringBuilder sb=new StringBuilder();
        int cnt=1;
        for(MyReceiver rec: receivers) {
            List<Message> list=rec.getList();
            sb.append("receiver #" + cnt++).append(":\n");
            for(Message msg: list) {
                sb.append(msg.getObject()).append("\n");
            }
        }
        System.out.println(sb);
    }


    public void testSharedTransportAndNonsharedTransport() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createChannel();
        a.setReceiver(new MyReceiver("first-channel"));
        b.setReceiver(new MyReceiver("second-channel"));

        a.connect("x");
        b.connect("x");

        View view=a.getView();
        assertEquals(2, view.size());
        view=b.getView();
        assertEquals(2, view.size());
    }

    public void testCreationOfDifferentCluster() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_2);
        a.connect("x");
        b.connect("x");
        View view=b.getView();
        System.out.println("b's view is " + view);
        assertEquals(2, view.size());
    }


    public void testReferenceCounting() throws ChannelException {
        a=createSharedChannel(SINGLETON_1);
        r1=new MyReceiver("a");
        a.setReceiver(r1);

        b=createSharedChannel(SINGLETON_1);
        r2=new MyReceiver("b");
        b.setReceiver(r2);

        c=createSharedChannel(SINGLETON_1);
        r3=new MyReceiver("c");
        c.setReceiver(r3);

        a.connect("A");
        b.connect("B");
        c.connect("C");

        a.send(null, null, "message from a");
        b.send(null, null, "message from b");
        c.send(null, null, "message from c");
        Util.sleep(500);
        assertEquals(1, r1.size());
        assertEquals(1, r2.size());
        assertEquals(1, r3.size());
        r1.clear(); r2.clear(); r3.clear();

        b.disconnect();
        System.out.println("\n");
        a.send(null, null, "message from a");
        c.send(null, null, "message from c");
        Util.sleep(500);
        assertEquals(1, r1.size());
        assertEquals(1, r3.size());
        r1.clear(); r3.clear();

        c.disconnect();
        System.out.println("\n");
        a.send(null, null, "message from a");
        Util.sleep(500);
        assertEquals(1, r1.size());
    }
    
    /**
     * Tests that a second channel with the same group name can be
     * created and connected once the first channel is disconnected.
     *
     * @throws Exception
     */
    public void testSimpleReCreation() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        a.setReceiver(new MyReceiver("A"));
        a.connect("A");
        a.disconnect();
        b=createSharedChannel(SINGLETON_1);
        b.setReceiver(new MyReceiver("A'"));
        b.connect("A");
    }

    /**
     * Tests that a second channel with the same group name can be
     * created and connected once the first channel is disconnected even
     * if 3rd channel with a different group name is still using the shared
     * transport.
     *
     * @throws Exception
     */
    public void testCreationFollowedByDeletion() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        a.setReceiver(new MyReceiver("A"));
        a.connect("A");

        b=createSharedChannel(SINGLETON_1);
        b.setReceiver(new MyReceiver("B"));
        b.connect("B");

        b.close();
        a.close();
    }


     public void test2ChannelsCreationFollowedByDeletion() throws Exception {
         a=createSharedChannel(SINGLETON_1);
         a.setReceiver(new MyReceiver("A"));
         a.connect("A");

         b=createSharedChannel(SINGLETON_2);
         b.setReceiver(new MyReceiver("B"));
         b.connect("A");

         c=createSharedChannel(SINGLETON_2);
         c.setReceiver(new MyReceiver("C"));
         c.connect("B");

         c.send(null, null, "hello world from C");
    }

 

    public void testReCreationWithSurvivingChannel() throws Exception {

        // Create 2 channels sharing a transport
        System.out.println("-- creating A");
        a=createSharedChannel(SINGLETON_1);
        a.setReceiver(new MyReceiver("A"));
        a.connect("A");

        System.out.println("-- creating B");
        b=createSharedChannel(SINGLETON_1);
        b.setReceiver(new MyReceiver("B"));
        b.connect("B");

        System.out.println("-- disconnecting A");
        a.disconnect();

        // a is disconnected so we should be able to create a new channel with group "A"
        System.out.println("-- creating A'");
        c=createSharedChannel(SINGLETON_1);
        c.setReceiver(new MyReceiver("A'"));
        c.connect("A");
    }


    /** Create channels A, B and C. Close A. This will close the timer and transports threads (!), so B will
     * not be able to send messages anymore, so C will not receive any messages 
     * Tests http://jira.jboss.com/jira/browse/JGRP-737 */
    public void testSendingOfMessagesAfterChannelClose() throws ChannelException {
        MyReceiver rec_a=new MyReceiver("A"), rec_b=new MyReceiver("B"), rec_c=new MyReceiver("C");
        System.out.println("-- creating A");
        a=createSharedChannel(SINGLETON_1);
        a.setReceiver(rec_a);
        a.connect("A");

        System.out.println("-- creating B");
        b=createSharedChannel(SINGLETON_1);
        b.setReceiver(rec_b);
        b.connect("B");

        System.out.println("-- creating C");
        c=createSharedChannel(SINGLETON_2);
        c.setReceiver(rec_c);
        c.connect("B");

        b.send(null, null, "first");
        Util.sleep(500); // msg delivery is asynchronous, so give members some time to receive the msg (incl retransmission)
        assertSize(1, rec_b, rec_c);
        assertSize(0, rec_a);
        a.close();

        b.send(null, null, "second");
        Util.sleep(500);
        assertSize(0, rec_a);
        assertSize(2, rec_b, rec_c);
    }
    
    public void testConcurrentCreation() throws ChannelException, InterruptedException
    {
       a=createSharedChannel(SINGLETON_1);
       r1=new MyReceiver("a");
       a.setReceiver(r1);

       b=createSharedChannel(SINGLETON_1);
       r2=new MyReceiver("b");
       b.setReceiver(r2);

       c=createSharedChannel(SINGLETON_1);
       r3=new MyReceiver("c");
       c.setReceiver(r3);
       
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
             fail("connectA threw exception " + connectA.exception);
          }
          if (connectB.exception != null)
          {
             fail("connectB threw exception " + connectB.exception);
          }
          if (connectC.exception != null)
          {
             fail("connectC threw exception " + connectC.exception);
          }
          
          if (!finished) {
             if (threadA.isAlive())
                fail("threadA did not finish");
             if (threadB.isAlive())
                fail("threadB did not finish");
             if (threadC.isAlive())
                fail("threadC did not finish");
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
        for(MyReceiver recv: receivers) {
            assertEquals(expected, recv.size());
        }
    }



    private static JChannel createSharedChannel(String singleton_name) throws ChannelException {
        ProtocolStackConfigurator config=ConfiguratorFactory.getStackConfigurator(CHANNEL_CONFIG);
        ProtocolData[] protocols=config.getProtocolStack();
        ProtocolData transport=protocols[0];
        transport.getParameters().put(Global.SINGLETON_NAME, new ProtocolParameter(Global.SINGLETON_NAME, singleton_name));
        return new JChannel(config);
    }


    private static class MyReceiver extends ReceiverAdapter {
        final List<Message> list=new LinkedList<Message>();
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
            StringBuilder sb=new StringBuilder();
            sb.append("[" + name + "]: view = " + new_view);
            System.out.println(sb);
        }

        public String toString() {
            return size() + " message(s)";
        }
    }
    
    private static class ConnectTask implements Runnable
    {
       private final Channel channel;
       private final String clusterName;
       private final CountDownLatch startLatch;
       private final CountDownLatch finishLatch;
       private Exception exception;
       
       ConnectTask(Channel channel, String clusterName, CountDownLatch startLatch, CountDownLatch finishLatch)
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
