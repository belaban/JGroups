// $Id: ChannelTest.java,v 1.10.2.1 2008/02/06 07:01:57 vlada Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.List;
import java.util.LinkedList;


/**
 * Tests various methods in JChannel
 * @author Bela Ban
 * @version $Id: ChannelTest.java,v 1.10.2.1 2008/02/06 07:01:57 vlada Exp $
 */
public class ChannelTest extends ChannelTestBase {
    Channel ch;

    private static final String GROUP="DiscardTestGroup";
    //used in testNoViewIsReceivedAferDisconnect()
    boolean receivedViewWhenDisconnected;


    public void setUp() throws Exception {
        super.setUp();
        ch=createChannel();
        ch.connect(GROUP);
    }

    public void tearDown() throws Exception {
        ch.close();
        super.tearDown();        
    }
    
    public void testBasicOperations() throws Exception
    {
       String groupName = GROUP;
       Channel c1 = createChannel("A");
       c1.connect(groupName);
       Util.sleep(1000);
       assertTrue(c1.isOpen());
       assertTrue(c1.isConnected());
       
       assertNotNull(c1.getLocalAddress());
       assertNotNull(c1.getView());
       assertTrue(c1.getView().getMembers().contains(c1.getLocalAddress()));
       
       try{
         c1.connect(groupName);
       }
       catch (Exception e)
       {
         fail("Should have NOT generated exception");
       }
       
       c1.disconnect();
       
       assertFalse(c1.isConnected());
       assertTrue(c1.isOpen());
       assertNull(c1.getLocalAddress());
       assertNull(c1.getView());
       
       assertNull(c1.getClusterName());
       
       try{
          c1.connect(groupName);
       }
       catch (Exception e)
       {
         fail("Should have NOT generated exception");
       }
       
       c1.close();
       
       try{
          c1.connect(groupName);
          fail("Should generated exception, and it has NOT");
       }
       catch (Exception e)
       {
         assertTrue(e instanceof ChannelClosedException);
       }
       
       assertFalse(c1.isConnected());
       assertFalse(c1.isOpen());
       assertNull(c1.getLocalAddress());
       assertNull(c1.getView());
       
       assertNull(c1.getClusterName());                       
       
       c1 = createChannel("A");
       c1.connect(groupName);
       Channel c2 = createChannel("A");
       c2.connect(groupName);
       
       Util.sleep(1000);
       
       assertTrue(c1.isOpen());
       assertTrue(c1.isConnected());
       
       assertNotNull(c1.getLocalAddress());
       assertNotNull(c1.getView());
       assertTrue(c1.getView().getMembers().contains(c1.getLocalAddress()));
       assertTrue(c1.getView().getMembers().contains(c2.getLocalAddress()));
       
       assertTrue(c2.isOpen());
       assertTrue(c2.isConnected());
       
       assertNotNull(c2.getLocalAddress());
       assertNotNull(c2.getView());
       assertTrue(c2.getView().getMembers().contains(c2.getLocalAddress()));
       assertTrue(c2.getView().getMembers().contains(c1.getLocalAddress()));
       
       c2.close();
       Util.sleep(1000);
       
       assertFalse(c2.isOpen());
       assertFalse(c2.isConnected());
       
       assertNull(c2.getLocalAddress());
       assertNull(c2.getView());
       
       assertTrue(c1.isOpen());
       assertTrue(c1.isConnected());
       
       assertNotNull(c1.getLocalAddress());
       assertNotNull(c1.getView());
       assertTrue(c1.getView().getMembers().contains(c1.getLocalAddress()));
       assertFalse(c1.getView().getMembers().contains(c2.getLocalAddress()));
       
       c1.close();       
    }

    public void testFirstView() throws Exception {
        Object obj=ch.receive(5000);
        if(!(obj instanceof View)) {
            fail("first object returned needs to be a View (was " + obj + ")");
        }
        else {
            System.out.println("view is " + obj);
        }
    }


    public void testViewChange() throws Exception {
        ViewChecker checker=new ViewChecker(ch);
        ch.setReceiver(checker);

        Channel ch2=createChannel();
        ch2.connect(GROUP);
        assertTrue(checker.getReason(), checker.isSuccess());

        ch2.close();
        assertTrue(checker.getReason(), checker.isSuccess());
    }


    public void testIsConnectedOnFirstViewChange() throws Exception {
        Channel ch2=createChannel();
        ConnectedChecker tmp=new ConnectedChecker(ch2);
        ch2.setReceiver(tmp);
        ch2.connect(GROUP);

        assertFalse(tmp.isConnected());
        ch2.close();
    }
    
    public void testNoViewIsReceivedAferDisconnect() throws Exception {
        final Channel ch2 = createChannel();
        ReceiverAdapter ra = new ReceiverAdapter() {

            @Override
            public void viewAccepted(View new_view) {
                receivedViewWhenDisconnected = !new_view.containsMember(ch2.getLocalAddress());
            }
        };
        ch2.setReceiver(ra);
        ch2.connect(GROUP);

        Util.sleep(1000);
        ch2.disconnect();
        Util.sleep(1000);
        assertFalse("Received view where not member", receivedViewWhenDisconnected);

        ch2.close();
    }
    
    public void testNoViewIsReceivedAferClose() throws Exception {
        final Channel ch2 = createChannel();
        ReceiverAdapter ra = new ReceiverAdapter() {

            @Override
            public void viewAccepted(View new_view) {
                receivedViewWhenDisconnected = !new_view.containsMember(ch2.getLocalAddress());
            }
        };
        ch2.setReceiver(ra);
        ch2.connect(GROUP);

        Util.sleep(1000);
        ch2.close();
        Util.sleep(1000);
        assertFalse("Received view where not member", receivedViewWhenDisconnected);
    }


    public void testReceiveTimeout() throws ChannelException, TimeoutException {
        ch.receive(1000); // this one works, because we're expecting a View

        // .. but this one doesn't (no msg available) - needs to throw a TimeoutException
        try {
            ch.receive(2000);
        }
        catch(ChannelNotConnectedException e) {
            fail("channel should be connected");
        }
        catch(ChannelClosedException e) {
            fail("channel should not be closed");
        }
        catch(TimeoutException e) {
            System.out.println("caught a TimeoutException - this is the expected behavior");
        }
    }

    public void testNullMessage() throws ChannelClosedException, ChannelNotConnectedException {
        try {
            ch.send(null);
            fail("null message should throw an exception - we should not get here");
        }
        catch(NullPointerException e) {
            System.out.println("caught NullPointerException - this is expected");
        }
    }



     public void testOrdering() throws Exception {
        final int NUM=100;
        MyReceiver receiver=new MyReceiver(NUM);
        ch.setReceiver(receiver);
        for(int i=1; i <= NUM; i++) {
            ch.send(new Message(null, null, new Integer(i)));
            System.out.println("-- sent " + i);
        }

        receiver.waitForCompletion();

        List<Integer> nums=receiver.getNums();
        checkMonotonicallyIncreasingNumbers(nums);
    }

    private static void checkMonotonicallyIncreasingNumbers(List<Integer> nums) {
        int current=-1;
        for(int num: nums) {
            if(current < 0) {
                current=num;
            }
            else {
                assertEquals("list is " + nums, ++current,  num);
            }
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        final List<Integer> nums=new LinkedList<Integer>();
        final int expected;

        public MyReceiver(int expected) {
            this.expected=expected;
        }

        public List<Integer> getNums() {
            return nums;
        }

        public void waitForCompletion() throws InterruptedException {
            synchronized(nums) {
                while(nums.size() < expected) {
                    nums.wait();
                }
            }
        }

        public void receive(Message msg) {
            Util.sleepRandom(100);
            Integer num=(Integer)msg.getObject();
            synchronized(nums) {
                System.out.println("-- received " + num);
                nums.add(num);
                if(nums.size() >= expected) {
                    nums.notifyAll();
                }
            }
            Util.sleepRandom(100);
        }
    }


    private static class ConnectedChecker extends ReceiverAdapter {
        boolean connected=false;

        public ConnectedChecker(Channel channel) {
            this.channel=channel;
        }

        final Channel channel;

        public boolean isConnected() {
            return connected;
        }

        public void viewAccepted(View new_view) {
            connected=channel.isConnected();
            System.out.println("ConnectedChecker: channel.isConnected()=" + connected + ", view=" + new_view);
        }
    }

    private static class ViewChecker extends ReceiverAdapter {
        final Channel channel;
        boolean success=true;
        String reason="";

        public ViewChecker(Channel channel) {
            this.channel=channel;
        }

        public String getReason() {
            return reason;
        }

        public boolean isSuccess() {
            return success;
        }

        public void viewAccepted(View new_view) {
            View view=channel.getView();
            String str="viewAccepted(): channel's view=" + view + "\nreceived view=" + new_view;
            System.out.println(str);
            if(!view.equals(new_view)) {
                success=false;
                reason+=str + "\n";
            }
        }
    }


    public static Test suite() {
        return new TestSuite(ChannelTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}


