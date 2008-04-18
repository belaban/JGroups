
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;


/**
 * Tests various methods in JChannel
 * @author Bela Ban
 * @version $Id: ChannelTest.java,v 1.16 2008/04/18 05:52:28 belaban Exp $
 */
@Test(groups="temp",sequential=true)
public class ChannelTest extends ChannelTestBase {
    Channel ch;

    private static final String GROUP="DiscardTestGroup";
    //used in testNoViewIsReceivedAferDisconnect()
    boolean receivedViewWhenDisconnected;

    @BeforeMethod
    public void setUp() throws Exception {
        ch=createChannel();
        ch.connect(GROUP);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        ch.close();
    }
    
    @Test
    public void testBasicOperations() throws Exception {
        String groupName = GROUP;
        Channel c1 = createChannel("A");
        c1.connect(groupName);
        Util.sleep(1000);
        assert c1.isOpen();
        assert c1.isConnected();
       
        assert c1.getLocalAddress() != null;
        assert c1.getView() != null;
        assert c1.getView().getMembers().contains(c1.getLocalAddress());
       
        c1.connect(groupName);

        c1.disconnect();
       
        assert c1.isConnected() == false;
        assert c1.isOpen();
        assert c1.getLocalAddress() == null;
        assert c1.getView() == null;
        assert c1.getClusterName() == null;
       
        c1.connect(groupName);

        c1.close();
       
        try {
            c1.connect(groupName);
            throw new IllegalStateException("Should generated exception, and it has NOT");
        }
        catch (Exception e) {
            assert e instanceof ChannelClosedException;
        }
       
        assert c1.isConnected() == false;
        assert c1.isOpen() == false;
        assert c1.getLocalAddress() == null;
        assert c1.getView() == null;
       
        assert c1.getClusterName() == null;
       
        c1 = createChannel("A");
        c1.connect(groupName);
        Channel c2 = createChannel("A");
        c2.connect(groupName);
       
        Util.sleep(1000);
       
        assert c1.isOpen();
        assert c1.isConnected();
       
        assert c1.getLocalAddress() != null;
        assert c1.getView() != null;
        assert c1.getView().getMembers().contains(c1.getLocalAddress());
        assert c1.getView().getMembers().contains(c2.getLocalAddress());
       
        assert c2.isOpen();
        assert c2.isConnected();
       
        assert c2.getLocalAddress() != null;
        assert c2.getView() != null;
        assert c2.getView().getMembers().contains(c2.getLocalAddress());
        assert c2.getView().getMembers().contains(c1.getLocalAddress());
       
        c2.close();
        Util.sleep(1000);
       
        assert c2.isOpen() == false;
        assert c2.isConnected() == false;
       
        assert c2.getLocalAddress() == null;
        assert c2.getView() == null;
       
        assert c1.isOpen();
        assert c1.isConnected();
       
        assert c1.getLocalAddress() != null;
        assert c1.getView() != null;
        assert c1.getView().getMembers().contains(c1.getLocalAddress());
        assert c1.getView().getMembers().contains(c2.getLocalAddress()) == false;
        c1.close();
    }

    @Test
    public void testFirstView() throws Exception {
        Object obj=ch.receive(5000);
        System.out.println("view is " + obj);
        assert obj instanceof View : "first object returned needs to be a View (was " + obj + ")";
    }


    @Test
    public void testViewChange() throws Exception {
        ViewChecker checker=new ViewChecker(ch);
        ch.setReceiver(checker);

        Channel ch2=createChannel();
        ch2.connect(GROUP);
        assert checker.isSuccess() : checker.getReason();

        ch2.close();
        assert checker.isSuccess() : checker.getReason();
    }


    @Test
    public void testIsConnectedOnFirstViewChange() throws Exception {
        Channel ch2=createChannel();
        ConnectedChecker tmp=new ConnectedChecker(ch2);
        ch2.setReceiver(tmp);
        ch2.connect(GROUP);

        assert tmp.isConnected() == false;
        ch2.close();
    }
    
    @Test
    public void testNoViewIsReceivedAferDisconnect() throws Exception {
        final Channel ch2 = createChannel();
        ReceiverAdapter ra = new ReceiverAdapter() {
            public void viewAccepted(View new_view) {
                receivedViewWhenDisconnected = !new_view.containsMember(ch2.getLocalAddress());
            }
        };
        ch2.setReceiver(ra);
        ch2.connect(GROUP);

        Util.sleep(1000);
        ch2.disconnect();
        Util.sleep(1000);
        assert !receivedViewWhenDisconnected : "Received view where not member";
        ch2.close();
    }
    
    @Test
    public void testNoViewIsReceivedAferClose() throws Exception {
        final Channel ch2 = createChannel();
        ReceiverAdapter ra = new ReceiverAdapter() {
            public void viewAccepted(View new_view) {
                receivedViewWhenDisconnected = !new_view.containsMember(ch2.getLocalAddress());
            }
        };
        ch2.setReceiver(ra);
        ch2.connect(GROUP);

        Util.sleep(1000);
        ch2.close();
        Util.sleep(1000);
        assert !receivedViewWhenDisconnected : "Received view where not member";
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testReceiveTimeout() throws ChannelException, TimeoutException {
        ch.receive(1000); // this one works, because we're expecting a View
        ch.receive(2000); // .. but this one doesn't (no msg available) - needs to throw a TimeoutException
    }

    @Test(expectedExceptions={NullPointerException.class})
    public void testNullMessage() throws ChannelClosedException, ChannelNotConnectedException {
        ch.send(null);
    }

    @Test
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
                assert ++current == num : "list is " + nums;
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
            Integer num=(Integer)msg.getObject();
            synchronized(nums) {
                System.out.println("-- received " + num);
                nums.add(num);
                if(nums.size() >= expected) {
                    nums.notifyAll();
                }
            }
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




}


