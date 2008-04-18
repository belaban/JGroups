
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
 * @version $Id: ChannelTest.java,v 1.17 2008/04/18 09:05:35 belaban Exp $
 */
@Test(groups="temp",sequential=false)
public class ChannelTest extends ChannelTestBase {
    private final ThreadLocal<Channel> ch=new ThreadLocal<Channel>();
    private final ThreadLocal<String>  PROPS=new ThreadLocal<String>();
    private final ThreadLocal<String>  GROUP=new ThreadLocal<String>();


    @BeforeMethod
    void init() throws Exception {
        String cluster_name=getUniqueClusterName("ChannelTest");
        GROUP.set(cluster_name);
        Channel tmp=createChannel(true, 2);
        String tmp_props=tmp.getProperties();
        PROPS.set(tmp_props);
        tmp.connect(GROUP.get());
        ch.set(tmp);
    }


    @AfterMethod
    void cleanup() {
        Channel tmp_ch=ch.get();
        Util.close(tmp_ch);
        ch.set(null);
        GROUP.set(null);
        PROPS.set(null);
    }


    @Test
    public void testBasicOperations() throws Exception {
        Channel c1 = createChannelWithProps(PROPS.get());
        Channel c2=null;

        try {
            c1.connect(GROUP.get());
            assert c1.isOpen();
            assert c1.isConnected();
       
            assert c1.getLocalAddress() != null;
            assert c1.getView() != null;
            assert c1.getView().getMembers().contains(c1.getLocalAddress());
       
            c1.connect(GROUP.get());
            c1.disconnect();
            assert c1.isConnected() == false;
            assert c1.isOpen();
            assert c1.getLocalAddress() == null;
            assert c1.getView() == null;
            assert c1.getClusterName() == null;
       
            c1.connect(GROUP.get());

            c1.close();
       
            try {
                c1.connect(GROUP.get());
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
       
            c1 = createChannelWithProps(PROPS.get());
            c1.connect(GROUP.get());
            c2 = createChannelWithProps(PROPS.get());
            c2.connect(GROUP.get());
       
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
        }
        finally {
            Util.close(c2);
            Util.close(c1);
        }
    }

    @Test
    public void testFirstView() throws Exception {
        Object obj=ch.get().receive(5000);
        assert obj instanceof View : "first object returned needs to be a View (was " + obj + ")";
    }


    @Test
    public void testViewChange() throws Exception {
        ViewChecker checker=new ViewChecker(ch.get());
        ch.get().setReceiver(checker);

        Channel ch2=createChannelWithProps(PROPS.get());
        try {
            ch2.connect(GROUP.get());
            assertTrue(checker.getReason(), checker.isSuccess());
            ch2.close();
            assertTrue(checker.getReason(), checker.isSuccess());
        }
        finally {
            Util.close(ch2);
        }
    }


    @Test
    public void testIsConnectedOnFirstViewChange() throws Exception {
        Channel ch2=createChannelWithProps(PROPS.get());
        ConnectedChecker tmp=new ConnectedChecker(ch2);
        ch2.setReceiver(tmp);
        try {
            ch2.connect(GROUP.get());
            assertFalse(tmp.isConnected());
            ch2.close();
        }
        finally {
            Util.close(ch2);
        }
    }



    @Test
    public void testNoViewIsReceivedAferDisconnect() throws Exception {
        final Channel ch2 = createChannelWithProps(PROPS.get());
        MyViewChecker ra = new MyViewChecker(ch2);
        ch2.setReceiver(ra);

        try {
            ch2.connect(GROUP.get());
            Util.sleep(500);
            ch2.disconnect();
            Util.sleep(1000);
            assert !ra.receivedViewWhenDisconnected : "Received view where not member";
        }
        finally {
            ch2.close();
        }
    }
    
    @Test
    public void testNoViewIsReceivedAferClose() throws Exception {
        final Channel ch2 = createChannelWithProps(PROPS.get());
        MyViewChecker ra = new MyViewChecker(ch2);
        ch2.setReceiver(ra);

        try {
            ch2.connect(GROUP.get());
            Util.sleep(200);
            ch2.close();
            Util.sleep(1000);
            assert !ra.receivedViewWhenDisconnected : "Received view where not member";
        }
        finally {
            Util.close(ch2);
        }
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testReceiveTimeout() throws ChannelException, TimeoutException {
        ch.get().receive(1000); // this one works, because we're expecting a View
        ch.get().receive(2000); // .. but this one doesn't (no msg available) - needs to throw a TimeoutException
    }

    @Test(expectedExceptions={NullPointerException.class})
    public void testNullMessage() throws ChannelClosedException, ChannelNotConnectedException {
        ch.get().send(null);
    }

    @Test
    public void testOrdering() throws Exception {
        final int NUM=100;
        MyReceiver receiver=new MyReceiver(NUM);
        ch.get().setReceiver(receiver);
        for(int i=1; i <= NUM; i++) {
            ch.get().send(new Message(null, null, new Integer(i)));
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


    private static class MyViewChecker extends ReceiverAdapter {
        private boolean receivedViewWhenDisconnected;
        private final Channel ch;

        public MyViewChecker(Channel ch) {
            this.ch=ch;
        }

        public void viewAccepted(View new_view) {
            receivedViewWhenDisconnected = !new_view.containsMember(ch.getLocalAddress());
        }

    }



}


