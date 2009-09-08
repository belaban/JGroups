package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests the FLUSH protocol. Adds a FLUSH layer on top of the stack unless already present. Should
 * work with any stack.
 * 
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.84 2009/09/08 19:52:01 vlada Exp $
 */
@Test(groups = Global.FLUSH, sequential = false)
public class FlushTest extends ChannelTestBase {

    @Test
    public void testSingleChannel() throws Exception {
        Semaphore s = new Semaphore(1);
        FlushTestReceiver receivers[] = new FlushTestReceiver[] { new FlushTestReceiver("c1", s, 0,
                        FlushTestReceiver.CONNECT_ONLY) };
        receivers[0].start();
        s.release(1);

        // Make sure everyone is in sync
        Channel[] tmp = new Channel[receivers.length];
        for (int i = 0; i < receivers.length; i++)
            tmp[i] = receivers[i].getChannel();
        Util.blockUntilViewsReceived(60000, 1000, tmp);

        // Reacquire the semaphore tickets; when we have them all
        // we know the threads are done
        s.tryAcquire(1, 60, TimeUnit.SECONDS);
        receivers[0].cleanup();
        Util.sleep(1000);

        checkEventStateTransferSequence(receivers[0]);
    }

    /**
     * Tests issue #1 in http://jira.jboss.com/jira/browse/JGRP-335
     * 
     * @throws Exception
     */
    @Test
    public void testJoinFollowedByUnicast() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        try {
            c1 = createChannel(true, 2);
            c1.setReceiver(new SimpleReplier(c1, true));
            c1.connect("testJoinFollowedByUnicast");

            Address target = c1.getAddress();
            Message unicast_msg = new Message(target);

            c2 = createChannel(c1);
            c2.setReceiver(new SimpleReplier(c2, false));
            c2.connect("testJoinFollowedByUnicast");

            // now send unicast, this might block as described in the case
            c2.send(unicast_msg);
            // if we don't get here this means we'd time out
        } finally {
            Util.close(c2, c1);
        }
    }

    /**
     * Tests issue #2 in http://jira.jboss.com/jira/browse/JGRP-335
     * 
     * @throws Exception
     */
    @Test
    public void testStateTransferFollowedByUnicast() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        try {
            c1 = createChannel(true, 2);
            c1.setReceiver(new SimpleReplier(c1, true));
            c1.connect("testStateTransferFollowedByUnicast");

            Address target = c1.getAddress();
            Message unicast_msg = new Message(target);

            c2 = createChannel(c1);
            c2.setReceiver(new SimpleReplier(c2, false));
            c2.connect("testStateTransferFollowedByUnicast");

            log.info("\n** Getting the state **");
            c2.getState(null, 10000);
            // now send unicast, this might block as described in the case
            c2.send(unicast_msg);
        } finally {
            Util.close(c2, c1);
        }
    }

    @Test
    public void testFlushWithCrashedFlushCoordinator() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        JChannel c3 = null;

        try {
            c1 = createChannel(true, 3);
            c1.connect("testFlushWithCrashedFlushCoordinator");

            c2 = createChannel(c1);
            c2.connect("testFlushWithCrashedFlushCoordinator");

            c3 = createChannel(c1);
            c3.connect("testFlushWithCrashedFlushCoordinator");

            Util.startFlush(c2);
            c2.shutdown(); // kill the flush coordinator

            Util.blockUntilViewsReceived(10000, 500, c1, c3);

            // cluster should not hang and two remaining members should have a correct view
            assertTrue("correct view size", c1.getView().size() == 2);
            assertTrue("correct view size", c3.getView().size() == 2);
        } finally {
            Util.close(c3, c2, c1);
        }
    }

    @Test
    public void testFlushWithCrashedNonCoordinator() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        JChannel c3 = null;

        try {
            c1 = createChannel(true, 3);
            c1.connect("testFlushWithCrashedFlushCoordinator");

            c2 = createChannel(c1);
            c2.connect("testFlushWithCrashedFlushCoordinator");

            c3 = createChannel(c1);
            c3.connect("testFlushWithCrashedFlushCoordinator");

            Util.startFlush(c2);
            c3.shutdown(); // kill the flush coordinator

            c2.stopFlush();
            Util.blockUntilViewsReceived(10000, 500, c1, c2);

            // cluster should not hang and two remaining members should have a
            // correct view
            assertTrue("correct view size", c1.getView().size() == 2);
            assertTrue("correct view size", c2.getView().size() == 2);
        } finally {
            Util.close(c3, c2, c1);
        }
    }

    @Test
    public void testFlushWithCrashedNonCoordinators() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        JChannel c3 = null;

        try {
            c1 = createChannel(true, 3);
            c1.connect("testFlushWithCrashedFlushCoordinator");

            c2 = createChannel(c1);
            c2.connect("testFlushWithCrashedFlushCoordinator");

            c3 = createChannel(c1);
            c3.connect("testFlushWithCrashedFlushCoordinator");

            // start flush
            Util.startFlush(c2);

            // and then kill members other than flush coordinator
            c3.shutdown();
            c1.shutdown();

            c2.stopFlush();
            Util.blockUntilViewsReceived(10000, 500, c2);

            // cluster should not hang and one remaining member should have a
            // correct view
            assertTrue("correct view size", c2.getView().size() == 1);
        } finally {
            Util.close(c3, c2, c1);
        }
    }

    /**
     * Tests http://jira.jboss.com/jira/browse/JGRP-661
     * 
     * @throws Exception
     */
    @Test
    public void testPartialFlush() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        try {
            c1 = createChannel(true, 2);
            c1.setReceiver(new SimpleReplier(c1, true));
            c1.connect("testPartialFlush");

            c2 = createChannel(c1);
            c2.setReceiver(new SimpleReplier(c2, false));
            c2.connect("testPartialFlush");

            List<Address> members = new ArrayList<Address>();
            members.add(c2.getLocalAddress());
            boolean flushedOk = Util.startFlush(c2, members);

            assertTrue("Partial flush worked", flushedOk);

            c2.stopFlush(members);
        } finally {
            Util.close(c2, c1);
        }
    }

    /** Tests the emition of block/unblock/get|set state events */
    @Test
    public void testBlockingNoStateTransfer() {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_ONLY);
    }

    /** Tests the emition of block/unblock/get|set state events */
    @Test
    public void testBlockingWithStateTransfer() {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE);
    }

    /** Tests the emition of block/unblock/get|set state events */
    @Test
    public void testBlockingWithConnectAndStateTransfer() {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_AND_GET_STATE);
    }

    private void _testChannels(String names[], int connectType) {
        int count = names.length;

        List<FlushTestReceiver> channels = new ArrayList<FlushTestReceiver>(count);
        try {
            // Create a semaphore and take all its permits
            Semaphore semaphore = new Semaphore(count);
            semaphore.acquire(count);

            // Create channels and their threads that will block on the
            // semaphore
            boolean first = true;
            for (String channelName : names) {
                FlushTestReceiver channel = null;
                if (first)
                    channel = new FlushTestReceiver(channelName, semaphore, 0, connectType);
                else {
                    channel = new FlushTestReceiver((JChannel) channels.get(0).getChannel(),
                                    channelName, semaphore, 0, connectType);
                }
                channels.add(channel);

                // Release one ticket at a time to allow the thread to start working
                channel.start();
                semaphore.release(1);
                if (first)
                    Util.sleep(3000); // minimize changes of a merge happening
                first = false;
            }

            Channel[] tmp = new Channel[channels.size()];
            int cnt = 0;
            for (FlushTestReceiver receiver : channels)
                tmp[cnt++] = receiver.getChannel();
            Util.blockUntilViewsReceived(10000, 1000, tmp);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            semaphore.tryAcquire(count, 40, TimeUnit.SECONDS);

        } catch (Exception ex) {
            log.warn("Exception encountered during test", ex);
            assert false : "Exception encountered during test execution: " + ex;
        } finally {
            //let all events propagate...
            Util.sleep(1000);
            for (FlushTestReceiver app : channels)
                app.getChannel().setReceiver(null);
            for (FlushTestReceiver app : channels)
                app.cleanup();
                        

            // verify block/unblock/view/get|set state sequences for all members
            for (FlushTestReceiver receiver : channels) {
                checkEventStateTransferSequence(receiver);
                System.out.println("event sequence for " + receiver.getChannel().getAddress()
                                + " is OK");
            }
        }
    }

    private class FlushTestReceiver extends PushChannelApplicationWithSemaphore {
        private int connectMethod;

        public static final int CONNECT_ONLY = 1;

        public static final int CONNECT_AND_SEPARATE_GET_STATE = 2;

        public static final int CONNECT_AND_GET_STATE = 3;

        int msgCount = 0;

        protected FlushTestReceiver(String name, Semaphore semaphore, int msgCount,
                        int connectMethod) throws Exception {
            super(name, semaphore);
            this.connectMethod = connectMethod;
            this.msgCount = msgCount;
            events = Collections.synchronizedList(new LinkedList<Object>());
            if (connectMethod == CONNECT_ONLY || connectMethod == CONNECT_AND_SEPARATE_GET_STATE)
                channel.connect("FlushTestReceiver");

            if (connectMethod == CONNECT_AND_GET_STATE) {
                channel.connect("FlushTestReceiver", null, null, 25000);
            }
        }

        protected FlushTestReceiver(JChannel ch, String name, Semaphore semaphore, int msgCount,
                        int connectMethod) throws Exception {
            super(ch, name, semaphore);
            this.connectMethod = connectMethod;
            this.msgCount = msgCount;
            events = Collections.synchronizedList(new LinkedList<Object>());
            if (connectMethod == CONNECT_ONLY || connectMethod == CONNECT_AND_SEPARATE_GET_STATE)
                channel.connect("FlushTestReceiver");

            if (connectMethod == CONNECT_AND_GET_STATE) {
                channel.connect("FlushTestReceiver", null, null, 25000);
            }
        }

        public List<Object> getEvents() {
            return new LinkedList<Object>(events);
        }

        public byte[] getState() {
            events.add(new GetStateEvent(null, null));
            return new byte[] { 'b', 'e', 'l', 'a' };
        }

        public void getState(OutputStream ostream) {
            super.getState(ostream);
            byte[] payload = new byte[] { 'b', 'e', 'l', 'a' };
            try {
                ostream.write(payload);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                Util.close(ostream);
            }
        }

        public void setState(InputStream istream) {
            super.setState(istream);
            byte[] payload = new byte[4];
            try {
                istream.read(payload);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                Util.close(istream);
            }
        }

        protected void useChannel() throws Exception {
            if (connectMethod == CONNECT_AND_SEPARATE_GET_STATE) {
                channel.getState(null, 25000);
            }
            if (msgCount > 0) {
                for (int i = 0; i < msgCount; i++) {
                    channel.send(new Message());
                    Util.sleep(100);
                }
            }
        }
    }

    private class SimpleReplier extends ExtendedReceiverAdapter {
        Channel channel;

        boolean handle_requests = false;

        public SimpleReplier(Channel channel, boolean handle_requests) {
            this.channel = channel;
            this.handle_requests = handle_requests;
        }

        public void receive(Message msg) {
            Message reply = new Message(msg.getSrc());
            try {
                log.info("-- MySimpleReplier[" + channel.getAddress() + "]: received message from "
                                + msg.getSrc());
                if (handle_requests) {
                    log.info(", sending reply");
                    channel.send(reply);
                } else
                    System.out.println("\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void viewAccepted(View new_view) {
            log.info("-- MySimpleReplier[" + channel.getAddress() + "]: viewAccepted(" + new_view
                            + ")");
        }

        public void block() {
            log.info("-- MySimpleReplier[" + channel.getAddress() + "]: block()");
        }

        public void unblock() {
            log.info("-- MySimpleReplier[" + channel.getAddress() + "]: unblock()");
        }
    }

}