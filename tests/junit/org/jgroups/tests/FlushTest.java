package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests the FLUSH protocol. Adds a FLUSH layer on top of the stack unless already present. Should
 * work with any stack.
 * 
 * @author Bela Ban
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
        Util.waitUntilAllChannelsHaveSameSize(60000, 1000, tmp);

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
    public void testSequentialFlushInvocation() throws Exception {
        Channel channel = null, channel2 = null, channel3 = null;
        try {
            channel = createChannel(true, 3);
            channel.setName("A");

            channel2 = createChannel((JChannel) channel);
            channel2.setName("B");

            channel3 = createChannel((JChannel) channel);
            channel3.setName("C");

            channel.connect("x");
            channel2.connect("x");
            channel3.connect("x");
            
            //we need to sleep here since coordinator (channel)
            //might be unblocked before channel3.connect() returns
            Util.sleep(500);

            for (int i = 0; i < 100; i++) {
                System.out.print("flush #" + i + ": ");
                long start = System.currentTimeMillis();
                channel.startFlush(false);
                channel.stopFlush();
                long diff = System.currentTimeMillis() - start;
            }
        } finally {
            Util.close(channel, channel2, channel3);
        }
    }

    @Test
    public void testFlushWithCrashedFlushCoordinator() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        JChannel c3 = null;

        try {
            c1 = createChannel(true, 3, "C1"); changeProps(c1);
            c1.connect("testFlushWithCrashedFlushCoordinator");

            c2 = createChannel(c1, "C2"); changeProps(c2);
            c2.connect("testFlushWithCrashedFlushCoordinator");

            c3 = createChannel(c1, "C3"); changeProps(c3);
            c3.connect("testFlushWithCrashedFlushCoordinator");




            System.out.println("shutting down flush coordinator C2");
            // send out START_FLUSH and then return
            c2.down(new Event(Event.SUSPEND_BUT_FAIL));

            // now shut down C2. This means, after failure detection kicks in and the new coordinator takes over
            // (either C1 or C3), that the current flush started by C2 will be cancelled and a new flush (by C1 or C3)
            // will be started
            Util.shutdown(c2);

            c1.getProtocolStack().findProtocol(FLUSH.class).setLevel("trace");
            c3.getProtocolStack().findProtocol(FLUSH.class).setLevel("trace");

            Util.waitUntilAllChannelsHaveSameSize(10000, 500, c1, c3);

            // cluster should not hang and two remaining members should have a correct view
            assertTrue("correct view size", c1.getView().size() == 2);
            assertTrue("correct view size", c3.getView().size() == 2);

            c1.getProtocolStack().findProtocol(FLUSH.class).setLevel("warn");
            c3.getProtocolStack().findProtocol(FLUSH.class).setLevel("warn");

        } finally {
            Util.close(c3, c2, c1);
        }
    }

    @Test
    public void testFlushWithCrashedParticipant() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        JChannel c3 = null;

        try {
            c1 = createChannel(true, 3, "C1"); changeProps(c1);
            c1.connect("testFlushWithCrashedParticipant");

            c2 = createChannel(c1, "C2"); changeProps(c2);
            c2.connect("testFlushWithCrashedParticipant");

            c3 = createChannel(c1, "C3"); changeProps(c3);
            c3.connect("testFlushWithCrashedParticipant");

            System.out.println("shutting down C3");
            Util.shutdown(c3); // kill a flush participant

            System.out.println("C2: starting flush");
            boolean rc=Util.startFlush(c2);
            System.out.println("flush " + (rc? " was successful" : "failed"));
            assert rc;

            System.out.println("stopping flush");
            c2.stopFlush();

            System.out.println("waiting for view to contain C1 and C2");
            Util.waitUntilAllChannelsHaveSameSize(10000, 500, c1, c2);

            // cluster should not hang and two remaining members should have a correct view
            System.out.println("C1: view=" + c1.getView() + "\nC2: view=" + c2.getView());
            assertTrue("correct view size", c1.getView().size() == 2);
            assertTrue("correct view size", c2.getView().size() == 2);
        } finally {
            Util.close(c3, c2, c1);
        }
    }

    @Test
    public void testFlushWithCrashedParticipants() throws Exception {
        JChannel c1 = null;
        JChannel c2 = null;
        JChannel c3 = null;

        try {
            c1 = createChannel(true, 3, "C1"); changeProps(c1);
            c1.connect("testFlushWithCrashedFlushCoordinator");

            c2 = createChannel(c1, "C2"); changeProps(c2);
            c2.connect("testFlushWithCrashedFlushCoordinator");

            c3 = createChannel(c1, "C3"); changeProps(c3);
            c3.connect("testFlushWithCrashedFlushCoordinator");

            // and then kill members other than flush coordinator
            Util.shutdown(c3);
            Util.shutdown(c1);

            // start flush
            Util.startFlush(c2);

            c2.stopFlush();
            Util.waitUntilAllChannelsHaveSameSize(10000, 500, c2);

            // cluster should not hang and one remaining member should have a correct view
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
            members.add(c2.getAddress());
            boolean flushedOk = Util.startFlush(c2, members);

            assertTrue("Partial flush worked", flushedOk);

            c2.stopFlush(members);
        } finally {
            Util.close(c2, c1);
        }
    }

    /** Tests the emition of block/unblock/get|set state events */
    @Test
    public void testBlockingNoStateTransfer() throws Exception {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_ONLY);
    }

    /** Tests the emition of block/unblock/get|set state events */
    @Test
    public void testBlockingWithStateTransfer() throws Exception {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE);
    }

    /** Tests the emition of block/unblock/get|set state events */
    @Test
    public void testBlockingWithConnectAndStateTransfer() throws Exception {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_AND_GET_STATE);
    }

    private void _testChannels(String names[], int connectType) throws Exception {
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
            Util.waitUntilAllChannelsHaveSameSize(30000, 1000, tmp);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            semaphore.tryAcquire(count, 40, TimeUnit.SECONDS);

            Util.sleep(1000); //let all events propagate...
            for (FlushTestReceiver app : channels)
                app.getChannel().setReceiver(null);
            for (FlushTestReceiver app : channels)
                app.cleanup();

            // verify block/unblock/view/get|set state sequences for all members
            for (FlushTestReceiver receiver : channels) {
                checkEventStateTransferSequence(receiver);
                System.out.println("event sequence is OK");
            }
        }
        finally {
            for (FlushTestReceiver app : channels)
                app.cleanup();
        }
    }

    private static void changeProps(JChannel ... channels) {
        for(JChannel ch: channels) {
            FD fd=(FD)ch.getProtocolStack().findProtocol(FD.class);
            if(fd != null) {
                fd.setTimeout(1000);
                fd.setMaxTries(2);
            }
            FD_ALL fd_all=(FD_ALL)ch.getProtocolStack().findProtocol(FD_ALL.class);
            if(fd_all != null) {
                fd_all.setTimeout(2000);
                fd_all.setInterval(800);
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
            events = new StringBuilder();
            if (connectMethod == CONNECT_ONLY || connectMethod == CONNECT_AND_SEPARATE_GET_STATE)
                channel.connect("FlushTestReceiver");

            if (connectMethod == CONNECT_AND_GET_STATE) {
                channel.connect("FlushTestReceiver", null, 25000);
            }
        }

        protected FlushTestReceiver(JChannel ch, String name, Semaphore semaphore, int msgCount,
                        int connectMethod) throws Exception {
            super(ch, name, semaphore);
            this.connectMethod = connectMethod;
            this.msgCount = msgCount;
            events = new StringBuilder();
            if (connectMethod == CONNECT_ONLY || connectMethod == CONNECT_AND_SEPARATE_GET_STATE)
                channel.connect("FlushTestReceiver");

            if (connectMethod == CONNECT_AND_GET_STATE) {
                channel.connect("FlushTestReceiver", null, 25000);
            }
        }

        public String getEventSequence() {
            return events.toString();
        }


        public void getState(OutputStream ostream) throws Exception {
            super.getState(ostream);
            byte[] payload = new byte[] { 'b', 'e', 'l', 'a' };
            ostream.write(payload);
        }

        public void setState(InputStream istream) throws Exception {
            super.setState(istream);
            byte[] payload = new byte[4];
            istream.read(payload);
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

    private class SimpleReplier extends ReceiverAdapter {
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