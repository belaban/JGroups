package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Tests the FLUSH protocol. Adds a FLUSH layer on top of the stack unless already present. Should
 * work with any stack.
 * 
 * @author Bela Ban
 */
@Test(groups = {Global.FLUSH, Global.EAP_EXCLUDED}, singleThreaded = true)
public class FlushTest {

    public void testSingleChannel() throws Exception {
        Semaphore s = new Semaphore(1);
        FlushTestReceiver receivers[] ={ new FlushTestReceiver("c1", s, 0, FlushTestReceiver.CONNECT_ONLY) };
        receivers[0].start();
        s.release(1);

        // Make sure everyone is in sync
        JChannel[] tmp = new JChannel[receivers.length];
        for (int i = 0; i < receivers.length; i++)
            tmp[i] = receivers[i].getChannel();
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, tmp);

        // Reacquire the semaphore tickets; when we have them all we know the threads are done
        s.tryAcquire(1, 10, TimeUnit.SECONDS);
        receivers[0].cleanup();
        Util.sleep(1000);

        checkEventStateTransferSequence(receivers[0]);
    }

    /** Tests issue #1 in http://jira.jboss.com/jira/browse/JGRP-335 */
    public void testJoinFollowedByUnicast() throws Exception {
        JChannel a=null, b=null;
        try {
            a = createChannel("A");
            a.setReceiver(new SimpleReplier(a,true));
            a.connect("testJoinFollowedByUnicast");

            Address target = a.getAddress();
            Message unicast_msg = new Message(target);

            b = createChannel("B");
            b.setReceiver(new SimpleReplier(b,false));
            b.connect("testJoinFollowedByUnicast");

            // now send unicast, this might block as described in the case
            b.send(unicast_msg);
            // if we don't get here this means we'd time out
        } finally {
            Util.close(b, a);
        }
    }

    /**
     * Tests issue #2 in http://jira.jboss.com/jira/browse/JGRP-335
     */
    public void testStateTransferFollowedByUnicast() throws Exception {
        JChannel a=null, b=null;
        try {
            a = createChannel("A");
            a.setReceiver(new SimpleReplier(a,true));
            a.connect("testStateTransferFollowedByUnicast");

            Address target = a.getAddress();
            Message unicast_msg = new Message(target);

            b = createChannel("B");
            b.setReceiver(new SimpleReplier(b,false));
            b.connect("testStateTransferFollowedByUnicast");

            System.out.println("\n** Getting the state **");
            b.getState(null,10000);
            // now send unicast, this might block as described in the case
            b.send(unicast_msg);
        } finally {
            Util.close(b, a);
        }
    }
    
    public void testSequentialFlushInvocation() throws Exception {
        JChannel a=null, b=null, c=null;
        try {
            a = createChannel("A");
            a.connect("testSequentialFlushInvocation");

            b = createChannel("B");
            b.connect("testSequentialFlushInvocation");

            c = createChannel("C");
            c.connect("testSequentialFlushInvocation");

            Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c);
            
            for (int i = 0; i < 100; i++) {
                System.out.print("flush #" + i + ": ");
                a.startFlush(false);
                a.stopFlush();
                System.out.println("OK");
            }
        } finally {
            Util.close(a, b, c);
        }
    }

    public void testFlushWithCrashedFlushCoordinator() throws Exception {
        JChannel a=null, b=null, c=null;
        try {
            a = createChannel("A");
            b = createChannel("B");
            c = createChannel("C");
            changeProps(a,b,c);
            b.connect("testFlushWithCrashedFlushCoordinator");
            a.connect("testFlushWithCrashedFlushCoordinator");

            c.connect("testFlushWithCrashedFlushCoordinator");

            Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);

            System.out.println("shutting down flush coordinator B");
            b.down(new Event(Event.SUSPEND_BUT_FAIL)); // send out START_FLUSH and then return

            // now shut down B. This means, after failure detection kicks in and the new coordinator takes over
            // (either A or C), that the current flush started by B will be cancelled and a new flush (by A or C)
            // will be started
            Util.shutdown(b);
            Stream.of(a,c).forEach(ch -> ch.getProtocolStack().findProtocol(FLUSH.class).setLevel("debug"));

            for(int i=0; i < 20; i++) {
                if(Stream.of(a,c).allMatch(ch -> ch.view().size() == 2))
                    break;
                Util.sleep(500);
            }

            // cluster should not hang and two remaining members should have a correct view
            assert a.getView().size() == 2 : String.format("A's view: %s", a.getView());
            assert c.getView().size() == 2 : String.format("C's view: %s", c.getView());
        }
        finally {
            Util.close(c, b, a);
        }
    }

    public void testFlushWithCrashedParticipant() throws Exception {
        JChannel a=null, b=null, c=null;

        try {
            a = createChannel("A"); changeProps(a);
            a.connect("testFlushWithCrashedParticipant");

            b = createChannel("B"); changeProps(b);
            b.connect("testFlushWithCrashedParticipant");

            c = createChannel("C"); changeProps(c);
            c.connect("testFlushWithCrashedParticipant");

            System.out.println("shutting down C3");
            Util.shutdown(c); // kill a flush participant

            System.out.println("C2: starting flush");
            boolean rc=Util.startFlush(b);
            System.out.println("flush " + (rc? " was successful" : "failed"));
            assert rc;

            System.out.println("stopping flush");
            b.stopFlush();

            System.out.println("waiting for view to contain C1 and C2");
            Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

            // cluster should not hang and two remaining members should have a correct view
            System.out.println("C1: view=" + a.getView() + "\nC2: view=" + b.getView());
            assert a.getView().size() == 2;
            assert b.getView().size() == 2;
        } finally {
            Util.close(c, b, a);
        }
    }

    public void testFlushWithCrashedParticipants() throws Exception {
        JChannel a=null, b=null, c=null;

        try {
            a = createChannel("A"); changeProps(a);
            a.connect("testFlushWithCrashedFlushCoordinator");

            b = createChannel("B"); changeProps(b);
            b.connect("testFlushWithCrashedFlushCoordinator");

            c = createChannel("C"); changeProps(c);
            c.connect("testFlushWithCrashedFlushCoordinator");

            // and then kill members other than flush coordinator
            Util.shutdown(c);
            Util.shutdown(a);

            // start flush
            Util.startFlush(b);

            b.stopFlush();
            for(int i=0; i < 20; i++) {
                if(b.getView().size() == 1)
                    break;
                Util.sleep(500);
            }

            // cluster should not hang and one remaining member should have a correct view
            assert b.getView().size() == 1 : String.format("B's view is %s", b.getView());
        } finally {
            Util.close(c, b, a);
        }
    }

    /**
     * Tests http://jira.jboss.com/jira/browse/JGRP-661
     */
    public void testPartialFlush() throws Exception {
        JChannel a=null, b=null;
        try {
            a = createChannel("A");
            a.setReceiver(new SimpleReplier(a,true));
            a.connect("testPartialFlush");

            b = createChannel("B");
            b.setReceiver(new SimpleReplier(b,false));
            b.connect("testPartialFlush");

            List<Address> members = new ArrayList<>();
            members.add(b.getAddress());
            assert Util.startFlush(b, members);
            b.stopFlush(members);
        } finally {
            Util.close(b, a);
        }
    }

    /** Tests the emition of block/unblock/get|set state events */
    public void testBlockingNoStateTransfer() throws Exception {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_ONLY);
    }

    /** Tests the emition of block/unblock/get|set state events */
    public void testBlockingWithStateTransfer() throws Exception {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_AND_SEPARATE_GET_STATE);
    }

    /** Tests the emition of block/unblock/get|set state events */
    public void testBlockingWithConnectAndStateTransfer() throws Exception {
        String[] names = { "A", "B", "C", "D" };
        _testChannels(names, FlushTestReceiver.CONNECT_AND_GET_STATE);
    }

    private void _testChannels(String names[], int connectType) throws Exception {
        int count = names.length;

        List<FlushTestReceiver> channels = new ArrayList<>(count);
        try {
            // Create a semaphore and take all its permits
            Semaphore semaphore = new Semaphore(count);
            semaphore.acquire(count);

            // Create channels and their threads that will block on the
            // semaphore
            boolean first = true;
            for (String channelName : names) {
                FlushTestReceiver channel = null;
                channel = new FlushTestReceiver(channelName, semaphore, 0, connectType);
                channels.add(channel);

                // Release one ticket at a time to allow the thread to start working
                channel.start();
                semaphore.release(1);
                if (first)
                    Util.sleep(3000); // minimize changes of a merge happening
                first = false;
            }

            JChannel[] tmp = new JChannel[channels.size()];
            int cnt = 0;
            for (FlushTestReceiver receiver : channels)
                tmp[cnt++] = receiver.getChannel();
            Util.waitUntilAllChannelsHaveSameView(30000, 1000, tmp);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            semaphore.tryAcquire(count, 40, TimeUnit.SECONDS);

            Util.sleep(1000); //let all events propagate...
            for (FlushTestReceiver app : channels)
                app.getChannel().setReceiver(null);
            channels.forEach(FlushTestReceiver::cleanup);

            // verify block/unblock/view/get|set state sequences for all members
            for (FlushTestReceiver receiver : channels) {
                checkEventStateTransferSequence(receiver);
                System.out.println("event sequence is OK");
            }
        }
        finally {
            channels.forEach(FlushTestReceiver::cleanup);
        }
    }

    protected static void checkEventStateTransferSequence(EventSequence receiver) {
        String events = receiver.getEventSequence();
        assert events != null;
        final String validSequence = "([b][vgs]*[u])+";
        // translate the eventTrace to an eventString
        try {
            assert validateEventString(translateEventTrace(events), validSequence) : "Invalid event sequence " + events;
        } catch (Exception e) {
            assert false : "Invalid event sequence " + events;
        }
    }

    /**
     * Method for validating event strings against event string specifications, where a
     * specification is a regular expression involving event symbols. e.g. [b]([sgv])[u]
     */
    protected static boolean validateEventString(String eventString, String spec) {
        Pattern pattern = null;
        Matcher matcher = null;

        // set up the regular expression specification
        pattern = Pattern.compile(spec);
        // set up the actual event string
        matcher = pattern.matcher(eventString);

        // check if the actual string satisfies the specification
        if (matcher.find()) {
            // a match has been found, but we need to check that the whole event string
            // matches, and not just a substring
            if (!(matcher.start() == 0 && matcher.end() == eventString.length())) {
                // match on full eventString not found
                System.err.println("event string invalid (proper substring matched): event string = "
                                     + eventString
                                     + ", specification = "
                                     + spec
                                     + "matcher.start() "
                                     + matcher.start()
                                     + " matcher.end() " + matcher.end());
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    /**
     * Method for translating event traces into event strings, where each event in the trace is
     * represented by a letter.
     */
    protected static String translateEventTrace(String s) throws Exception {
        // if it ends with block, strip it out because it will be regarded as error sequence
        while (s.endsWith("b")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    protected JChannel createChannel(String name) throws Exception {
          Protocol[] protocols={
            new SHARED_LOOPBACK(),
            new SHARED_LOOPBACK_PING(),
            new FD_ALL().setValue("timeout", 3000).setValue("interval", 1000),
            new NAKACK2(),
            new UNICAST3(),
            new STABLE(),
            new GMS(),
            new FRAG2().fragSize(8000),
            new STATE_TRANSFER(),
            new FLUSH()
          };

          return new JChannel(protocols).name(name);
      }

    private static void changeProps(JChannel ... channels) {
        for(JChannel ch: channels) {
            FD fd=ch.getProtocolStack().findProtocol(FD.class);
            if(fd != null) {
                fd.setTimeout(1000);
                fd.setMaxTries(2);
            }
            FD_ALL fd_all=ch.getProtocolStack().findProtocol(FD_ALL.class);
            if(fd_all != null) {
                fd_all.setTimeout(2000);
                fd_all.setInterval(800);
                fd_all.setTimeoutCheckInterval(3000);
            }
        }
    }

    private class FlushTestReceiver extends ReceiverAdapter implements Runnable, EventSequence {
        private final int             connectMethod;
        public static final int       CONNECT_ONLY = 1;
        public static final int       CONNECT_AND_SEPARATE_GET_STATE = 2;
        public static final int       CONNECT_AND_GET_STATE = 3;
        protected int                 msgCount = 0;
        protected final StringBuilder events=new StringBuilder();
        protected final Semaphore     semaphore;
        protected final JChannel      channel;
        protected Thread              thread;
        protected Exception           exception;

        protected FlushTestReceiver(String name, Semaphore semaphore, int msgCount,
                        int connectMethod) throws Exception {
            this.semaphore=semaphore;
            this.connectMethod = connectMethod;
            this.msgCount = msgCount;
            this.channel=createChannel(name);
            this.channel.setReceiver(this);
            if (connectMethod == CONNECT_ONLY || connectMethod == CONNECT_AND_SEPARATE_GET_STATE)
                channel.connect("FlushTestReceiver");

            if (connectMethod == CONNECT_AND_GET_STATE) {
                channel.connect("FlushTestReceiver", null, 25000);
            }
        }

        public void start() {
            thread=new Thread(this);
            thread.start();
        }

        public void cleanup() {
            Util.close(channel);
            thread.interrupt();
        }

        public String    getEventSequence()   {return events.toString();}
        public Exception getException()       {return exception;}
        public JChannel  getChannel()         {return channel;}
        public String    getName()            {return channel != null? channel.getName() : "n/a";}
        public void      block()              {events.append('b');}
        public void      unblock()            {events.append('u');}
        public void      viewAccepted(View v) {events.append('v');}

        public void getState(OutputStream ostream) throws Exception {
            events.append('g');
            byte[] payload ={ 'b', 'e', 'l', 'a' };
            ostream.write(payload);
        }

        public void setState(InputStream istream) throws Exception {
            events.append('s');
            byte[] payload = new byte[4];
            istream.read(payload);
        }

        public void run() {
            try {
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
            catch(Exception ex) {
                exception=ex;
            }
        }
    }

    private static class SimpleReplier extends ReceiverAdapter {
        protected final JChannel channel;
        protected boolean       handle_requests=false;

        public SimpleReplier(JChannel channel, boolean handle_requests) {
            this.channel = channel;
            this.handle_requests = handle_requests;
        }

        public void receive(Message msg) {
            Message reply = new Message(msg.getSrc());
            try {
                System.out.println("-- MySimpleReplier[" + channel.getAddress() + "]: received message from " + msg.getSrc());
                if (handle_requests) {
                    System.out.println(", sending reply");
                    channel.send(reply);
                } else
                    System.out.println("\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void viewAccepted(View new_view) {
            System.out.println("-- MySimpleReplier[" + channel.getAddress() + "]: viewAccepted(" + new_view + ")");
        }

        public void block() {
            System.out.println("-- MySimpleReplier[" + channel.getAddress() + "]: block()");
        }

        public void unblock() {
            System.out.println("-- MySimpleReplier[" + channel.getAddress() + "]: unblock()");
        }
    }

    interface EventSequence {
        /** Return an event string. Events are translated as follows: get state='g', set state='s',
         *  block='b', unlock='u', view='v' */
        String getEventSequence();
        String getName();
    }

}