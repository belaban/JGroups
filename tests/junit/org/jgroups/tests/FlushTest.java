package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;


/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and configured to use FLUSH
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.13 2006/10/05 07:12:56 belaban Exp $
 */
public class FlushTest extends TestCase {
    Channel c1, c2,c3;
    static final String CONFIG="flush-udp.xml";


    public FlushTest(String name) {
        super(name);
    }

    public void tearDown() throws Exception {
        super.tearDown();

        if(c2 != null) {
            c2.close();
            Util.sleep(1000);
            c2=null;
        }

        if(c1 != null) {
            c1.close();
            Util.sleep(1000);
            c1=null;
        }

        if(c3 != null) {
            c3.close();
            Util.sleep(1000);
            c3=null;
        }
    }


    public void testSingleChannel() throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        Util.sleep(1000);
        checkEventSequence(receiver);
    }

    /**
     * Tests issue #1 in http://jira.jboss.com/jira/browse/JGRP-335
     */
    public void testJoinFollowedByUnicast() throws ChannelException {
        c1=createChannel();
        c1.setReceiver(new MySimpleReplier(c1, true));
        c1.connect("bla");

        Address target=c1.getLocalAddress();
        Message unicast_msg=new Message(target);

        c2=createChannel();
        c2.setReceiver(new MySimpleReplier(c2, false));
        c2.connect("bla");

        // now send unicast, this might block as described in the case
        c2.send(unicast_msg);
        // if we don't get here this means we'd time out
    }


    /**
     * Tests issue #2 in http://jira.jboss.com/jira/browse/JGRP-335
     */
    public void testStateTransferFollowedByUnicast() throws ChannelException {
        c1=createChannel();
        c1.setReceiver(new MySimpleReplier(c1, true));
        c1.connect("bla");

        Address target=c1.getLocalAddress();
        Message unicast_msg=new Message(target);

        c2=createChannel();
        c2.setReceiver(new MySimpleReplier(c2, false));
        c2.connect("bla");

        Util.sleep(100);
        System.out.println("\n** Getting the state **");
        c2.getState(null, 10000);
        // now send unicast, this might block as described in the case
        c2.send(unicast_msg);
    }

    public void testTwoChannelsWithMessages() throws ChannelException {
        twoChannelsTestHelper(true);
    }

    public void testTwoChannelsNoMessages() throws ChannelException {
        twoChannelsTestHelper(false);
    }

    public void testThreeChannelsWithMessages() throws ChannelException {
        threeChannelsTestHelper(true);
    }

    public void testThreeChannelsNoMessages() throws ChannelException {
        threeChannelsTestHelper(false);
    }

    public void testStateTransferWithMessages() throws ChannelException {
        stateTransferTestHelper(true);
    }

    public void testStateTransferNoMessages() throws ChannelException {
        stateTransferTestHelper(false);
    }

    private void twoChannelsTestHelper(boolean sendMessages) throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        if(sendMessages){
            c1.send(new Message());
        }
        Util.sleep(1000);

        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        View view=c2.getView();
        assertEquals(2, view.size());
        Util.sleep(1000);
        if(sendMessages){
            c1.send(new Message());
            c2.send(new Message());
        }

        checkEventSequence(receiver2);

        c2.close();
        Util.sleep(500);
        if(sendMessages){
            c1.send(new Message());
        }
        Util.sleep(1000);

        checkEventSequence(receiver);
    }


    private void threeChannelsTestHelper(boolean sendMessages) throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        if(sendMessages){
            c1.send(new Message());
        }

        Util.sleep(1000);

        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        View view=c2.getView();
        assertEquals(2, view.size());
        Util.sleep(1000);

        if(sendMessages){
            c1.send(new Message());
            c2.send(new Message());
        }

        c3=createChannel();
        MyReceiver receiver3=new MyReceiver("c3");
        c3.setReceiver(receiver3);
        c3.connect("bla");
        view=c3.getView();
        assertEquals(3, view.size());
        Util.sleep(1000);
        if(sendMessages){
            c1.send(new Message());
            c2.send(new Message());
            c3.send(new Message());
        }

        //close coordinator
        checkEventSequence(receiver);

        c1.close();
        if(sendMessages){
            c2.send(new Message());
            c2.send(new Message());
        }
        Util.sleep(1000);

        //close coordinator one more time
        checkEventSequence(receiver2);
        c2.close();
        Util.sleep(1000);

        checkEventSequence(receiver3);
    }



    private void stateTransferTestHelper(boolean sendMessages) throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver("c1");
        c1.setReceiver(receiver);
        c1.connect("bla");
        Util.sleep(1000);
        if(sendMessages){
            c1.send(new Message());
            c1.send(new Message());
        }

        Util.sleep(1000);

        c2=createChannel();
        MyReceiver receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        Util.sleep(1000);


        c3=createChannel();
        MyReceiver receiver3=new MyReceiver("c3");
        c3.setReceiver(receiver3);
        c3.connect("bla");
        Util.sleep(1000);
        if(sendMessages){
            c1.send(new Message());
            c2.send(new Message());
        }

        Util.sleep(500);
        if(sendMessages){
            c1.send(new Message());
            c2.send(new Message());
            c3.send(new Message());
        }

        checkEventSequence(receiver);
        checkEventSequence(receiver2);
        checkEventSequence(receiver3);

        System.out.println("=== fetching the state ====");
        c2.getState(null, 10000);
        Util.sleep(2000);
        if(sendMessages){
            c1.send(new Message());
            c1.send(new Message());
            c2.send(new Message());
            c2.send(new Message());
        }

        checkNonStateTransferMemberSequence(receiver3);
        checkBlockStateUnBlockSequence(receiver);
        checkBlockStateUnBlockSequence(receiver2);

        c2.close();
        c2=null;
        Util.sleep(2000);

        c2=createChannel();
        receiver2=new MyReceiver("c2");
        c2.setReceiver(receiver2);
        c2.connect("bla");
        Util.sleep(2000);
        if(sendMessages){
            c1.send(new Message());
        }

        checkEventSequence(receiver);
        checkEventSequence(receiver2);
        checkEventSequence(receiver3);

        System.out.println("=== fetching the state ====");
        c3.getState(null, 10000);
        if(sendMessages){
            c1.send(new Message());
            c2.send(new Message());
        }
        Util.sleep(2000);

        checkNonStateTransferMemberSequence(receiver2);
        checkBlockStateUnBlockSequence(receiver);
        checkBlockStateUnBlockSequence(receiver3);
    }



    private void checkBlockStateUnBlockSequence(MyReceiver receiver) {
        List events = receiver.getEvents();
        String name = receiver.getName();
        assertNotNull(events);
        assertEquals("Should have three events [block,get|setstate,unblock] but " + name + " has "
                + events, 3, events.size());
        Object obj=events.remove(0);
        assertTrue(name, obj instanceof BlockEvent);
        obj=events.remove(0);
        assertTrue(name, obj instanceof GetStateEvent || obj instanceof SetStateEvent);
        obj=events.remove(0);
        assertTrue(name, obj instanceof UnblockEvent);
        receiver.clear();
    }

    private void checkEventSequence(MyReceiver receiver) {
       List events = receiver.getEvents();
       assertNotNull(events);
       int size = events.size();
       for (int i = 0; i < size; i++)
       {
          Object event = events.get(i);
          if(event instanceof BlockEvent)
          {
             if(i+1<size)
             {
                assertTrue("After Block should be View ",events.get(i+1) instanceof View);
             }
             if(i!=0)
             {
                assertTrue("Before Block should be Unblock ",events.get(i-1) instanceof UnblockEvent);
             }
          }
          if(event instanceof View)
          {
             if(i+1<size)
             {
                assertTrue("After View should be Unblock ",events.get(i+1) instanceof UnblockEvent);
             }
             assertTrue("Before View should be Block ",events.get(i-1) instanceof BlockEvent);
          }
          if(event instanceof UnblockEvent)
          {
             if(i+1<size)
             {
                assertTrue("After UnBlock should be Block ",events.get(i+1) instanceof BlockEvent);
             }
             assertTrue("Before UnBlock should be View ",events.get(i-1) instanceof View);
          }


       }
       receiver.clear();
   }

    private void checkNonStateTransferMemberSequence(MyReceiver receiver) {
        List events = receiver.getEvents();
        assertNotNull(events);
        assertEquals("Should have two events [block,unblock] but " + receiver.getName() + " has "
                        + events, 2, events.size());

        Object obj = events.remove(0);
        assertTrue(obj instanceof BlockEvent);
        obj = events.remove(0);
        assertTrue(obj instanceof UnblockEvent);
        receiver.clear();
    }

    private Channel createChannel() throws ChannelException {
        Channel ret=new JChannel(CONFIG);
        ret.setOpt(Channel.BLOCK, Boolean.TRUE);
        Protocol flush=((JChannel)ret).getProtocolStack().findProtocol("FLUSH");
        if(flush != null) {
            Properties p=new Properties();
            p.setProperty("timeout", "0");
            flush.setProperties(p);

            // send timeout up and down the stack, so other protocols can use the same value too
            Map map = new HashMap();
            map.put("flush_timeout", new Long(0));
            flush.passUp(new Event(Event.CONFIG, map));
            flush.passDown(new Event(Event.CONFIG, map));
        }
        return ret;
    }


    public static Test suite() {
        return new TestSuite(FlushTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(FlushTest.suite());
    }

    private static class MyReceiver extends ExtendedReceiverAdapter {
        List events;
        String name;
        boolean verbose = true;

        public MyReceiver(String name) {
            this.name=name;
            events=Collections.synchronizedList(new LinkedList());
        }

        public String getName()
        {
            return name;
        }

        public void clear() {
            events.clear();
        }

        public List getEvents() {return new LinkedList(events);}

        public void block() {
            if(verbose)
                System.out.println("[" + name + "]: BLOCK");
            events.add(new BlockEvent());
        }

        public void unblock() {
            if(verbose)
                System.out.println("[" + name + "]: UNBLOCK");
            events.add(new UnblockEvent());
        }

        public void viewAccepted(View new_view) {
            if(verbose)
                System.out.println("[" + name + "]: " + new_view);
            events.add(new_view);
        }

        public byte[] getState() {
            if(verbose)
                System.out.println("[" + name + "]: GetStateEvent");
            events.add(new GetStateEvent(null, null));
            return new byte[]{'b', 'e', 'l', 'a'};
        }

        public void setState(byte[] state) {
            if(verbose)
                System.out.println("[" + name + "]: SetStateEvent");
            events.add(new SetStateEvent(null, null));
        }

        public void getState(OutputStream ostream) {
            if(verbose)
                System.out.println("[" + name + "]: GetStateEvent streamed");
            events.add(new GetStateEvent(null, null));
            try {
                ostream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        public void setState(InputStream istream) {
            if(verbose)
                System.out.println("[" + name + "]: SetStateEvent streamed");
            events.add(new SetStateEvent(null, null));
            try {
                istream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class MySimpleReplier extends ExtendedReceiverAdapter {
        Channel channel;
        boolean handle_requests=false;

        public MySimpleReplier(Channel channel, boolean handle_requests) {
            this.channel=channel;
            this.handle_requests=handle_requests;
        }

        public void receive(Message msg) {
            Message reply=new Message(msg.getSrc());
            try {
                System.out.print("-- MySimpleReplier[" + channel.getLocalAddress() + "]: received message from " + msg.getSrc());
                if(handle_requests) {
                    System.out.println(", sending reply");
                    channel.send(reply);
                }
                else
                    System.out.println("\n");
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void viewAccepted(View new_view) {
            System.out.println("-- MySimpleReplier[" + channel.getLocalAddress() + "]: viewAccepted(" + new_view + ")");
        }

        public void block() {
            System.out.println("-- MySimpleReplier[" + channel.getLocalAddress() + "]: block()");
        }

        public void unblock() {
            System.out.println("-- MySimpleReplier[" + channel.getLocalAddress() + "]: unblock()");
        }


    }
}
