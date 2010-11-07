package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.SCOPE;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tests the SCOPE protocol (https://jira.jboss.org/jira/browse/JGRP-822) 
 * @author Bela Ban
 * @version $Id: SCOPE_Test.java,v 1.1 2010/03/29 06:43:29 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class SCOPE_Test extends ChannelTestBase {
    JChannel c1, c2;
    static final int NUM_MSGS=5;
    static final long SLEEP_TIME=1000L;

    @BeforeMethod
    void setUp() throws Exception {
        c1=createChannel(true, 2); c1.setName("A");
        c2=createChannel(c1);      c2.setName("B");
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c2, c1);
    }


    public void testRegularMulticastMessages() throws Exception {
        sendMessages(null, false);
    }

    public void testScopedMulticastMessages() throws Exception {
        sendMessages(null, true);
    }

    public void testRegularUnicastMessages() throws Exception {
        sendMessages(c2.getAddress(), false);
    }

    public void testScopedUnicastMessages() throws Exception {
        sendMessages(c2.getAddress(), true);
    }

    public void testOrderWithScopedMulticasts() throws Exception {
        ProtocolStack stack=c2.getProtocolStack();
        Protocol neighbor=stack.findProtocol(UNICAST.class, UNICAST2.class);
        SCOPE scope=new SCOPE();
        stack.insertProtocolInStack(scope, neighbor, ProtocolStack.ABOVE);
        scope.init();

        c1.connect("SCOPE_Test");
        c2.connect("SCOPE_Test");
        assert c2.getView().size() == 2 : "c2.view is " + c2.getView();

        MyScopedReceiver receiver=new MyScopedReceiver();
        c2.setReceiver(receiver);
        Short[] scopes=new Short[]{'X', 'Y', 'Z'};

        for(short scope_id: scopes) {
            for(long i=1; i <=5; i++) {
                Message msg=new Message(null, null, i);
                msg.setScope(scope_id);
                System.out.println("-- sending message " + (char)scope_id + "#" + i);
                c1.send(msg);
            }
        }

        long target_time=System.currentTimeMillis() + NUM_MSGS * SLEEP_TIME * 2, start=System.currentTimeMillis();
        do {
            if(receiver.size() >= NUM_MSGS * scopes.length)
                break;
            Util.sleep(100);
            System.out.print(".");
        }
        while(target_time > System.currentTimeMillis());

        long time=System.currentTimeMillis() - start;

        ConcurrentMap<Short,List<Long>> msgs=receiver.getMsgs();
        System.out.println("seqnos:");
        for(Map.Entry<Short,List<Long>> entry: msgs.entrySet()) {
            short tmp=entry.getKey();
            System.out.println((char)tmp + ": " + entry.getValue());
        }
        System.out.println(receiver.size() + " msgs in " + time + " ms");

        assert receiver.size() == NUM_MSGS * scopes.length;

        assert time >= NUM_MSGS * SLEEP_TIME && time < NUM_MSGS *SLEEP_TIME * 2;

        System.out.println("checking order within the scopes:");

        for(short scope_id: scopes) {
            List<Long> list=msgs.get(scope_id);
            for(int i=0; i < NUM_MSGS; i++)
                assert (i+1) == list.get(i);
        }

        System.out.println("OK, order is correct");
    }



    private void sendMessages(Address dest, boolean use_scopes) throws Exception {
        if(use_scopes) {
            ProtocolStack stack=c2.getProtocolStack();
            Protocol neighbor=stack.findProtocol(UNICAST.class, UNICAST2.class);
            SCOPE scope=new SCOPE();
            stack.insertProtocolInStack(scope, neighbor, ProtocolStack.ABOVE);
            scope.init();
        }

        c1.connect("SCOPE_Test");
        c2.connect("SCOPE_Test");
        assert c2.getView().size() == 2 : "c2.view is " + c2.getView();

        MyReceiver receiver=new MyReceiver();
        c2.setReceiver(receiver);

        for(long i=1; i <=5; i++) {
            Message msg=new Message(dest, null, i);
            if(use_scopes)
                msg.setScope((short)i);
            System.out.println("-- sending message #" + i);
            c1.send(msg);
        }

        long target_time=System.currentTimeMillis() + NUM_MSGS * SLEEP_TIME * 2, start=System.currentTimeMillis();
        do {
            if(receiver.size() >= NUM_MSGS)
                break;
            Util.sleep(100);
            System.out.print(".");
        }
        while(target_time > System.currentTimeMillis());

        long time=System.currentTimeMillis() - start;

        List<Long> seqnos=receiver.getSeqnos();
        System.out.println("\nsequence numbers: " + seqnos + " in " + time + " ms");

        assert seqnos.size() == NUM_MSGS;

        if(use_scopes) {
            assert time > SLEEP_TIME * 1 && time < NUM_MSGS * SLEEP_TIME;
            for(int i=0; i < NUM_MSGS; i++)
                assert seqnos.contains((long)i+1);
        }
        else {
            assert time >= NUM_MSGS * SLEEP_TIME;
            for(int i=0; i < NUM_MSGS; i++)
                assert (i+1) == seqnos.get(i);
        }
    }




    public static class MyReceiver extends ReceiverAdapter {
        /** List<Long> of unicast sequence numbers */
        final List<Long> seqnos=Collections.synchronizedList(new LinkedList<Long>());

        public MyReceiver() {
        }

        public List<Long> getSeqnos() {
            return seqnos;
        }

        public void receive(Message msg) {
            Util.sleep(SLEEP_TIME);
            Long num=(Long)msg.getObject();
            seqnos.add(num);
        }

        public int size() {return seqnos.size();}
    }


    public static class MyScopedReceiver extends ReceiverAdapter {
        final ConcurrentMap<Short,List<Long>> msgs=new ConcurrentHashMap<Short,List<Long>>();

        public void receive(Message msg) {
            Util.sleep(SLEEP_TIME);
            Short scope=msg.getScope();
            if(scope.shortValue() > 0) {
                List<Long> list=msgs.get(scope);
                if(list == null) {
                    list=new ArrayList<Long>(5);
                    List<Long> tmp=msgs.putIfAbsent(scope, list);
                    if(tmp != null)
                        list=tmp;
                }
                list.add((Long)msg.getObject());
            }
        }

        public ConcurrentMap<Short, List<Long>> getMsgs() {
            return msgs;
        }

        public int size() {
            int retval=0;
            for(List<Long> list: msgs.values())
                retval+=list.size();
            return retval;
        }
    }

}