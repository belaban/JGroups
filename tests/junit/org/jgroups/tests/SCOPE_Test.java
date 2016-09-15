package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.SCOPE;
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
 */
@Test(groups={Global.STACK_DEPENDENT,Global.EAP_EXCLUDED},singleThreaded=true)
public class SCOPE_Test extends ChannelTestBase {
    protected JChannel          a, b;
    protected static final int  NUM_MSGS=5;
    protected static final long SLEEP_TIME=1000L;

    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel(true, 2, "A");
        b=createChannel(a, "B");
    }

    @AfterMethod void tearDown() throws Exception {Util.close(b,a);}


    public void testRegularMulticastMessages() throws Exception {
        sendMessages(null, false);
    }

    public void testScopedMulticastMessages() throws Exception {
        sendMessages(null, true);
    }

    public void testRegularUnicastMessages() throws Exception {
        sendMessages(b.getAddress(), false);
    }

    public void testScopedUnicastMessages() throws Exception {
        sendMessages(b.getAddress(), true);
    }

    public void testOrderWithScopedMulticasts() throws Exception {
        ProtocolStack stack=b.getProtocolStack();
        Protocol neighbor=stack.findProtocol(Util.getUnicastProtocols());
        SCOPE scope=new SCOPE();
        stack.insertProtocolInStack(scope, neighbor, ProtocolStack.ABOVE);
        scope.init();

        a.connect("SCOPE_Test");
        b.connect("SCOPE_Test");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b);

        MyScopedReceiver receiver=new MyScopedReceiver();
        b.setReceiver(receiver);
        Short[] scopes={'X', 'Y', 'Z'};

        for(short scope_id: scopes) {
            for(long i=1; i <=5; i++) {
                Message msg=new Message(null, null, i);
                msg.setScope(scope_id);
                System.out.println("-- sending message " + (char)scope_id + "#" + i);
                a.send(msg);
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



    protected void sendMessages(Address dest, boolean use_scopes) throws Exception {
        if(use_scopes) {
            ProtocolStack stack=b.getProtocolStack();
            Protocol neighbor=stack.findProtocol(Util.getUnicastProtocols());
            SCOPE scope=new SCOPE();
            stack.insertProtocolInStack(scope, neighbor, ProtocolStack.ABOVE);
            scope.init();
        }

        a.connect("SCOPE_Test");
        b.connect("SCOPE_Test");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000,a,b);

        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);

        for(long i=1; i <= NUM_MSGS; i++) {
            Message msg=new Message(dest, i);
            if(use_scopes)
                msg.setScope((short)i);
            System.out.println("-- sending message #" + i);
            a.send(msg);
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
        final ConcurrentMap<Short,List<Long>> msgs=new ConcurrentHashMap<>();

        public void receive(Message msg) {
            Util.sleep(SLEEP_TIME);
            Short scope=msg.getScope();
            if(scope > 0) {
                List<Long> list=msgs.get(scope);
                if(list == null) {
                    list=new ArrayList<>(5);
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