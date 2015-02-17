package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the behavior of receiving a unicast message before being connected and sending a response which will
 * throw an exception as the channel is not connected yet (https://issues.jboss.org/browse/JGRP-1545).
 * @author Bela Ban
 * @since 3.3
 */
@Test(groups=Global.BYTEMAN,singleThreaded=true)
public class MessageBeforeConnectedTest extends BMNGRunner {
    protected JChannel            a;
    protected static final String HELLO1="hello-1";
    protected static final String HELLO2="hello-2";
    protected Throwable           ex;
    protected final List<String>  msgs=new ArrayList<>();

    @AfterMethod
    protected void cleanup() {
        Util.close(a);
    }

    protected void receive(Message msg) {
        String greeting=(String)msg.getObject();
        msgs.add(greeting);
        System.out.println("received " + greeting + " from " + msg.getSrc());
        if(HELLO1.equals(greeting)) {
            try {
                a.send(new Message(a.getAddress(), HELLO2));
            }
            catch(Exception e) {
                ex=e;
            }
        }
    }

    /**
     * When we connect to a channel, but before the state is changed to 'connected', we send a message which will
     * trigger an exception.
     * Issue: https://issues.jboss.org/browse/JGRP-1545
     */
    @BMScript(dir="scripts/MessageBeforeConnectedTest", value="testSendingOfMsgsOnUnconnectedChannel")
    public void testSendingOfMsgsOnUnconnectedChannel() throws Throwable {
        a=createChannel("A");
        a.setReceiver(new ReceiverAdapter() {
            public void receive(Message msg)  {
                MessageBeforeConnectedTest.this.receive(msg);
            }
        });
        a.connect("MessageBeforeConnectedTest");
        System.out.println("\nA: " + a.getView());
        if(ex != null)
            throw ex;

        System.out.println("msgs = " + msgs);
        for(int i=0; i < 20; i++) {
            if(msgs.size() == 2)
                break;
            Util.sleep(500);
        }

        assert msgs.size() == 2 && msgs.contains(HELLO1) && msgs.contains(HELLO2) : "msgs: " + msgs;
    }



    protected JChannel createChannel(String name) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new SHARED_LOOPBACK_PING(),
                                 new NAKACK2().setValue("become_server_queue_size", 10),
                                 new UNICAST3(),
                                 new GMS().setValue("print_local_addr", false));
        ch.setName(name);
        return ch;
    }



}
