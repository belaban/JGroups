package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the behavior of queueing messages (in NAKACK{2}) before becoming a server
 * (https://issues.jboss.org/browse/JGRP-1522)
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.BYTEMAN,sequential=true)
public class BecomeServerTest extends BMNGRunner {
    JChannel          a, b;

    @AfterMethod
    protected void cleanup() {
        Util.close(b,a);
    }


    /**
     * When we flush the server queue and one or more of the delivered messages triggers a response (in the same thread),
     * we need to make sure the channel is connected, or else the JOIN will fail as the exception happens on the same
     * thread. Note that the suggested fix on JGRP-1522 will solve this. Issue: https://issues.jboss.org/browse/JGRP-1522
     */
    @BMScript(dir="scripts/BecomeServerTest", value="testSendingOfMsgsOnUnconnectedChannel")
    public void testSendingOfMsgsOnUnconnectedChannel() throws Exception {
        a=createChannel("A");
        a.setReceiver(new ReceiverAdapter() {
            public void receive(Message msg)  {
                System.out.println("A: received message from " + msg.getSrc() + ": " + msg.getObject());
            }
        });
        a.connect("BecomeServerTest");

        new Thread("MsgSender-A") {
            public void run() {
                sendMessage(a, "hello from A"); // will be blocked by byteman rendezvous
            }
        }.start();

        b=createChannel("B");
        b.setReceiver(new ReceiverAdapter() {
            public void receive(Message msg) {
                System.out.println("B: received message from " + msg.getSrc() + ": " + msg.getObject());
                if(msg.getSrc().equals(a.getAddress())) {
                    try {
                        b.send(null, "This message would trigger an exception if the channel was not yet connected");
                    }
                    catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        b.connect("BecomeServerTest");

        Util.waitUntilAllChannelsHaveSameSize(20000, 1000, a,b);

        System.out.println("\nA: " + a.getView() + "\nB: " + b.getView());
    }


    protected void sendMessage(JChannel ch, String message) {
        try {
            Message msg=new Message(null, message);
            msg.setFlag(Message.Flag.OOB);
            ch.send(msg);
        }
        catch(Exception e) {
            e.printStackTrace(System.err);
        }
    }



    protected JChannel createChannel(String name) throws Exception {
        JChannel ch=Util.createChannel(new SHARED_LOOPBACK().setValue("enable_bundling", false)
                                         .setValue("enable_unicast_bundling", false),
                                       new PING().setValue("timeout", 500).setValue("num_initial_members", 2),
                                       new NAKACK2().setValue("become_server_queue_size", 10),
                                       new UNICAST3(),
                                       new GMS().setValue("print_local_addr", false));
        ch.setName(name);
        return ch;
    }
}
