package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.*;
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
@Test(groups=Global.BYTEMAN,singleThreaded=true)
public class BecomeServerTest extends BMNGRunner {
    protected JChannel          a, b;

    @AfterMethod protected void cleanup() {Util.close(b,a);}

    /**
     * When we flush the server queue and one or more of the delivered messages triggers a response (in the same thread),
     * we need to make sure the channel is connected, or else the JOIN will fail as the exception happens on the same
     * thread. Note that the suggested fix on JGRP-1522 will solve this. Issue: https://issues.jboss.org/browse/JGRP-1522
     */
    @BMScript(dir="scripts/BecomeServerTest", value="testSendingOfMsgsOnUnconnectedChannel")
    public void testSendingOfMsgsOnUnconnectedChannel() throws Exception {
        a=createChannel("A");
        a.setReceiver(new Receiver() {
            public void receive(Message msg)  {
                System.out.println("A: received message from " + msg.getSrc() + ": " + msg.getObject());
            }
        });
        a.connect("BecomeServerTest");

        Thread t=new Thread(() -> sendMessage(a, "hello from A"), // will be blocked by byteman rendezvous
                            "MsgSender-A");

        b=createChannel("B");
        b.setReceiver(new Receiver() {
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

        t.start();

        b.connect("BecomeServerTest");

        Util.waitUntilAllChannelsHaveSameView(20000, 1000, a, b);

        System.out.println("\nA: " + a.getView() + "\nB: " + b.getView());
    }


    protected void sendMessage(JChannel ch, String message) {
        try {
            ch.send(new ObjectMessage(null, message).setFlag(Message.Flag.OOB));
        }
        catch(Exception e) {
            e.printStackTrace(System.err);
        }
    }



    protected static JChannel createChannel(String name) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                       new PING(),
                                       new NAKACK2().setBecomeServerQueueSize(10),
                                       new UNICAST3(),
                                       new GMS().printLocalAddress(false).setJoinTimeout((500)));
        ch.setName(name);
        return ch;
    }
}
