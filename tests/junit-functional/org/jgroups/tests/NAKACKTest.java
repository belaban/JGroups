
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;


/**
 * Tests the NAKACK protocol
 * @author Bela Ban
 * @version $Id: NAKACKTest.java,v 1.1 2009/03/16 11:12:27 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class NAKACKTest {
    Simulator s=null;
    NAKACK nakack;
    final Address local_addr=new IpAddress(1000);
    final Address sender_addr=new IpAddress(5000);



    @BeforeMethod
    void setUp() throws Exception {
        Vector members=new Vector();
        members.add(local_addr);
        members.add(sender_addr);
        View v=new View(local_addr, 1, members);
        s=new Simulator();
        s.setLocalAddress(local_addr);
        s.setView(v);
        s.addMember(local_addr);
        nakack=new NAKACK();
        Protocol[] stack=new Protocol[]{nakack};
        s.setProtocolStack(stack);
        s.start();
    }

    @AfterMethod
    void tearDown() throws Exception {
        s.stop();
    }


    public void testReceptionOfRegularMessages() {
        Receiver receiver=new Receiver();
        s.setReceiver(receiver);

        List<Message> list=createMessages(5, null, sender_addr, false);
        Collections.reverse(list); // send in reverse order
        for(Message msg: list) {
            nakack.up(new Event(Event.MSG, msg));
        }

        List<Message> msgs=receiver.getMsgs();
        assert msgs.size() == 5 : "msgs: " + msgs;
        int counter=1;
        for(Message msg: msgs) { // check correct order
            assert msg.getObject().equals(counter++);
        }
    }


    public void testReceptionOfRegularAndOOBMessages() {
        Receiver receiver=new Receiver();
        s.setReceiver(receiver);

        List<Message> list=createMessages(5, null, sender_addr, false);
        list.get(3).setFlag(Message.OOB);

        // send the first 3 in the correct order
        for(int i=0; i < 3; i++)
            nakack.up(new Event(Event.MSG, list.get(i)));

        // then send #5
        nakack.up(new Event(Event.MSG, list.get(4)));

        // finally send #4, but as OOB !
        nakack.up(new Event(Event.MSG, list.get(3)));

        List<Message> msgs=receiver.getMsgs();
        assert msgs.size() == 5 : "msgs: " + msgs;
        // we cannot check for ordering with the OOB message being present        
    }

    public void testReceptionOfRegularAndOOBMessagesThreaded() {
        Receiver receiver=new Receiver();
        s.setReceiver(receiver);

        final List<Message> list=createMessages(5, null, sender_addr, false);
        list.get(3).setFlag(Message.OOB);

        // send the first 3 in the correct order
        new Thread() {
            public void run() {
                Util.sleepRandom(1000);
                for(int i=0; i < 3; i++)
                    nakack.up(new Event(Event.MSG, list.get(i)));

            }
        }.start();

        // then send #5
        new Thread() {
            public void run() {
                Util.sleepRandom(1000);
                nakack.up(new Event(Event.MSG, list.get(4)));
            }
        }.start();


        // finally send #4, but as OOB !
        new Thread() {
            public void run() {
                Util.sleepRandom(1000);
                nakack.up(new Event(Event.MSG, list.get(3)));
            }
        }.start();

        Util.sleep(1200);

        List<Message> msgs=receiver.getMsgs();
        assert msgs.size() == 5 : "msgs: " + msgs;
        // we cannot check for ordering with the OOB message being present
    }


    private static List<Message> createMessages(int num, Address dest, Address sender, boolean oob) {
        List<Message> retval=new ArrayList<Message>(num);
        for(int i=1; i <= num; i++) {
            Message msg=new Message(dest, sender, new Integer(i));
            if(oob)
                msg.setFlag(Message.OOB);
            NakAckHeader hdr=new NakAckHeader(NakAckHeader.MSG, i);
            msg.putHeader("NAKACK", hdr);
            retval.add(msg);
        }

        return retval;
    }


    static class Receiver implements Simulator.Receiver {
        private List<Message> msgs=new LinkedList<Message>();

        public void receive(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                System.out.println("received message from " + msg.getSrc() + ": " + msg.getObject());
                msgs.add(msg);
            }
        }

        public List<Message> getMsgs() {
            return msgs;
        }

        public void clear() {msgs.clear();}

    }



}