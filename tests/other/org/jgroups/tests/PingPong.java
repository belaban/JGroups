package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple protocol to test round trip times. Requests are [PING], responses are [PONG]. Start multiple instances
 * and press <return> to get the round trip times for all nodes in the cluster<p/>
 * See {@link org.jgroups.tests.PingPongDatagram} for the same program using MulticastSockets
 * @author Bela Ban
 */
public class PingPong extends ReceiverAdapter {
    JChannel ch;

    static final byte PING = 1;
    static final byte PONG = 2;

    static final byte[] PING_REQ={PING};
    static final byte[] PONG_RSP={PONG};

    long start=0;

    final List<Address> members=new ArrayList<>();


    public void start(String props, String name, boolean unicast) throws Exception {
        ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
        ch.setReceiver(this);
        ch.connect("ping");

        while(true) {
            Util.keyPress("enter to ping");
            Address dest=null;
            if(unicast)
                dest=Util.pickRandomElement(members);
            
            Message msg=new BytesMessage(dest, PING_REQ);
            msg.setFlag(Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
            start=System.nanoTime();
            ch.send(msg);
        }
    }

    public void viewAccepted(View new_view) {
        System.out.println("view: " + new_view);
        members.clear();
        members.addAll(new_view.getMembers());
    }

    public void receive(Message msg) {
        byte type=msg.getArray()[msg.getOffset()];
        switch(type) {
            case PING:
                final Message rsp=new BytesMessage(msg.getSrc(), PONG_RSP);
                rsp.setFlag(Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
                try {
                    ch.send(rsp);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case PONG:
                long rtt=System.nanoTime() - start;
                double ms=rtt / 1000.0 / 1000.0;
                System.out.printf("RTT for %s: %.2f ms\n", msg.getSrc(), ms);
                break;
        }
    }



    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String name=null;
        boolean unicast=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-unicast")) {
                unicast=true;
                continue;
            }
            System.out.println("PingPong [-props <XML config>] [-name name] [-unicast]");
            return;
        }

        new PingPong().start(props, name, unicast);
    }
}
