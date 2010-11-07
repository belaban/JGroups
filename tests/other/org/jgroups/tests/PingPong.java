package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple protocol to test round trip times. Requests are [PING], responses are [PONG]. Start multiple instances
 * and press <return> to get the round trip times for all nodes in the cluster<p/>
 * See {@link org.jgroups.tests.PingPongDatagram} for the same program using MulticastSockets, and
 * {@link LatencyTest} for simple latency tests (not round trip).
 * @author Bela Ban
 * @version $Id: PingPong.java,v 1.11 2010/02/22 14:42:24 belaban Exp $
 */
public class PingPong extends ReceiverAdapter {
    JChannel ch;

    static final byte PING = 1;
    static final byte PONG = 2;

    static final byte[] PING_REQ=new byte[]{PING};
    static final byte[] PONG_RSP=new byte[]{PONG};

    long start=0;

    final List<Address> members=new ArrayList<Address>();


    public void start(String props, String name, boolean unicast) throws ChannelException {
        ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
        ch.setReceiver(this);
        ch.connect("ping");

        while(true) {
            Util.keyPress("enter to ping");
            Address dest=null;
            if(unicast)
                dest=(Address)Util.pickRandomElement(members);
            
            Message msg=new Message(dest, null, PING_REQ);
            msg.setFlag((byte)(Message.DONT_BUNDLE | Message.NO_FC));
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
        byte type=msg.getRawBuffer()[0];
        switch(type) {
            case PING:
                final Message rsp=new Message(msg.getSrc(), null, PONG_RSP);
                rsp.setFlag((byte)(Message.DONT_BUNDLE | Message.NO_FC));
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
                System.out.println("RTT for " + msg.getSrc() + ": " + Util.format(ms) + " ms");
                break;
        }
    }



    public static void main(String[] args) throws ChannelException {
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
