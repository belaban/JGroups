package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.text.NumberFormat;

/**
 * Simple protocol to test round trip times. Requests are [PING], responses are [PONG]. Start multiple instances
 * and press <return> to get the round trip times for all nodes in the cluster
 * @author Bela Ban
 * @version $Id: PingPong.java,v 1.1 2010/02/10 09:01:20 belaban Exp $
 */
public class PingPong extends ReceiverAdapter {
    JChannel ch;

    static final byte PING = 1;
    static final byte PONG = 2;

    static final byte[] PING_REQ=new byte[]{PING};
    static final byte[] PONG_RSP=new byte[]{PONG};

    long start=0;

    private static NumberFormat f;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }


    public void start(String props) throws ChannelException {
        ch=new JChannel(props);
        ch.setReceiver(this);
        ch.connect("ping");

        while(true) {
            Util.keyPress("enter to ping");
            Message msg=new Message(null, null, PING_REQ);
            msg.setFlag(Message.DONT_BUNDLE);
            msg.setFlag(Message.NO_FC);
            start=System.nanoTime();
            ch.send(msg);
        }
    }

    public void viewAccepted(View new_view) {
        System.out.println("view: " + new_view);
    }

    public void receive(Message msg) {
        byte type=msg.getRawBuffer()[0];
        switch(type) {
            case PING:
                Message rsp=new Message(msg.getSrc(), null, PONG_RSP);
                rsp.setFlag(Message.DONT_BUNDLE);
                rsp.setFlag(Message.NO_FC);
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
                System.out.println("RTT for " + msg.getSrc() + ": " + f.format(ms) + " ms");
                break;
        }
    }

    public static void main(String[] args) throws ChannelException {
        String props="udp.xml";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            System.out.println("PingPong [-props <XML config>]");
        }

        new PingPong().start(props);
    }
}
