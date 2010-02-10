package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple protocol to test round trip times. Requests are [PING], responses are [PONG]. Start multiple instances
 * and press <return> to get the round trip times for all nodes in the cluster
 * @author Bela Ban
 * @version $Id: PingPong.java,v 1.4 2010/02/10 12:14:49 belaban Exp $
 */
public class PingPong extends ReceiverAdapter {
    JChannel ch;

    static final byte PING = 1;
    static final byte PONG = 2;

    static final byte[] PING_REQ=new byte[]{PING};
    static final byte[] PONG_RSP=new byte[]{PONG};

    long start=0;

    final List<Address> members=new ArrayList<Address>();

    private static NumberFormat f;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
        ClassConfigurator.add((short)5000, PerfHeader.class);
    }


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
            msg.setFlag(Message.DONT_BUNDLE);
            msg.setFlag(Message.NO_FC);
            PerfHeader hdr=new PerfHeader();
            msg.putHeader("PERF", hdr);
            start=System.nanoTime();
            hdr.request_sent=start;
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
        PerfHeader hdr=(PerfHeader)msg.getHeader("PERF");
        switch(type) {
            case PING:
                hdr.request_received=System.nanoTime();
                Message rsp=new Message(msg.getSrc(), null, PONG_RSP);
                rsp.putHeader("PERF", hdr);
                rsp.setFlag(Message.DONT_BUNDLE);
                rsp.setFlag(Message.NO_FC);
                hdr.response_sent=System.nanoTime();
                try {
                    ch.send(rsp);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case PONG:
                hdr.response_received=System.nanoTime();
                long rtt=System.nanoTime() - start;
                double ms=rtt / 1000.0 / 1000.0;
                System.out.println("RTT for " + msg.getSrc() + ": " + f.format(ms) + " ms" +
                        ", perf: " + hdr.print());

                break;
        }
    }

    public static class PerfHeader extends Header implements Streamable {
        private static final long serialVersionUID=-241191876873521101L;

        // all times in nanoseconds
        long request_sent;
        long request_received;
        long response_sent;
        long response_received;


        public PerfHeader() {
        }

        public PerfHeader(long request_sent, long request_received, long response_sent, long response_received) {
            this.request_sent=request_sent;
            this.request_received=request_received;
            this.response_sent=response_sent;
            this.response_received=response_received;
        }

        public PerfHeader(PerfHeader hdr) {
            this.request_sent=hdr.request_sent;
            this.request_received=hdr.request_received;
            this.response_sent=hdr.response_sent;
            this.response_received=hdr.response_received;
        }

        public void writeExternal(ObjectOutput out) throws IOException {

        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }


        public void writeTo(DataOutputStream out) throws IOException {
            out.writeLong(request_sent);
            out.writeLong(request_received);
            out.writeLong(response_sent);
            out.writeLong(response_received);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            request_sent=in.readLong();
            request_received=in.readLong();
            response_sent=in.readLong();
            response_received=in.readLong();
        }

        public int size() {
            return Global.LONG_SIZE * 4;
        }


        public String print() {
            long request_time=request_received - request_sent;
            long response_time=response_received - response_sent;

            double req_ms=request_time / 1000.0 / 1000.0;
            double rsp_ms=response_time / 1000.0 / 1000.0;
            return "request time: " + f.format(req_ms) + " ms, response time: " + f.format(rsp_ms) + " ms";
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
        }

        new PingPong().start(props, name, unicast);
    }
}
