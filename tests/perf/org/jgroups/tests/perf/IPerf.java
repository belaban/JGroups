package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.*;
import java.util.Date;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests sending large messages from one sender to multiple receivers
 * @author Bela Ban
 * @version $Id: IPerf.java,v 1.1 2008/07/24 08:45:26 belaban Exp $
 */
public class IPerf {
    private final boolean sender;
    private final String props;
    private final int sleep;
    private JChannel ch;
    private final int MIN_SIZE, MAX_SIZE, FIXED_SIZE;
    private int seqno=1;
    private final static String NAME="IPerf";
    private final Map<Integer,Map<Address, Long>> stats=new ConcurrentHashMap<Integer,Map<Address,Long>>();



    public IPerf(boolean sender, String props, int sleep, int min, int max, int fixed) {
        this.sender=sender;
        this.props=props;
        this.sleep=sleep;
        this.MIN_SIZE=min;
        this.MAX_SIZE=max;
        this.FIXED_SIZE=fixed;
    }

    public void start() throws Exception {
        ch=new JChannel(props);
        ch.setReceiver(new MyReceiver(ch));
        ch.connect("IPerfCluster");

        MBeanServer server=Util.getMBeanServer();
        if(server == null)
            System.err.println("No MBeanServers found;" +
                    "\nIPerf needs to be run with an MBeanServer present, or inside JDK 5");
        JmxConfigurator.registerChannel(ch, server, "jgroups", ch.getClusterName(), true);


        if(sender) {
            ch.setOpt(Channel.LOCAL, false);
            System.out.println("min=" + Util.printBytes(MIN_SIZE) + ", max=" + Util.printBytes(MAX_SIZE) + ", sleep time=" + sleep);
            while(true) {
                Util.sleepRandom(sleep);
                sendMessage();
            }
        }
    }

    private void sendMessage() throws ChannelException {
        int size=(int)Util.random(MAX_SIZE);
        size=Math.max(size, MIN_SIZE);
        if(FIXED_SIZE > 0)
            size=FIXED_SIZE;
        byte[] buf=new byte[size];
        Message msg=new Message(null, null, buf);
        // stats.clear();
        Vector<Address> mbrs=ch.getView().getMembers();
        long current_time=System.currentTimeMillis();
        Map<Address,Long> map=new ConcurrentHashMap<Address,Long>();
        for(Address mbr: mbrs)
            map.put(mbr, current_time);
        stats.put(seqno, map);
        MyHeader hdr=new MyHeader(MyHeader.Type.DATA, seqno, size);
        msg.putHeader(NAME, hdr);
        System.out.println("\n[" + new Date() + "] --> sending #" + seqno + ": " + Util.printBytes(size));
        ch.send(msg);
        seqno++;
    }

    public static void main(String[] args) throws Exception {
        boolean sender=false;
        int sleep=10000, min=100 * 1000, max=100 * 1000 * 1000, fixed=0;
        String props="udp.xml";

        for(int i=0; i < args.length; i++) {
            String tmp=args[i];
            if(tmp.equalsIgnoreCase("-sender")) {
                sender=true;
                continue;
            }
            if(tmp.equalsIgnoreCase("-props")) {
                props=args[++i];
                continue;
            }
            if(tmp.equalsIgnoreCase("-sleep")) {
                sleep=Integer.parseInt(args[++i]);
                continue;
            }
            if(tmp.equalsIgnoreCase("-min")) {
                min=Integer.parseInt(args[++i]);
                continue;
            }
            if(tmp.equalsIgnoreCase("-max")) {
                max=Integer.parseInt(args[++i]);
                continue;
            }
            if(tmp.equalsIgnoreCase("-fixed")) {
                fixed=Integer.parseInt(args[++i]);
                continue;
            }
            if(tmp.equalsIgnoreCase("-bind_addr")) {
                System.setProperty("jgroups.bind_addr", args[++i]);
                continue;
            }
            help();
            return;
        }

        ClassConfigurator.add((short)10000, MyHeader.class);

        new IPerf(sender, props, sleep, min, max, fixed).start();
    }

    static void help() {
        System.out.println("IPerf [-sender] [-props <props>] [-bind_addr <addr>] [-sleep <time in ms>] " +
                "[-min <size>] [-max <size>] [-fixed <size>]");
    }


    private class MyReceiver extends ReceiverAdapter {
        private final JChannel channel;

        public MyReceiver(JChannel channel) {
            this.channel=channel;
        }

        public void viewAccepted(View new_view) {
            log("view: " + new_view);
        }

        public void receive(Message msg) {
            int len=msg.getLength();
            MyHeader hdr=(MyHeader)msg.getHeader(NAME);
            switch(hdr.type) {
                case DATA:
                    log("<-- received #" + hdr.seqno + ": " + Util.printBytes(len) + " from " + msg.getSrc());
                    if(hdr.size != len)
                        System.err.println("hdr.size (" + hdr.size + ") != length (" + len + ")");
                    sendConfirmation(msg.getSrc(), hdr.seqno, hdr.size);
                    break;
                case CONFIRMATION:
                    handleConfirmation(msg.getSrc(), hdr.seqno);
                    break;
                default:
                    System.err.println("received invalid header: " + hdr);
            }
        }

        private void handleConfirmation(Address sender, int seqno) {
            Map<Address, Long> map=stats.get(seqno);
            if(map == null) {
                System.err.println("no map for seqno #" + seqno);
                return;
            }
            Long start_time=map.remove(sender);
            if(start_time != null) {
                long diff=System.currentTimeMillis() - start_time;
                System.out.println("time for #" + seqno + ": " + sender + ": " + diff + "ms");
            }
            if(map.isEmpty()) {
                stats.remove(seqno);
            }
        }

        private void sendConfirmation(Address dest, int seqno, int size) {
            Message rsp=new Message(dest, null, null);
            rsp.setFlag(Message.OOB);
            MyHeader rsp_hdr=new MyHeader(MyHeader.Type.CONFIRMATION, seqno, size);
            rsp.putHeader(NAME, rsp_hdr);
            try {
                channel.send(rsp);
            }
            catch(Throwable e) {
                e.printStackTrace();
            }
        }

        private void log(String msg) {
            // System.out.println("[" + new Date() + "]: " + msg);
            System.out.println(msg);
        }
    }

    public static class MyHeader extends Header implements Streamable {
        private static final long serialVersionUID=-8796883857099720796L;
        private static enum Type {DATA, CONFIRMATION};
        private Type type;
        private int seqno;
        private int size;


        public MyHeader() {
            type=Type.DATA;
            seqno=-1;
            size=-1;
        }

        public MyHeader(Type type, int seqno, int size) {
            this.type=type;
            this.seqno=seqno;
            this.size=size;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeUTF(type.name());
            out.writeInt(seqno);
            out.writeInt(size);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            String name=in.readUTF();
            type=Type.valueOf(name);
            seqno=in.readInt();
            size=in.readInt();
        }

        public int size() {
            int retval=Global.INT_SIZE * 2;
            retval += type.name().length() +2;
            return retval;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("type=" + type);
            switch(type) {
                case DATA:
                    sb.append(", seqno=" + seqno + ", size=" + Util.printBytes(size));
                    break;
                case CONFIRMATION:
                    sb.append(", seqno=" + seqno);
                    break;
            }
            return sb.toString();
        }
    }
}
