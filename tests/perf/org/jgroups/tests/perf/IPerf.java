package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests sending large messages from one sender to multiple receivers
 * @author Bela Ban
 * @version $Id: IPerf.java,v 1.3 2008/07/24 10:15:09 belaban Exp $
 */
public class IPerf {
    private final Configuration config;
    private int seqno=1;
    private final static String NAME="IPerf";
    private final Map<Integer,Map<Object, Long>> stats=new ConcurrentHashMap<Integer,Map<Object,Long>>();
    private Transport transport=null;


    public IPerf(Configuration config) {
        this.config=config;
    }

    public void start() throws Exception {
        transport=(Transport)Class.forName(config.getTransport()).newInstance();
        transport.create(config);
        transport.start();

        if(config.isSender()) {
            sendMessage();
            transport.stop();
            transport.destroy();
        }
        else {
            System.out.println("Transport " + transport.getClass().getName() + " started at " + new Date());
            System.out.println("Listening on " + transport.getLocalAddress());
        }
    }

    private void sendMessage() throws Exception {
        int size=config.getSize();
        byte[] buf=new byte[size];
        List<Object> mbrs=transport.getClusterMembers();
        long current_time=System.currentTimeMillis();
        Map<Object,Long> map=new ConcurrentHashMap<Object,Long>();
        for(Object mbr: mbrs)
            map.put(mbr, current_time);
        stats.put(seqno, map);
        MyHeader hdr=new MyHeader(MyHeader.Type.DATA, seqno, size);
        // msg.putHeader(NAME, hdr);
        System.out.println("\n[" + new Date() + "] --> sending #" + seqno + ": " + Util.printBytes(size));
        transport.send(null, buf, false);
        seqno++;
    }

    public static void main(String[] args) throws Exception {
        Configuration config=new Configuration();

        List<String> unused_args=new ArrayList<String>(args.length);

        for(int i=0; i < args.length; i++) {
            String tmp=args[i];
            if(tmp.equalsIgnoreCase("-sender")) {
                config.setSender(true);
                continue;
            }
            if(tmp.equalsIgnoreCase("-size")) {
                config.setSize(Integer.parseInt(args[++i]));
                continue;
            }
            if(tmp.equals("-transport")) {
                config.setTransport(args[++i]);
                continue;
            }
            if(tmp.equals("-h") || tmp.equals("-help")) {
                help(config.getTransport());
            }
            unused_args.add(tmp);
        }

        if(!unused_args.isEmpty()) {
            String[] tmp=new String[unused_args.size()];
            for(int i=0; i < unused_args.size(); i++)
                tmp[i]=unused_args.get(i);
            config.setTransportArgs(tmp);
        }

        ClassConfigurator.add((short)10000, MyHeader.class);

        new IPerf(config).start();
    }

    static void help(String transport) {
        StringBuilder sb=new StringBuilder();
        sb.append("IPerf [-sender] [-bind_addr <addr>] [-transport <class name>]");
        try {
            Transport tp=(Transport)Class.forName(transport).newInstance();
            String tmp=tp.help();
            if(tmp != null && tmp.length() > 0)
                sb.append("\nTransport specific options:\n" + tp.help());
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println(sb);
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
            Map<Object, Long> map=stats.get(seqno);
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
