package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tests sending large messages from one sender to multiple receivers. The following messages are exchanged:
 * <pre>
 * | START (1 byte) |
 * | DATA (1 byte) | size (int, 4 bytes) | byte[] buf |
 * | STOP (1 byte) |
 * | RESULT (1 byte) | total time (long, 8 bytes) | total_bytes (long, 8 bytes)| 
 *
 * </pre>
 * @author Bela Ban
 * @version $Id: IPerf.java,v 1.5 2008/09/11 12:08:16 belaban Exp $
 */
public class IPerf implements Receiver {
    private final Configuration config;
    private final ConcurrentMap<Object,Entry> receiver_table=new ConcurrentHashMap<Object,Entry>();
    private Transport transport=null;


    public IPerf(Configuration config) {
        this.config=config;
    }

    public void start() throws Exception {
        transport=(Transport)Class.forName(config.getTransport()).newInstance();
        transport.create(config);
        transport.setReceiver(this);
        transport.start();

        if(config.isSender()) {
            send();
            transport.stop();
            transport.destroy();
        }
        else {
            System.out.println("Transport " + transport.getClass().getName() + " started at " + new Date());
            System.out.println("Listening on " + transport.getLocalAddress());
        }
    }


    public void receive(Object sender, byte[] payload) {
        ByteBuffer buf=ByteBuffer.wrap(payload);
        byte b=buf.get();
        Type type=Type.getType(b);
        switch(type) {
            case START:
                receiver_table.remove(sender);
                break;
            case DATA:
                Entry entry=receiver_table.get(sender);
                if(entry == null) {
                    entry=new Entry();
                    Entry tmp=receiver_table.putIfAbsent(sender, entry);
                    if(tmp != null)
                        entry=tmp;
                }
                int length=buf.getInt();
                entry.total_bytes+=length;
                break;
            case STOP:
                entry=receiver_table.get(sender);
                if(entry == null) {
                    err("entry for " + sender + " not found");
                    return;
                }
                if(entry.stop_time == 0)
                    entry.stop_time=System.currentTimeMillis();
                sendResult(sender, entry);
                break;
            case RESULT:
                long total_time=buf.getLong(), total_bytes=buf.getLong();
                System.out.println("time for " + Util.printBytes(total_bytes) + ": " + total_time + " ms");
                break;
        }
    }




    private void send() throws Exception {
        int size=config.getSize();

        byte[] buf=createStartMessage();
        transport.send(null, buf, false);

        buf=createDataMessage(size);
        System.out.println("\n[" + new Date() + "] --> sending " + Util.printBytes(size));
        transport.send(null, buf, false);

        buf=createStopMessage();
        transport.send(null, buf, false);


        Util.sleep(30000);
    }


    private void sendResult(Object destination, Entry entry) {
        ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.LONG_SIZE * 2);
        buf.put(Type.RESULT.getByte());
        buf.putLong(entry.stop_time - entry.start_time);
        buf.putLong(entry.total_bytes);
        try {
            transport.send(destination, buf.array(), false);
        }
        catch(Exception e) {
            err(e.toString());
        }
    }


    private static byte[] createStartMessage() {
        return createMessage(Type.START);
    }


    private static byte[] createDataMessage(int length) {
        ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + length + Global.INT_SIZE);
        buf.put(Type.DATA.getByte());
        buf.putInt(length);
        return buf.array();
    }


    private static byte[] createStopMessage() {
        return createMessage(Type.STOP);
    }


    private static byte[] createMessage(Type type) {
        ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE);
        buf.put(type.getByte());
        return buf.array();
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
                return;
            }
            unused_args.add(tmp);
        }

        if(!unused_args.isEmpty()) {
            String[] tmp=new String[unused_args.size()];
            for(int i=0; i < unused_args.size(); i++)
                tmp[i]=unused_args.get(i);
            config.setTransportArgs(tmp);
        }

        new IPerf(config).start();
    }

    static void help(String transport) {
        StringBuilder sb=new StringBuilder();
        sb.append("IPerf [-sender] [-bind_addr <addr>] [-transport <class name>]");
        try {
            Transport tp=(Transport)Class.forName(transport).newInstance();
            String tmp=tp.help();
            if(tmp != null && tmp.length() > 0)
                sb.append("\nTransport specific options for " + tp.getClass().getName() + ":\n" + tp.help());
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println(sb);
    }

    private static void log(String msg) {
        System.out.println(msg);
    }

    private static void err(String msg) {
        System.err.println(msg);
    }



    enum Type {
        START(1),
        DATA(2),
        STOP(3),
        RESULT(4);

        final byte b;

        Type(int i) {
            b=(byte)i;
        }

        public byte getByte() {
            return b;
        }

        public static Type getType(byte input) {
            switch(input) {
                case 1: return START;
                case 2: return DATA;
                case 3: return STOP;
                case 4: return RESULT;
            }
            throw new IllegalArgumentException("type " + input + " is not valid");
        }
    }

    private static class Entry {
        private final long start_time;
        private long stop_time=0;
        private long total_bytes=0;

        public Entry() {
            this.start_time=System.currentTimeMillis();
        }
    }
    


}
