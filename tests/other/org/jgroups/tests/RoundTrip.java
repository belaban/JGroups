package org.jgroups.tests;

import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.tests.rt.transports.*;
import org.jgroups.util.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Class that measure RTT for unicast messages between 2 cluster members. See {@link RoundTripRpc} for RPCs.
 * <br/>
 * Note that request and response latencies are measured using {@link System#nanoTime()} which might yield
 * incorrect values when run on different cores, so these numbers may not be reliable.<p>
 * @author Bela Ban
 */
public class RoundTrip implements RtReceiver {
    protected RtTransport                     tp;
    protected boolean                         direct_memory;
    protected int                             num_msgs=100_000;
    protected int                             size; // additional data to be sent (default: 0)
    protected int                             num_senders=1; // number of sender threads
    protected boolean                         details;

    // format: | type (byte) | index (short) | payload (byte[], optional) |
    // Examples:
    // | REQ  | id | <null data>
    // | REQ  | id | <1024 bytes> |
    // | RSP  | id | <null data>
    // | RSP  | id | <1024 bytes> |
    // | DONE |
    // | EXIT |
    protected static final byte               REQ=1, RSP=2, DONE=3, EXIT=4;
    protected static final ByteBuffer         DONE_BUF;
    protected static final ByteBuffer         EXIT_BUF;
    protected static final int                METADATA_SIZE=Byte.BYTES + Short.BYTES;
    protected Sender[]                        senders;
    protected Thread[]                        sender_threads;
    protected ThreadFactory                   thread_factory;
    // time for sending a request, from RtTransport.send() until the req is sent (or queued) by the transport
    protected final AverageMinMax             req_send_time=new AverageMinMax(1024).unit(NANOSECONDS);
    // time for sending a response, from RtTransport.send() until the rsp is sent (or queued) by the transport
    protected final AverageMinMax             rsp_send_time=new AverageMinMax(1024).unit(NANOSECONDS);
    // time for delivery of a request (including sending of the response)
    protected final AverageMinMax             req_delivery_time=new AverageMinMax(1024).unit(NANOSECONDS);
    // time for delivery of a response
    protected final AverageMinMax             rsp_delivery_time=new AverageMinMax(1024).unit(NANOSECONDS);
    protected static final Map<String,String> TRANSPORTS=new HashMap<>(16);

    static {
        TRANSPORTS.put("jg",      JGroupsTransport.class.getName());
        TRANSPORTS.put("tcp",     TcpTransport.class.getName());
        TRANSPORTS.put("nio",     NioTransport.class.getName());
        TRANSPORTS.put("server",  ServerTransport.class.getName());
        TRANSPORTS.put("udp",     UdpTransport.class.getName());
        DONE_BUF=ByteBuffer.allocate(1).put(0, DONE);
        EXIT_BUF=java.nio.ByteBuffer.allocate(1).put(0, EXIT);
    }

    public int size() {
        return Byte.BYTES + Short.BYTES + size;
    }

    protected void start(String transport, boolean direct_memory, String[] args) throws Exception {
        thread_factory=new DefaultThreadFactory("sender", false, true).useVirtualThreads(true);
        tp=create(transport).roundTrip(this).receiver(this);
        this.direct_memory=direct_memory;
        try {
            tp.start(args);
            loop();
        }
        finally {
            tp.stop();
        }
    }

    /** On the server: receive a request, send a response. On the client: send a request, wait for the response */
    public void receive(Object sender, byte[] req_buf, int offset, int length) {
        receive(sender, ByteBuffer.wrap(req_buf, offset, length));
       /* long msg_start=System.nanoTime();
        switch(req_buf[offset]) {
            case REQ:
                short id=Bits.readShort(req_buf, 1+offset);
                byte[] tmp=new byte[length];
                tmp[0]=RSP;
                Bits.writeShort(id, tmp, 1);
                if(length > METADATA_SIZE) {
                    // initialize response buffer
                    for(int i=0; i < length - METADATA_SIZE; i++)
                        tmp[i+METADATA_SIZE]=RSP;
                }
                try {
                    long start=System.nanoTime();
                    tp.send(sender, tmp, 0, tmp.length);
                    long time=System.nanoTime() - start;
                    rsp_send_time.add(time);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                req_delivery_time.add(System.nanoTime() - msg_start);
                break;
            case RSP:
                id=Bits.readShort(req_buf, 1+offset);
                senders[id].promise.setResult(true, false); // notify all senders of the response
                rsp_delivery_time.add(System.nanoTime() - msg_start);
                break;
            case DONE:
                //noinspection TextBlockMigration
                System.out.printf(Util.bold("req-delivery-time  = %s\n" +
                                              "rsp-send-time      = %s\n\n"), req_delivery_time, rsp_send_time);
                rsp_send_time.clear();
                req_delivery_time.clear();
                break;
            case EXIT:
                System.out.printf("-- received EXIT from %s; terminating\n", sender);
                System.exit(1);
                break;
            default:
                throw new IllegalArgumentException("invalid request " + req_buf[0]);
        }*/
    }

    @Override
    public void receive(Object sender, ByteBuffer buf) {
        long msg_start=System.nanoTime();
        byte b=buf.get(0);
        switch(b) {
            case REQ:
                short id=buf.getShort(1);
                buf.put(0, RSP).putShort(1, id);
                if(buf.capacity() > METADATA_SIZE) {
                    // initialize buffer
                    for(int i=0; i < buf.capacity() - METADATA_SIZE; i++)
                        buf.put(i+METADATA_SIZE, RSP);
                }
                buf.clear();
                try {
                    long start=System.nanoTime();
                    tp.send(sender, buf);
                    long time=System.nanoTime() - start;
                    rsp_send_time.add(time);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                req_delivery_time.add(System.nanoTime() - msg_start);
                break;
            case RSP:
                id=buf.getShort(1);
                senders[id].promise.setResult(true, false); // notify the sender of the response
                rsp_delivery_time.add(System.nanoTime() - msg_start);
                break;
            case DONE:
                //noinspection TextBlockMigration
                System.out.printf(Util.bold("req-delivery-time  = %s\n" +
                                              "rsp-send-time      = %s\n\n"),
                                  req_delivery_time, rsp_send_time);
                rsp_send_time.clear();
                req_delivery_time.clear();
                break;
            case EXIT:
                System.out.printf("-- received EXIT from %s; terminating\n", sender);
                System.exit(1);
                break;
            default:
                throw new IllegalArgumentException("first byte needs to be either REQ or RSP but not " + b);
        }
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("""
                                                [1] send [2] num_msgs (%d) [3] senders (%d) [4] size (%s)
                                                [d] details (%b) [x] exit [X] exit all
                                                """, num_msgs, num_senders, Util.printBytes(size), details));
            try {
                switch(c) {
                    case '1':
                        sendRequests();
                        break;
                    case '2':
                        num_msgs=Util.readIntFromStdin("num_msgs: ");
                        break;
                    case '3':
                        num_senders=Util.readIntFromStdin("num_senders: ");
                        break;
                    case '4':
                        size=Util.readIntFromStdin("size: ");
                        break;
                    case 'd':
                        details=!details;
                        break;
                    case 'x':
                    case -1:
                        looping=false;
                        break;
                    case 'X':
                        tp.send(null, EXIT_BUF);
                        EXIT_BUF.clear(); // not really needed...
                        looping=false;
                        System.out.println("-- terminated");
                        break;
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
    }

    protected void sendRequests() throws Exception {
        List<Object> mbrs=(List<Object>)tp.clusterMembers();
        if(mbrs != null && mbrs.size() != 2) {
            System.err.printf("Cluster must have exactly 2 members: %s\n", mbrs);
            return;
        }
        req_send_time.clear();
        rsp_delivery_time.clear();
        Object target=mbrs != null? Util.pickNext(mbrs, tp.localAddress()) : null;
        final CountDownLatch latch=new CountDownLatch(1);
        final AtomicInteger sent_msgs=new AtomicInteger(0);
        senders=new Sender[num_senders];
        sender_threads=new Thread[num_senders];
        for(int i=0; i < num_senders; i++) {
            senders[i]=new Sender((short)i, latch, sent_msgs, target);
            sender_threads[i]=thread_factory.newThread(senders[i], "sender=" + i);
            sender_threads[i].start();
        }
        System.out.printf("-- sending %d messages to %s\n", num_msgs, target);
        long start=System.nanoTime();
        latch.countDown(); // start all sender threads
        for(Thread t: sender_threads)
            t.join();
        long total_time=System.nanoTime() - start;

        tp.send(target, DONE_BUF.clear());
        double msgs_sec=num_msgs / (total_time / 1_000_000_000.0);
        AverageMinMax avg=null;
        if(details)
            System.out.println();
        for(Sender sender: senders) {
            if(details)
                System.out.printf("%d: round-trip = %s\n", sender.id, sender.rtt);
            if(avg == null)
                avg=sender.rtt;
            else
                avg.merge(sender.rtt);
        }

        //noinspection TextBlockMigration
        System.out.printf(Util.bold("\n\nreqs/sec          = %.2f\n" +
                                      "round-trip        = %s\n" +
                                      "req-send-time     = %s\n" +
                                      "rsp-delivery-time = %s\n\n"),
                          msgs_sec, avg, req_send_time, rsp_delivery_time);
    }


    protected class Sender implements Runnable {
        protected final short            id;
        protected final CountDownLatch   latch;
        protected final AtomicInteger    sent_msgs; // current number of messages; senders stop if sent_msgs >= num_msgs
        protected final Promise<Boolean> promise=new Promise<>(); // for the sender thread to wait for the response
        protected final int              print;
        protected final Object           target;
        protected final AverageMinMax    rtt=new AverageMinMax(2048).unit(NANOSECONDS); // client -> server -> client

        public Sender(short id, CountDownLatch latch, AtomicInteger sent_msgs, Object target) {
            this.id=id;
            this.latch=latch;
            this.sent_msgs=sent_msgs;
            this.target=target;
            print=Math.max(1, num_msgs / 10);
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            ByteBuffer buf=direct_memory? ByteBuffer.allocateDirect(size()): java.nio.ByteBuffer.allocate(size());
            if(size > 0) {
                // initialize buffer
                for(int i=0; i < size; i++)
                    buf.put(i+METADATA_SIZE, REQ);
            }
            for(;;) {
                int num=sent_msgs.incrementAndGet();
                if(num > num_msgs)
                    break;
                if(num > 0 && num % print == 0)
                    System.out.print(".");

                promise.reset(false);
                buf.clear();

                // The request contains
                // * the type of the message (byte): REQ or RSP (or DONE)
                // * the ID (short) of the sender thread (for response correlation)
                buf.put(0, REQ).putShort(1, id);
                try {
                    long start=System.nanoTime();
                    tp.send(target, buf);
                    long send_time=System.nanoTime() - start;
                    promise.getResult(0);
                    long rtt_time=System.nanoTime() - start;
                    req_send_time.add(send_time);
                    rtt.add(rtt_time);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String tp="jg";
        boolean direct_memory=false;
        for(int i=0; i < args.length; i++) {
            switch(args[i]) {
                case "-tp" -> tp=args[++i];
                case "-direct-memory" -> direct_memory=Boolean.parseBoolean(args[++i]);
                case "-h" -> {
                    help(tp);
                    return;
                }
            }
        }
        RtTransport transport=create(tp);
        String[] opts=transport.options();
        if(opts != null) {
            for(int i=0; i < args.length; i++) {
                if(args[i].equals("-tp") || args[i].equals("-h")
                  || !args[i].startsWith("-") || args[i].startsWith("-direct-memory"))
                    continue;
                String option=args[i];
                boolean match=false;
                for(String opt: opts) {
                    if(opt.startsWith(option)) {
                        match=true;
                        break;
                    }
                }
                if(!match) {
                    help(tp);
                    return;
                }
            }
        }
        new RoundTrip().start(tp, direct_memory, args);
    }


    protected static void help(String transport) {
        RtTransport tp=null;
        try {
            tp=create(transport);
        }
        catch(Exception e) {
        }
        System.out.println();
        System.out.printf("%s [-tp classname | (%s)]\n%s[-direct-memory <boolean> (%s uses direct memory to send)]\n%s%s\n\n",
                          RoundTrip.class.getSimpleName(), availableTransports(),
                          " ".repeat(10), RoundTrip.class.getSimpleName(),
                          " ".repeat(10),
                          tp != null? printOptions(tp.options()) : "");
    }

    protected static RtTransport create(String transport) throws Exception {
        String clazzname=TRANSPORTS.get(transport);
        Class<?> clazz=Util.loadClass(clazzname != null? clazzname : transport, RoundTrip.class);
        return (RtTransport)clazz.getDeclaredConstructor().newInstance();
    }

    protected static String availableTransports() {
        return Util.printListWithDelimiter(TRANSPORTS.keySet(), "|", 0, false);
    }

    protected static String printOptions(String[] opts) {
        if(opts == null) return "";
        StringBuilder sb=new StringBuilder();
        for(String opt: opts)
            sb.append("[").append(opt).append("] ");
        return sb.toString();
    }

}
