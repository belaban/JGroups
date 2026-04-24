package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.tests.rt.transports.*;
import org.jgroups.util.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Class that measure RTT for multicast messages between 2 cluster members. See {@link RpcDispatcherSpeedTest} for
 * RPCs. Note that request and response latencies are measured using {@link System#nanoTime()} which might yield
 * incorrect values when run on different cores, so these numbers may not be reliable.<p>
 * @author Bela Ban
 */
public class RoundTrip implements RtReceiver {
    protected RtTransport                     tp;
    protected int                             num_msgs=50000;
    protected int                             num_senders=1; // number of sender threads
    protected boolean                         details;
    protected static final byte               REQ=0, RSP=1, DONE=2;
    // | REQ or RSP | index (short) |
    public static final int                   PAYLOAD=Global.BYTE_SIZE + Global.SHORT_SIZE;
    protected Sender[]                        senders;
    // time for sending a request, from RtTransport.send() until the req is sent (or queued) by the transport
    protected final AverageMinMax             req_send_time=new AverageMinMax().unit(NANOSECONDS);
    // time for sending a response, from RtTransport.send() until the rsp is sent (or queued) by the transport
    protected final AverageMinMax             rsp_send_time=new AverageMinMax().unit(NANOSECONDS);
    // time for delivery of a request (including sending of the response)
    protected final AverageMinMax             req_delivery_time=new AverageMinMax().unit(NANOSECONDS);
    // time for delivery of a response
    protected final AverageMinMax             rsp_delivery_time=new AverageMinMax().unit(NANOSECONDS);
    protected static final Map<String,String> TRANSPORTS=new HashMap<>(16);

    static {
        TRANSPORTS.put("jg",      JGroupsTransport.class.getName());
        TRANSPORTS.put("tcp",     TcpTransport.class.getName());
        TRANSPORTS.put("nio",     NioTransport.class.getName());
        TRANSPORTS.put("server",  ServerTransport.class.getName());
        TRANSPORTS.put("udp",     UdpTransport.class.getName());
    }

    protected void start(String transport, String[] args) throws Exception {
        tp=create(transport);
        tp.receiver(this);
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
        long msg_start=System.nanoTime();
        switch(req_buf[offset]) {
            case REQ:
                short id=Bits.readShort(req_buf, 1+offset);
                byte[] rsp_buf=new byte[PAYLOAD];
                rsp_buf[0]=RSP;
                Bits.writeShort(id, rsp_buf, 1);
                try {
                    long start=System.nanoTime();
                    tp.send(sender, rsp_buf, 0, rsp_buf.length);
                    long time=System.nanoTime() - start;
                    rsp_send_time.add(time);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                req_delivery_time.add(System.nanoTime() - msg_start);
                break;
            case RSP:
                id=Bits.readShort(req_buf, 1);
                senders[id].promise.setResult(true); // notify the sender of the response
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
            default:
                throw new IllegalArgumentException("first byte needs to be either REQ or RSP but not " + req_buf[0]);
        }
    }


    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("""
                                                [1] send [2] num_msgs (%d) [3] senders (%d)
                                                [d] details (%b) [x] exit
                                                """, num_msgs, num_senders, details));
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
                    case 'd':
                        details=!details;
                        break;
                    case 'x':
                    case -1:
                        looping=false;
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
        for(int i=0; i < num_senders; i++) {
            senders[i]=new Sender((short)i, latch, sent_msgs, target);
            senders[i].start();
        }
        System.out.printf("-- sending %d messages to %s\n", num_msgs, target);
        long start=System.nanoTime();
        latch.countDown(); // start all sender threads
        for(Sender sender: senders)
            sender.join();
        long total_time=System.nanoTime() - start;

        byte[] done_buf=new byte[PAYLOAD];
        done_buf[0]=DONE;
        tp.send(target, done_buf, 0, done_buf.length);
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


    protected class Sender extends Thread {
        protected final short            id;
        protected final CountDownLatch   latch;
        protected final AtomicInteger    sent_msgs; // current number of messages; senders stop if sent_msgs >= num_msgs
        protected final Promise<Boolean> promise=new Promise<>(); // for the sender thread to wait for the response
        protected final int              print;
        protected final Object           target;
        protected final AverageMinMax    rtt=new AverageMinMax().unit(NANOSECONDS); // client -> server -> client

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
            for(;;) {
                int num=sent_msgs.incrementAndGet();
                if(num > num_msgs)
                    break;
                if(num > 0 && num % print == 0)
                    System.out.print(".");

                promise.reset(false);
                byte[] req_buf=new byte[PAYLOAD];

                // The request contains
                // * the type of the message (byte): REQ or RSP (or DONE)
                // * the ID (short) of the sender thread (for response correlation)
                req_buf[0]=REQ;
                Bits.writeShort(id, req_buf, 1);
                try {
                    long start=System.nanoTime();
                    tp.send(target, req_buf, 0, req_buf.length);
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
        for(int i=0; i < args.length; i++) {
            switch(args[i]) {
                case "-tp" -> tp=args[++i];
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
                if(args[i].equals("-tp") || args[i].equals("-h") || !args[i].startsWith("-"))
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
        new RoundTrip().start(tp, args);
    }


    protected static void help(String transport) {
        RtTransport tp=null;
        try {
            tp=create(transport);
        }
        catch(Exception e) {
        }
        System.out.printf("\n%s [-tp classname | (%s)]\n          %s\n\n",
                          RoundTrip.class.getSimpleName(), availableTransports(), tp != null? printOptions(tp.options()) : "");
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
