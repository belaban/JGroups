package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.tests.rt.transports.*;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Bits;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that measure RTT for multicast messages between 2 cluster members. See {@link RpcDispatcherSpeedTest} for
 * RPCs. Note that request and response times are measured using {@link System#nanoTime()} which might yield incorrect
 * values when run on different cores, so these numbers may not be reliable.<p/>
 * Running this on different nodes will not yield correct results for request- and response-latencies, but should be ok
 * for round-trip times. *If* times across nodes are synchronized, flag -use-wall-clock can switch to currentTimeMillis()
 * which gives more meaningful results for request- and response-latency across boxes.
 * @author Bela Ban
 */
public class RoundTrip implements RtReceiver {
    protected RtTransport                     tp;
    protected int                             num_msgs=50000;
    protected int                             num_senders=1; // number of sender threads
    protected boolean                         details;
    protected boolean                         use_ms; // typically we use us, but if the flag is true, we use ms
    protected static final byte               REQ=0, RSP=1, DONE=2;
    // | REQ or RSP | index (short) | time (long) |
    public static final int                   PAYLOAD=Global.BYTE_SIZE + Global.SHORT_SIZE + Global.LONG_SIZE;
    protected Sender[]                        senders;
    protected final AverageMinMax             req_latency=new AverageMinMax(); // client -> server
    protected final AverageMinMax             rsp_latency=new AverageMinMax(); // server -> client
    protected static final Map<String,String> TRANSPORTS=new HashMap<>(16);

    static {
        TRANSPORTS.put("jg",      JGroupsTransport.class.getName());
        TRANSPORTS.put("jgroups", JGroupsTransport.class.getName());
        TRANSPORTS.put("tcp",     TcpTransport.class.getName());
        TRANSPORTS.put("nio",     NioTransport.class.getName());
        TRANSPORTS.put("server",  ServerTransport.class.getName());
        TRANSPORTS.put("udp",     UdpTransport.class.getName());
    }

    public RoundTrip(boolean use_ms) {
        this.use_ms=use_ms;
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
        switch(req_buf[offset]) {
            case REQ:
                short id=Bits.readShort(req_buf, 1+offset);
                long time=time(use_ms) - Bits.readLong(req_buf, 3+offset);
                byte[] rsp_buf=new byte[PAYLOAD];
                rsp_buf[0]=RSP;
                Bits.writeShort(id, rsp_buf, 1);
                try {
                    Bits.writeLong(time(use_ms), rsp_buf, 3);
                    tp.send(sender, rsp_buf, 0, rsp_buf.length);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                synchronized(req_latency) {
                    req_latency.add(time);
                }
                break;
            case RSP:
                id=Bits.readShort(req_buf, 1);
                time=time(use_ms) - Bits.readLong(req_buf, 3);
                senders[id].promise.setResult(true); // notify the sender of the response
                synchronized(rsp_latency) {
                    rsp_latency.add(time);
                }
                break;
            case DONE:
                System.out.printf(Util.bold("req-latency = min/avg/max: %d / %.2f / %d %s\n"),
                                  req_latency.min(), req_latency.average(), req_latency.max(), unit());
                req_latency.clear();
                break;
            default:
                throw new IllegalArgumentException("first byte needs to be either REQ or RSP but not " + req_buf[0]);
        }
    }


    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("[1] send [2] num_msgs (%d) [3] senders (%d)\n" +
                                                "[d] details (%b) [x] exit\n",
                                              num_msgs, num_senders, details));
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
        List mbrs=tp.clusterMembers();
        if(mbrs != null && mbrs.size() != 2) {
            System.err.printf("Cluster must have exactly 2 members: %s\n", mbrs);
            return;
        }
        rsp_latency.clear();
        Object target=mbrs != null? Util.pickNext(mbrs, tp.localAddress()) : null;
        final CountDownLatch latch=new CountDownLatch(1);
        final AtomicInteger sent_msgs=new AtomicInteger(0);
        senders=new Sender[num_senders];
        for(int i=0; i < num_senders; i++) {
            senders[i]=new Sender((short)i, latch, sent_msgs, target);
            senders[i].start();
        }

        long start=time(use_ms);
        latch.countDown(); // start all sender threads
        for(Sender sender: senders)
            sender.join();
        long total_time=time(use_ms)-start;

        byte[] done_buf=new byte[PAYLOAD];
        done_buf[0]=DONE;
        tp.send(target, done_buf, 0, done_buf.length);

        double divisor=use_ms? 1_000.0 : 1_000_000.0;
        double msgs_sec=num_msgs / (total_time / divisor);

        AverageMinMax avg=null;
        if(details)
            System.out.println("");
        for(Sender sender: senders) {
            if(details)
                System.out.printf("%d: %s\n", sender.id, print(sender.rtt));
            if(avg == null)
                avg=sender.rtt;
            else
                avg.merge(sender.rtt);
        }

        System.out.printf(Util.bold("\n\nreqs/sec    = %.2f" +
                                      "\nround-trip  = min/avg/max: %d / %.2f / %d %s" +
                                      "\nrsp-latency = min/avg/max: %d / %.2f / %d %s\n\n"),
                          msgs_sec, avg.min(), avg.average(), avg.max(), unit(),
                          rsp_latency.min(), rsp_latency.average(), rsp_latency.max(), unit());
    }

    protected String print(AverageMinMax avg) {
        return String.format("round-trip min/avg/max = %d / %.2f / %d %s", avg.min(), avg.average(), avg.max(), unit());
    }

    protected static long time(boolean use_ms) {return use_ms? System.currentTimeMillis() : Util.micros();}
    protected String unit() {return use_ms? "ms" : "us";}



    protected class Sender extends Thread {
        protected final short            id;
        protected final CountDownLatch   latch;
        protected final AtomicInteger    sent_msgs; // current number of messages; senders stop if sent_msgs >= num_msgs
        protected final Promise<Boolean> promise=new Promise<>(); // for the sender thread to wait for the response
        protected final int              print;
        protected final Object           target;
        protected final AverageMinMax    rtt=new AverageMinMax(); // client -> server -> client

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
                int num=sent_msgs.getAndIncrement();
                if(num > num_msgs)
                    break;
                if(num > 0 && num % print == 0)
                    System.out.printf(".");

                promise.reset(false);
                byte[] req_buf=new byte[PAYLOAD];
                req_buf[0]=REQ;
                Bits.writeShort(id, req_buf, 1);
                try {
                    long start=time(use_ms);
                    Bits.writeLong(start, req_buf, 3);
                    tp.send(target, req_buf, 0, req_buf.length);
                    promise.getResult(0);
                    long time=time(use_ms)-start;
                    rtt.add(time);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String tp="jg";
        boolean use_ms=false;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-tp")) {
                tp=args[++i];
                continue;
            }
            if(args[i].equals("-use-wall-clock")) {
                use_ms=true;
                continue;
            }
            if(args[i].equals("-h")) {
                help(tp);
                return;
            }
        }
        RtTransport transport=create(tp);
        String[] opts=transport.options();
        if(opts != null) {
            for(int i=0; i < args.length; i++) {
                if(args[i].equals("-tp") || args[i].equals("-h") || args[i].equals("-use-wall-clock")
                  || !args[i].startsWith("-"))
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
        new RoundTrip(use_ms).start(tp, args);
    }


    protected static void help(String transport) {
        RtTransport tp=null;
        try {
            tp=create(transport);
        }
        catch(Exception e) {
        }
        System.out.printf("\n%s [-tp classname | (%s)] [-use-wall-clock]\n          %s\n\n",
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
