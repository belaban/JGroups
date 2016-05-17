package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that measure RTT for multicast messages between 2 cluster members. See {@link RpcDispatcherSpeedTest} for
 * RPCs
 * @author Bela Ban
 */
public class RoundTrip extends ReceiverAdapter {
    protected JChannel               channel;
    protected int                    num_msgs=20000;
    protected int                    num_senders=1; // number of sender threads
    protected boolean                oob=true, dont_bundle, details;
    protected static final byte      REQ=0, RSP=1;
    protected Sender[]               senders;



    protected void start(String props, String name) throws Exception {
        channel=new JChannel(props).name(name).setReceiver(this);
        TP transport=channel.getProtocolStack().getTransport();
        // uncomment below to disable the regular and OOB thread pools
        // transport.setOOBThreadPool(new DirectExecutor());
        // transport.setDefaultThreadPool(new DirectExecutor());

        //ThreadPoolExecutor thread_pool=new ThreadPoolExecutor(4, 4, 30000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(5000));
        //transport.setDefaultThreadPool(thread_pool);
        //transport.setOOBThreadPool(thread_pool);
        //transport.setInternalThreadPool(thread_pool);

        channel.connect("rt");
        View view=channel.getView();
        if(view.size() > 2)
            System.err.printf("More than 2 members found (%s); terminating\n", view);
        else
            loop();
        Util.close(channel);
    }

    /**
     * On the server: receive a request, send a response. On the client: send a request, wait for the response
     * @param msg
     */
    public void receive(Message msg) {
        byte[] req_buf=msg.getRawBuffer();
        byte type=req_buf[0];
        short id=Bits.readShort(req_buf, 1);
        switch(type) {
            case REQ:
                byte[] rsp_buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE];
                rsp_buf[0]=RSP;
                Bits.writeShort(id, rsp_buf, 1);
                Message rsp=new Message(msg.src(), rsp_buf);
                try {
                    channel.send(rsp);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case RSP:
                senders[id].promise.setResult(true); // notify the sender of the response
                break;
            default:
                throw new IllegalArgumentException("first byte needs to be either REQ or RSP but not " + req_buf[0]);
        }
    }

    public void viewAccepted(View view) {
        System.out.println("view = " + view);
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("[1] send [2] num_msgs (%d) [3] senders (%d)\n" +
                                                "[o] oob (%b) [b] dont_bundle (%b) [d] details (%b) [x] exit\n",
                                              num_msgs, num_senders, oob, dont_bundle, details));
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
                    case 'o':
                        oob=!oob;
                        break;
                    case 'b':
                        dont_bundle=!dont_bundle;
                        break;
                    case 'd':
                        details=!details;
                        break;
                    case 'x':
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
        View view=channel.getView();
        if(view.size() != 2) {
            System.err.printf("Cluster must have exactly 2 members: %s\n", view);
            return;
        }

        Address target=Util.pickNext(view.getMembers(), channel.getAddress());
        final CountDownLatch latch=new CountDownLatch(num_senders +1);
        final AtomicInteger sent_msgs=new AtomicInteger(0);
        senders=new Sender[num_senders];
        for(int i=0; i < num_senders; i++) {
            senders[i]=new Sender((short)i, latch, sent_msgs, target);
            senders[i].start();
        }

        long start=System.nanoTime();
        latch.countDown(); // start all sender threads
        for(Sender sender: senders)
            sender.join();
        long total_time=System.nanoTime()-start;
        double msgs_sec=num_msgs / (total_time / 1_000_000_000.0);

        AverageMinMax avg=null;
        if(details)
            System.out.println("");
        for(Sender sender: senders) {
            if(details)
                System.out.printf("%d: %s\n", sender.id, print(sender.avg));
            if(avg == null)
                avg=sender.avg;
            else
                avg.merge(sender.avg);
        }

        System.out.printf(Util.bold("\n\nreqs/sec = %.2f, " +
                                      "round-trip = min/avg/max: %.2f / %.2f / %.2f us\n\n"),
                          msgs_sec, avg.min()/1000.0, avg.average() / 1000.0, avg.max()/1000.0);
    }

    protected static String print(AverageMinMax avg) {
        return String.format("round-trip min/avg/max = %.2f / %.2f / %.2f us",
                             avg.min() / 1000.0, avg.average() / 1000.0, avg.max() / 1000.0);
    }


    protected class Sender extends Thread {
        protected final short            id;
        protected final byte[]           req_buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE]; // request and id
        protected final CountDownLatch   latch;
        protected final AtomicInteger    sent_msgs; // current number of messages; senders stop if sent_msgs >= num_msgs
        protected final Promise<Boolean> promise=new Promise<>(); // for the sender thread to wait for the response
        protected final int              print;
        protected final Address          target;
        protected final AverageMinMax    avg=new AverageMinMax(); // in ns

        public Sender(short id, CountDownLatch latch, AtomicInteger sent_msgs, Address target) {
            this.id=id;
            this.latch=latch;
            this.sent_msgs=sent_msgs;
            this.target=target;
            req_buf[0]=REQ;
            Bits.writeShort(id, req_buf, 1); // writes id at buf_reqs[1-2]
            print=num_msgs / 10;
        }

        public void run() {
            for(;;) {
                int num=sent_msgs.getAndIncrement();
                if(num >= num_msgs)
                    break;
                if(num > 0 && num % print == 0)
                    System.out.printf(".");
                promise.reset(false);
                Message req=new Message(target, req_buf);
                if(oob)
                    req.setFlag(Message.Flag.OOB);
                if(dont_bundle)
                    req.setFlag(Message.Flag.DONT_BUNDLE);
                try {
                    long start=System.nanoTime();
                    channel.send(req);
                    promise.getResult(0);
                    long time=System.nanoTime()-start;
                    avg.add(time);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String props=null, name=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }
        new RoundTrip().start(props, name);
    }



    private static void help() {
        System.out.println("RoundTrip [-props <properties>] [-name name]");
    }
}
