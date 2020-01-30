package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Class that measure RTT for multicast messages between 2 cluster members. See {@link RpcDispatcherSpeedTest} for
 * RPCs
 * @author Bela Ban
 */
public class RoundTripRpc implements Receiver {
    protected JChannel            channel;
    protected RpcDispatcher       disp;
    protected int                 num_msgs=20000;
    protected int                 num_senders=1; // number of sender threads
    protected boolean             oob=true, dont_bundle, details;
    protected Invoker[]           invokers;
    protected static final Method requestMethod;

    static {
        try {
            requestMethod=RoundTripRpc.class.getDeclaredMethod("request", short.class);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    protected void start(String props, String name) throws Exception {
        channel=new JChannel(props).name(name);
        disp=new RpcDispatcher(channel, this).setReceiver(this);
        disp.setMethodLookup(ignored -> requestMethod);
        channel.connect("rt");
        View view=channel.getView();
        if(view.size() > 2)
            System.err.printf("More than 2 members found (%s); terminating\n", view);
        else
            loop();
        Util.close(channel, disp);
    }

    public static short request(short id) {
        return id;
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
                        invokeRequests();
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

    protected void invokeRequests() throws Exception {
        View view=channel.getView();
        if(view.size() != 2) {
            System.err.printf("Cluster must have exactly 2 members: %s\n", view);
            return;
        }

        Address target=Util.pickNext(view.getMembers(), channel.getAddress());
        final CountDownLatch latch=new CountDownLatch(1);
        final AtomicInteger sent_msgs=new AtomicInteger(0);
        invokers=new Invoker[num_senders];
        for(int i=0; i < num_senders; i++) {
            invokers[i]=new Invoker((short)i, latch, sent_msgs, target);
            invokers[i].start();
        }

        long start=System.nanoTime();
        latch.countDown(); // start all sender threads
        for(Invoker invoker : invokers)
            invoker.join();
        long total_time=System.nanoTime()-start;
        double msgs_sec=num_msgs / (total_time / 1_000_000_000.0);

        AverageMinMax avg=null;
        if(details)
            System.out.println("");
        for(Invoker invoker : invokers) {
            if(details)
                System.out.printf("%d: %s\n", invoker.id, print(invoker.avg));
            if(avg == null)
                avg=invoker.avg;
            else
                avg.merge(invoker.avg);
        }

        System.out.printf(Util.bold("\n\nreqs/sec = %.2f, " +
                                      "round-trip = min/avg/max: %.2f / %.2f / %.2f us\n\n"),
                          msgs_sec, avg.min()/1000.0, avg.average() / 1000.0, avg.max()/1000.0);
    }

    protected static String print(AverageMinMax avg) {
        return String.format("round-trip min/avg/max = %.2f / %.2f / %.2f us",
                             avg.min() / 1000.0, avg.average() / 1000.0, avg.max() / 1000.0);
    }


    protected class Invoker extends Thread {
        protected final short            id;
        protected final CountDownLatch   latch;
        protected final AtomicInteger    sent_msgs; // current number of messages; senders stop if sent_msgs >= num_msgs
        protected final int              print;
        protected final Address          target;
        protected final AverageMinMax    avg=new AverageMinMax(); // in ns

        public Invoker(short id, CountDownLatch latch, AtomicInteger sent_msgs, Address target) {
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
            RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, 0);
            if(oob) opts.flags(Message.Flag.OOB);
            if(dont_bundle) opts.flags(Message.Flag.DONT_BUNDLE);
            MethodCall call=new MethodCall((short)1, id);
            for(;;) {
                int num=sent_msgs.getAndIncrement();
                if(num > num_msgs)
                    break;
                if(num > 0 && num % print == 0)
                    System.out.printf(".");
                try {
                    long start=System.nanoTime();
                    disp.callRemoteMethod(target, call, opts);
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
        new RoundTripRpc().start(props, name);
    }



    private static void help() {
        System.out.println("RoundTripRpc [-props <properties>] [-name name]");
    }
}
