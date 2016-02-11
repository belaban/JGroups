package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.blocks.*;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;


/**
 * Test for measuring performance of group RPCs.
 * @author Bela Ban
 * @version $Revision: 1.23 $
 */
public class RpcDispatcherSpeedTest implements MembershipListener {
    Channel               channel;
    RpcDispatcher         disp;
    String                props=null;
    boolean               server=false; // role is client by default
    boolean               jmx=false;
    int                   num=1000;
    int                   num_threads=1;
    static final long     TIMEOUT=10000;
    final Method[]        METHODS=new Method[1];
    private long          sleep=0;
    private final boolean async;
    private final boolean oob;


    public RpcDispatcherSpeedTest(String props, boolean server, boolean async, boolean oob, int num, int num_threads,
                                  boolean jmx, long sleep) throws NoSuchMethodException {
        this.props=props;
        this.server=server;
        this.async=async;
        this.oob=oob;
        this.num=num;
        this.num_threads=num_threads;
        this.jmx=jmx;
        this.sleep=sleep;
        initMethods();
    }

    final void initMethods() throws NoSuchMethodException {
        Class cl=this.getClass();
        METHODS[0]=cl.getMethod("measure");
    }

    public long measure() throws Exception {
        long retval=System.currentTimeMillis();
        if(sleep > 0)
            Util.sleep(sleep);
        return retval;
    }


    public void start() throws Exception {
        channel=new JChannel(props);
        channel.setDiscardOwnMessages(true);
        disp=new RpcDispatcher(channel, this) // no concurrent processing on incoming method calls
          .setMembershipListener(this).setMethodLookup(id -> METHODS[0]);

        if(jmx) {
            MBeanServer srv=Util.getMBeanServer();
            if(srv == null)
                throw new Exception("No MBeanServers found;" +
                        "\nDraw needs to be run with an MBeanServer present, or inside JDK 5");
            JmxConfigurator.registerChannel((JChannel)channel, srv, "jgroups", channel.getClusterName(), true);
        }

        channel.connect("RpcDispatcherSpeedTestGroup");

        try {
            if(server) {
                System.out.println("-- Started as server. Press ctrl-c to kill");
                while(true) {
                    Util.sleep(10000);
                }
            }
            else {
                invokeRpcs(num, num_threads, async, oob);
            }
        }
        catch(Throwable t) {
            t.printStackTrace(System.err);
        }
        finally {
            channel.close();
            disp.stop();
        }
    }


    void invokeRpcs(int num, int num_threads, boolean async, boolean oob) throws Exception {
        long    start;
        int     show=num / 10;

        Method measure_method=getClass().getMethod("measure");
        MethodCall measure_method_call=new MethodCall(measure_method);

        if(show <=0)
            show=1;
        ResponseMode request_type=async ? ResponseMode.GET_NONE : ResponseMode.GET_ALL;

        measure_method_call=new MethodCall((short)0);
        RequestOptions opts=new RequestOptions(request_type, TIMEOUT, false, null,
                                               Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
        if(oob)
            opts.flags(Message.Flag.OOB);

        final AtomicInteger sent=new AtomicInteger(0);
        final CountDownLatch latch=new CountDownLatch(1);
        final Publisher[] senders=new Publisher[num_threads];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Publisher(measure_method_call, sent, num, opts, disp, latch);
            senders[i].start();
        }

        start=System.currentTimeMillis();
        latch.countDown();

        for(Publisher sender: senders)
            sender.join();

        long stop=System.currentTimeMillis();
        printStats(stop-start, num);
    }


    static class Publisher extends Thread {
        final MethodCall call;
        final RequestOptions options;
        final AtomicInteger sent;
        final int num_msgs_to_send;
        final RpcDispatcher disp;
        final CountDownLatch latch;
        final int print;

        public Publisher(MethodCall call, AtomicInteger sent, final int num, RequestOptions options, RpcDispatcher disp, CountDownLatch latch) {
            this.call=call;
            this.sent=sent;
            this.num_msgs_to_send=num;
            this.options=options;
            this.disp=disp;
            this.latch=latch;
            print = num_msgs_to_send / 10;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            while(true) {
                int tmp=sent.incrementAndGet();
                if(tmp > num_msgs_to_send)
                    break;
                try {
                    disp.callRemoteMethods(null, call, options);
                    if(tmp > 0 && tmp % print == 0)
                    System.out.println(tmp);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    static void printStats(long total_time, int num) {
        double throughput=((double)num)/((double)total_time/1000.0);
        System.out.println("time for " + num + " remote calls was " +
                           total_time + "ms, avg=" + (total_time / (double)num) +
                           "ms/invocation, " + (long)throughput + " calls/sec");
    }

    public void viewAccepted(View new_view) {
        System.out.println("-- new view: " + new_view);
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }

    public void unblock() {
    }


    public static void main(String[] args) {
        String                 props=null;
        boolean                server=false, jmx=false;
        int                    num=1000;
        int                    num_threads=1;
        long                   sleep=0;
        RpcDispatcherSpeedTest test;
        boolean                async=false;
        boolean                oob=false;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-server".equals(args[i])) {
                server=true;
                continue;
            }
            if("-async".equals(args[i])) {
                async=true;
                continue;
            }
            if("-num".equals(args[i])) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            if("-jmx".equals(args[i])) {
                jmx=true;
                continue;
            }
            if("-sleep".equals(args[i])) {
                sleep=Long.parseLong(args[++i]);
                continue;
            }
            if("-num_threads".equals(args[i])) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if("-oob".equals(args[i])) {
                oob=true;
                continue;
            }
            help();
            return;
        }


        try {
            test=new RpcDispatcherSpeedTest(props, server, async, oob, num, num_threads, jmx, sleep);
            test.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println("RpcDispatcherSpeedTest [-help] [-props <props>] " +
                           "[-server] [-async] [-num <number of calls>] [-jmx] [-sleep <ms>]");
    }
}
