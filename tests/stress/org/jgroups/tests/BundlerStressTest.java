package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.protocols.FD_SOCK2;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * Tests bundler performance. Creates N members, always sends from the first member to a random member (unicast)
 * and waits for the request to be received by sender_threads (same process, synchronous communication).
 * The request includes the thread's ID.
 * <p>
 * Each sender adds their thread-id to a hashmap (sender_threads) and waits on the promise (associated value). The
 * receiver reads the thread-id, grabs the promise and calls {@link org.jgroups.util.Promise#setResult(Object)},
 * waking up the sender thread to send the next message.
 * @author Bela Ban
 * @since  4.0
 */
public class BundlerStressTest {
    protected String                        bundler;
    protected int                           time=60 /* seconds */, warmup=time/2, nodes=4, msg_size=1000;
    protected int                           num_sender_threads=100;
    protected boolean                       details;
    protected String                        cfg="tcp.xml";
    protected JChannel[]                    channels;
    protected ThreadFactory                 thread_factory; // taken from channels[0]
    protected final Map<Long,Promise<Long>> sender_threads=new ConcurrentHashMap<>();
    protected static final Field            BUNDLER_TYPE=Util.getField(TP.class, "bundler_type");


    public BundlerStressTest(String config, String bundler, int time_secs, int warmup,
                             int nodes, int num_sender_threads, int msg_size) {
        this.cfg=config;
        this.bundler=bundler;
        this.time=time_secs;
        this.warmup=warmup;
        this.nodes=nodes;
        this.num_sender_threads=num_sender_threads;
        this.msg_size=msg_size;
    }


    protected BundlerStressTest createChannels() throws Exception {
        if(channels != null)
            Util.closeReverse(channels);
        String field_name=BUNDLER_TYPE.getName();
        channels=new JChannel[nodes];
        for(int i=0; i < channels.length; i++) {
            char ch=(char)('A' + i);
            String name=String.valueOf(ch);
            ProtocolStackConfigurator configurator=ConfiguratorFactory.getStackConfigurator(cfg);
            ProtocolConfiguration transport_config=configurator.getProtocolStack().get(0);
            transport_config.getProperties().put(field_name, bundler);
            channels[i]=new JChannel(configurator).name(name);
            GMS gms=channels[i].stack().findProtocol(GMS.class);
            gms.printLocalAddress(false);
            System.out.print(".");
            System.out.flush();
            channels[i].connect("bst");
            channels[i].setReceiver(new BundlerTestReceiver());
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        for(int i=0; i < channels.length; i++) {
            UNICAST3 uni=channels[i].getProtocolStack().findProtocol(UNICAST3.class);
            if(uni != null)
                uni.sendPendingAcks();
        }
        System.out.printf("\n-- view: %s (bundler=%s)\n", channels[0].getView(), getBundlerType());
        for(int i=0; i < channels.length; i++) {
            // UNICAST3.sendPendingAcks() (called by stop()) would cause an NPE (down_prot is null)
            UNICAST3 uni=channels[i].stack().findProtocol(UNICAST3.class);
            if(uni != null) {
                uni.stopRetransmitTask();
                uni.sendPendingAcks();
            }
            FD_SOCK2 fd=channels[i].stack().findProtocol(FD_SOCK2.class);
            if(fd != null)
                fd.setHandlerToNull(); // so we don't get a NPE (null down_prot)
        }
        thread_factory=channels[0].stack().getTransport().getThreadFactory();
        return removeProtocols();
    }

    // Removes all protocols but the transports
    protected BundlerStressTest removeProtocols() {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.stack();
            Protocol prot=stack.getTopProtocol();
            while(prot != null && !(prot instanceof TP)) {
                try {
                    stack.removeProtocol(prot);
                }
                catch(Throwable ignored) {}
                prot=stack.getTopProtocol();
            }
        }
        return this;
    }

    protected void start(boolean interactive) throws Exception {
        try {
            createChannels();
            if(interactive)
                loop();
            else {
                sendMessages(true);
                sendMessages(false);
            }
        }
        finally {
            stop();
        }
    }

    protected void stop() {
        Util.closeReverse(channels);
        channels=null;
    }

    protected String getBundlerType() {
        return channels[0].getProtocolStack().getTransport().getBundler().getClass().getSimpleName();
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("[1] send [2] num_sender_threads (%d) [3] nodes (%d) " +
                                                "[4] msg size (%d bytes) [5] time %s)\n" +
                                                "[b] change bundler (%s) [d] details (%b) [x] exit\n",
                                              num_sender_threads, nodes, msg_size, Util.printTime(time, SECONDS),
                                              getBundlerType(), details));
            try {
                switch(c) {
                    case '1':
                        sendMessages(false);
                        break;
                    case '2':
                        num_sender_threads=Util.readIntFromStdin("num_sender_threads: ");
                        break;
                    case '3':
                        int old=nodes;
                        nodes=Util.readIntFromStdin("nodes: ");
                        if(old != nodes)
                            createChannels();
                        break;
                    case '4':
                        msg_size=Util.readIntFromStdin("msg_size: ");
                        break;
                    case '5':
                        time=Util.readIntFromStdin("time (secs): ");
                        break;
                    case 'b':
                        String type=null;
                        try {
                            type=Util.readStringFromStdin("new bundler type: ");
                            TP tp=channels[0].getProtocolStack().getTransport();
                            tp.bundler(type);
                        }
                        catch(Throwable t) {
                            System.err.printf("failed changing bundler to %s: %s\n", type, t);
                        }
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
        stop();
    }

    protected Address pickRandomDestination() {
        if(channels == null) return null;
        int size=channels.length;
        int index=Util.random(size-1);
        return channels[index].getAddress();
    }

    protected void sendMessages(boolean is_warmup) throws Exception {
        sender_threads.clear();
        CountDownLatch latch=new CountDownLatch(1);
        LongAdder sent_msgs=new LongAdder();
        Thread[] threads=new Thread[num_sender_threads];
        Sender[] senders=new Sender[num_sender_threads];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(latch, sent_msgs);
            threads[i]=thread_factory.newThread(senders[i], "sender-" + i);
            threads[i].start();
        }
        if(is_warmup)
            System.out.printf("-- warmup for %d seconds\n", this.warmup);
        else
            System.out.printf("-- %d sender threads sending messages for %d seconds\n", num_sender_threads, time);
        long start=System.nanoTime();
        latch.countDown(); // starts all sender threads

        // wait for time seconds
        long t=is_warmup? this.warmup : this.time;
        long interval=(long)((t * 1000.0) / 10.0), sent_since_last_interval=0;
        for(int i=1; i <= 10; i++) {
            Util.sleep(interval);
            long curr=sent_msgs.sum();
            long sent=curr - sent_since_last_interval;
            sent_since_last_interval=curr;
            double reqs_sec=sent / (t/10.0);
            if(is_warmup)
                System.out.print(".");
            else
                System.out.printf("%d: %,.2f reqs/sec, %s / req\n", i, reqs_sec,
                                  Util.printTime(senders[0].send.average(), NANOSECONDS));
        }

        for(Sender sender: senders)
            sender.stop();
        for(Thread thread: threads)
            thread.join();
        if(is_warmup) {
            System.out.println();
            return;
        }
        long time_ns=System.nanoTime()-start;
        AverageMinMax send_avg=null;
        for(int i=0; i < num_sender_threads; i++) {
            Sender sender=senders[i];
            Thread thread=threads[i];
            if(details && !is_warmup)
                System.out.printf("[%d] count=%d, send-time = %s\n", thread.getId(), sender.send.count(), sender.send);
            if(send_avg == null)
                send_avg=sender.send;
            else
                send_avg.merge(sender.send);
        }

        long num_msgs=sent_msgs.sum();
        double msgs_sec=num_msgs / (time_ns / 1000.0 / 1000.0 / 1000.0);
        System.out.printf(Util.bold("\n" +
                                      "\nbundler:   %s" +
                                      "\nthreads:   %s" +
                                      "\nreqs/sec:  %,.2f (time: %s)" +
                                      "\nsend-time: %s / %s / %s (min/avg/max)\n"),
                          getBundlerType(), num_sender_threads,
                          msgs_sec, Util.printTime(time_ns, NANOSECONDS), Util.printTime(send_avg.min(), NANOSECONDS),
                          Util.printTime(send_avg.average(), NANOSECONDS),
                          Util.printTime(send_avg.max(), NANOSECONDS));
    }


    public static void main(String[] args) throws Exception {
        String bundler="transfer-queue", props="tcp.xml";
        int time=60, warmup=time/2, nodes=4, num_sender_threads=100, msg_size=1000;
        boolean interactive=true;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-bundler")) {
                bundler=args[++i];
                continue;
            }
            if("-time".equals(args[i])) {
                time=Integer.parseInt(args[++i]);
                warmup=time/2;
                continue;
            }
            if("-warmup".equals(args[i])) {
                warmup=Integer.parseInt(args[++i]);
                continue;
            }
            if("-nodes".equals(args[i])) {
                nodes=Integer.parseInt(args[++i]);
                continue;
            }
            if("-num_sender_threads".equals(args[i])) {
                num_sender_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if("-msg_size".equals(args[i])) {
                msg_size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-interactive".equals(args[i])) {
                interactive=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            System.out.print("BundlerStressTest [-props config] [-bundler bundler-type] [-time secs] [-warmup secs] " +
                               "[-num_sender_threads num] [-nodes num] [-msg_size size] [-interactive false|true]\n");
            return;
        }
        BundlerStressTest test=new BundlerStressTest(props, bundler, time, warmup, nodes, num_sender_threads, msg_size);
        test.start(interactive);
    }


    protected class Sender implements Runnable {
        protected final CountDownLatch latch;
        protected final AverageMinMax  send=new AverageMinMax().unit(NANOSECONDS); // ns
        protected long                 thread_id;
        protected Promise<Long>        promise;
        protected final LongAdder      sent_msgs;
        protected volatile boolean     running=true;

        public Sender(CountDownLatch latch, LongAdder sent_msgs) {
            this.latch=latch;
            this.sent_msgs=sent_msgs;
        }

        public void stop() {
            running=false;
        }

        public void run() {
            thread_id=Thread.currentThread().getId();
            sender_threads.put(thread_id, promise=new Promise<>());
            try {
                latch.await();
            }
            catch(InterruptedException e) {
            }
            byte[] buf=new byte[msg_size]; // the first 8 bytes are the thread_id
            Bits.writeLong(thread_id, buf, 0);
            while(running) {
                try {
                    promise.reset(false);
                    Address dest=pickRandomDestination();
                    Message msg=new BytesMessage(dest, buf);
                    long start=System.nanoTime();
                    channels[0].send(msg);
                    promise.getResult(0);
                    long time_ns=System.nanoTime()-start;
                    sent_msgs.increment();
                    send.add(time_ns); // single threaded; no lock needed
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected class BundlerTestReceiver implements Receiver {
        @Override
        public void receive(Message msg) {
            byte[] buf=msg.getArray();
            long thread_id=Bits.readLong(buf, msg.getOffset());
            Promise<Long> promise=sender_threads.get(thread_id);
            promise.setResult(thread_id); // wakes up sender
        }
    }

}
