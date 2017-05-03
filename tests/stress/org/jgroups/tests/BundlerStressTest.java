package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.*;
import org.jgroups.util.AsciiString;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


/**
 * Tests bundler performance
 * @author Bela Ban
 * @since  4.0
 */
public class BundlerStressTest {
    protected String                 bundler_type;
    protected Bundler                bundler;
    protected int                    num_msgs=50000, num_senders=20, msg_size=1000;
    protected boolean                details;
    protected static final Address[] ADDRESSES;
    protected final TP               transport=new MockTransport();
    protected static final int       BUFSIZE=50000;


    static {
        ADDRESSES=new Address[]{null, Util.createRandomAddress("A"), Util.createRandomAddress("B"),
          Util.createRandomAddress("C"), Util.createRandomAddress("D"), Util.createRandomAddress("E"),
          Util.createRandomAddress("F"), Util.createRandomAddress("G"), Util.createRandomAddress("H")};
    }


    public BundlerStressTest(String bundler_type) {
        this.bundler_type=bundler_type;
    }

    protected void start() {
        this.bundler=createBundler(bundler_type);
        this.bundler.init(transport);
        this.bundler.start();
        loop();
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("[1] send [2] num_msgs (%d) [3] senders (%d) [4] msg size (%d bytes)\n" +
                                                "[b] change bundler (%s) [d] details (%b) [x] exit\nbundler: %s\n",
                                              num_msgs, num_senders, msg_size, bundler.getClass().getSimpleName(),
                                              details, bundler.toString()));
            try {
                switch(c) {
                    case '1':
                        sendMessages();
                        break;
                    case '2':
                        num_msgs=Util.readIntFromStdin("num_msgs: ");
                        break;
                    case '3':
                        num_senders=Util.readIntFromStdin("num_senders: ");
                        break;
                    case '4':
                        msg_size=Util.readIntFromStdin("msg_size: ");
                        break;
                    case 'b':
                        String type=null;
                        try {
                            type=Util.readStringFromStdin("new bundler type: ");
                            Bundler old=this.bundler;
                            this.bundler=createBundler(type);
                            this.bundler.init(transport);
                            this.bundler.start();
                            if(old != null)
                                old.stop();
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
        if(this.bundler != null)
            this.bundler.stop();
    }

    protected void sendMessages() throws Exception {
        Message[] msgs=generateMessages(num_msgs);
        CountDownLatch latch=new CountDownLatch(1);
        AtomicInteger index=new AtomicInteger(0);
        Sender[] senders=new Sender[num_senders];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(latch, msgs, index);
            senders[i].start();
        }

        long start=Util.micros();
        latch.countDown(); // starts all sender threads

        for(Sender sender: senders)
            sender.join();

        // wait until the bundler has no pending msgs left
        long park_time=1;
        for(int i=0; i < 1_000_000; i++) {
            int pending_msgs=bundler.size();
            if(pending_msgs == 0)
                break;

            LockSupport.parkNanos(park_time);
            if(i % 10000 == 0) {
                park_time=Math.min(park_time*2, 1_000_000); // 1 ms max park time
            }

        }
        if(bundler.size() > 0)
            throw new Exception(String.format("bundler still has %d pending messages", bundler.size()));

        long time_us=Util.micros()-start;
        AverageMinMax send_avg=null;
        for(Sender sender: senders) {
            if(details)
                System.out.printf("[%d] count=%d, send-time = %s\n", sender.getId(), sender.send.count(), sender.send);
            if(send_avg == null)
                send_avg=sender.send;
            else
                send_avg.merge(sender.send);
        }




        double msgs_sec=num_msgs / (time_us / 1_000.0);
        System.out.printf(Util.bold("\n\nreqs/ms    = %.2f (time: %d us)" +
                                      "\nsend-time  = min/avg/max: %d / %.2f / %d ns\n"),
                          msgs_sec, time_us, send_avg.min(), send_avg.average(), send_avg.max());
    }

    protected Bundler createBundler(String bundler) {
        if(bundler == null)
            throw new IllegalArgumentException("bundler type has to be non-null");
        if(bundler.equals("stq"))
            return new SimplifiedTransferQueueBundler(BUFSIZE);
        if(bundler.equals("tq"))
            return new TransferQueueBundler(BUFSIZE);
        if(bundler.startsWith("sender-sends") || bundler.equals("ss"))
            return new SenderSendsBundler();
        if(bundler.endsWith("ring-buffer") || bundler.equals("rb"))
            return new RingBufferBundler(BUFSIZE);
        if(bundler.equals("ring-buffer-lockless") || bundler.equals("rbl"))
            return new RingBufferBundlerLockless(BUFSIZE);
        if(bundler.equals("ring-buffer-lockless2") || bundler.equals("rbl2"))
            return new RingBufferBundlerLockless2(BUFSIZE);
        if(bundler.startsWith("no-bundler") || bundler.equals("nb"))
            return new NoBundler();
        try {
            Class<Bundler> clazz=Util.loadClass(bundler, getClass());
            return clazz.newInstance();
        }
        catch(Throwable t) {
            throw new IllegalArgumentException(String.format("failed creating instance of bundler %s: %s", bundler, t));
        }
    }

    protected Message[] generateMessages(int num) {
        Message[] msgs=new Message[num];
        for(int i=0; i < msgs.length; i++)
            msgs[i]=new Message(pickAddress(), new byte[msg_size]);
        return msgs;
    }

    protected static Address pickAddress() {
        return Util.pickRandomElement(ADDRESSES);
    }

    public static void main(String[] args) {
        String bundler="ring-buffer-lockless2";
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-bundler")) {
                bundler=args[++i];
                continue;
            }
            System.out.printf("BundlerStressTest [-bundler bundler-type]\n");
            return;
        }
        new BundlerStressTest(bundler).start();
    }


    protected class Sender extends Thread {
        protected final CountDownLatch latch;
        protected final Message[]      msgs;
        protected final AtomicInteger  index;
        protected final AverageMinMax  send=new AverageMinMax(); // ns

        public Sender(CountDownLatch latch, Message[] msgs, AtomicInteger index) {
            this.latch=latch;
            this.msgs=msgs;
            this.index=index;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
            }
            while(true) {
                int idx=index.getAndIncrement();
                if(idx >= msgs.length)
                    break;
                try {
                    long start=System.nanoTime();
                    bundler.send(msgs[idx]);
                    long time_ns=System.nanoTime()-start;
                    send.add(time_ns);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected static class MockTransport extends TP {


        public MockTransport() {
            this.cluster_name=new AsciiString("mock");
            thread_factory=new DefaultThreadFactory("", false);
        }

        public boolean supportsMulticasting() {
            return false;
        }

        public void doSend(byte[] buf, int offset, int length, Address dest) throws Exception {

        }

        public void sendMulticast(byte[] data, int offset, int length) throws Exception {

        }

        public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {

        }

        public String getInfo() {
            return null;
        }

        protected PhysicalAddress getPhysicalAddress() {
            return null;
        }
    }

}
