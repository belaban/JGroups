package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Verify that all messages are delivered in the view they are sent in
 * regardless of members joining, leaving or crashing.
 * @author rnewson
 * @version $Id: ViewDeliveryDemo.java,v 1.8 2007/11/30 16:19:19 belaban Exp $
 *
 */
public final class ViewDeliveryDemo {


    private static final int SEND = 0;
    private static final int REOPEN = 1;
    private static final int RECONNECT = 2;

    private static Channel channel=null;
    private static final Lock lock=new ReentrantLock();
    private static boolean blocked=false;
    private static final Random random = new Random();
    private static MyReceiver mr = null;

    static String props="flush-udp.xml";

    public static void main(final String[] args) throws Exception {
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            System.out.println("ViewDeliveryDemo [-help] [-props <props>]");
            return;
        }

        channel=new JChannel(props);
        mr = new MyReceiver();
        channel.setReceiver(mr);
        channel.setOpt(Channel.BLOCK, true);
        channel.connect("view_test");


        while (true) {
            switch (random.nextInt(3)) {
            case SEND:
                lock.lock();
                try {
                    if(!blocked)
                        send();
                    else
                        System.out.println("Didn't send any messages because I was blocked");
                }
                finally {
                    lock.unlock();
                }

                break;
            case REOPEN:
                reopen();
                break;
            case RECONNECT:
                reconnect();
                break;
            default:
                assert false;
            }
            Thread.sleep(random.nextInt(2000));
        }
    }

    private static void send() throws Exception {
        int max=random.nextInt(1000);
        System.out.println("Sending " + max + " messages");
        for (int i = 0; i < max; i++) {
            channel.send(null, null, channel.getView().getVid());
        }
    }

    private static void reopen() throws Exception {
        System.out.println("closing and reopening.");
        channel.close();
        Thread.sleep(random.nextInt(5000));
        channel.open();
        channel.connect("view_test");
    }

    private static void reconnect() throws Exception {
        System.out.println("disconnecting and reconnecting.");
        channel.disconnect();
        Thread.sleep(random.nextInt(5000));
        channel.connect("view_test");
    }



    private static class MyReceiver extends ExtendedReceiverAdapter implements Runnable {
        ViewId my_vid;
        long last_time=System.currentTimeMillis();
        static final long MAX_TIME=10000;
        final AtomicInteger count=new AtomicInteger(0), violations=new AtomicInteger(0);

        private MyReceiver() {
            new Thread(this).start();
        }

        public void run() {
            while(true) {
                Util.sleep(MAX_TIME);
                System.out.println("==> received " + count.get() + " valid msgs, " + violations.get() + " violations so far");
            }
        }


        public void viewAccepted(View new_view) {
            System.out.println("new_view = " + new_view);
            my_vid=new_view.getVid();
        }
        

        public void receive(final Message msg) {
            final Object obj = msg.getObject ();
            if (obj instanceof ViewId) {
                count.incrementAndGet();
                final ViewId sent_in_vid = (ViewId) obj;
                final ViewId arrived_in_vid = my_vid;
                if (!sent_in_vid.equals(arrived_in_vid)) {
                    System.out.printf("******** VIOLATION: message sent in view %s received in %s\n",
                                      sent_in_vid, arrived_in_vid);
                    violations.incrementAndGet();
                }
            }
            else {
                System.out.println("ERROR: unexpected payload: " + obj);
            }
        }

        public void block() {
            System.out.println("block()");
            lock.lock();
            try {
                blocked=true;
            }
            finally {
                lock.unlock();
            }
        }

        public void unblock() {
            System.out.println("unblock()");
            lock.lock();
            try {
                blocked=false;
            }
            finally {
                lock.unlock();
            }
        }
    }



    
}
