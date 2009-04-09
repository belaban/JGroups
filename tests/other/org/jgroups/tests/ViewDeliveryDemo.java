package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.jgroups.util.BoundedList;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Verify that all messages are delivered in the view they are sent in
 * regardless of members joining, leaving or crashing.
 * @author rnewson
 * @version $Id: ViewDeliveryDemo.java,v 1.15 2009/04/09 09:11:20 belaban Exp $
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
            case REOPEN:
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
            /*case REOPEN:
                reopen();
                break;*/
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
        try {
            for (int i = 0; i < max; i++) {
                channel.send(null, null, channel.getView());
            }
        }
        catch(Throwable t) {
            System.err.println("failed to send messages");
            t.printStackTrace();
        }
    }

    private static void reopen() throws Exception {
        System.out.println("closing and reopening.");
        try {
            channel.close();
            System.out.println("closed");
            Thread.sleep(random.nextInt(5000));
            channel.open();
            channel.connect("view_test");
        }
        catch(Throwable t) {
            System.err.println("failed to reopen the channel");
            t.printStackTrace();
        }
    }

    private static void reconnect() throws Exception {
        System.out.println("disconnecting and reconnecting.");
        try {
            channel.disconnect();
            System.out.println("disconnected");
            Thread.sleep(random.nextInt(5000));
            System.out.println("connecting");
            channel.connect("view_test");
        }
        catch(Throwable t) {
            System.err.println("failed to reconnect channel");
            t.printStackTrace();
        }
    }



    private static class MyReceiver extends ExtendedReceiverAdapter implements Runnable {
        ViewId my_vid;
        long last_time=System.currentTimeMillis();
        static final long MAX_TIME=10000;
        final AtomicInteger count=new AtomicInteger(0), violations=new AtomicInteger(0);
        final List<String> violations_list=new LinkedList<String>();
        final BoundedList<View> views=new BoundedList<View>(10);

        private MyReceiver() {
            new Thread(this).start();
        }

        public void run() {
            while(true) {
                Util.sleep(MAX_TIME);
                StringBuilder sb=new StringBuilder();
                sb.append("==> received " + count.get() + " valid msgs, " + violations.get() + " violations so far");
                if(violations.get() > 0) {
                    sb.append("violations:\n" + printViolationsList());
                }
                sb.append("\nlast views:\n").append(printViews() + "\n");

                System.out.println(sb);
            }
        }

        private String printViews() {
            StringBuilder sb=new StringBuilder();
            for(View view: views)
                sb.append(view).append("\n");
            return sb.toString();
        }


        public void viewAccepted(View new_view) {
            System.out.println("new_view = " + new_view);
            my_vid=new_view.getVid();
            views.add(new_view);
        }
        

        public void receive(final Message msg) {
            final Object obj = msg.getObject ();
            if (obj instanceof View) {
                count.incrementAndGet();
                final View viewArrived = (View) obj;
                final ViewId sent_in_vid = viewArrived.getVid();
                final ViewId arrived_in_vid = my_vid;
                if (!sent_in_vid.equals(arrived_in_vid) && viewArrived.containsMember(channel.getAddress())) {
                    String tmp="*** VIOLATION: message sent in view "+sent_in_vid+" received in "+arrived_in_vid+"\n" +
                            "msg: " + msg + ", headers: " + msg.getHeaders();
                    violations_list.add(tmp);
                    System.out.println(tmp);
                    violations.incrementAndGet();
                }
            }
            else {
                System.out.println("ERROR: unexpected payload: " + obj);
            }
        }

        private String printViolationsList() {
            StringBuilder sb=new StringBuilder();
            for(String tmp: violations_list)
                sb.append(tmp).append("\n");
            return sb.toString();
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
