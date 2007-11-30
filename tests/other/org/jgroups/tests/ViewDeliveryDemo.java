package org.jgroups.tests;

import org.jgroups.*;

import java.security.SecureRandom;
import java.util.Random;

/**
 * Verify that all messages are delivered in the view they are sent in
 * regardless of members joining, leaving or crashing.
 * @author rnewson
 *
 */
public final class ViewDeliveryDemo {


    private static final int SEND = 0;
    private static final int REOPEN = 1;
    private static final int RECONNECT = 2;

    private static Channel channel=null;
    private static final Random random = new SecureRandom();
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
        channel.connect("view_test");
        mr = new MyReceiver();
        channel.setReceiver(mr);

        while (true) {
            switch (random.nextInt(3)) {
            case SEND:
                send();
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
            channel.send(null, null, mr.getViewId());
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



    private static class MyReceiver extends ReceiverAdapter {
        ViewId my_vid;
        long last_time=System.currentTimeMillis();
        static final long MAX_TIME=20000;
        int count=0, violations=0;


        public void viewAccepted(View new_view) {
            System.out.println("new_view = " + new_view);
            my_vid=new_view.getVid();
        }
        
        public ViewId getViewId(){
            return my_vid;            
        }

        public void receive(final Message msg) {
            final Object obj = msg.getObject ();
            if (obj instanceof ViewId) {
                count++;
                final ViewId sent_in_vid = (ViewId) obj;
                final ViewId arrived_in_vid = my_vid;
                if (!sent_in_vid.equals(arrived_in_vid)) {
                    System.out.printf("******** VIOLATION: message sent in view %s received in %s\n",
                                      sent_in_vid, arrived_in_vid);
                    violations++;
                }

                long curr_time=System.currentTimeMillis();
                long diff=curr_time - last_time;
                if(diff > MAX_TIME) {
                    last_time=System.currentTimeMillis();
                    System.out.println("-- received " + count + " valid msgs, " + violations + " violations so far");
                }
            }
            else {
                System.out.println("ERROR: unexpected payload: " + obj);
            }
        }
    }



    
}
