package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Date;

/**
 * Tests sending large messages from one sender to multiple receivers
 * @author Bela Ban
 * @version $Id: MySpaceTest.java,v 1.1.2.1 2008/07/22 08:06:34 belaban Exp $
 */
public class MySpaceTest {
    private final boolean sender;
    private final String props;
    private final int sleep;
    private JChannel ch;
    private final int MIN_SIZE, MAX_SIZE;

    public MySpaceTest(boolean sender, String props, int sleep, int min, int max) {
        this.sender=sender;
        this.props=props;
        this.sleep=sleep;
        this.MIN_SIZE=min;
        this.MAX_SIZE=max;
    }

    public void start() throws ChannelException {
        ch=new JChannel(props);
        ch.setReceiver(new MyReceiver(ch));
        ch.connect("MySpaceCluster");
        if(sender) {
            System.out.println("min=" + Util.printBytes(MIN_SIZE) + ", max=" + Util.printBytes(MAX_SIZE) + ", sleep time=" + sleep);
            while(true) {
                Util.sleepRandom(sleep);
                sendMessage();
            }
        }
    }

    private void sendMessage() throws ChannelException {
        int size=(int)Util.random(MAX_SIZE);
        // System.out.println("size = " + Util.printBytes(size));
        size=Math.max(size, MIN_SIZE);
        byte[] buf=new byte[size];
        Message msg=new Message(null, null, buf);
        System.out.println("\n[" + new Date() + "] --> sending " + Util.printBytes(size));
        ch.send(msg);
    }

    public static void main(String[] args) throws ChannelException {
        boolean sender=false;
        int sleep=10000, min=100 * 1000, max=100 * 1000 * 1000;
        String props="udp.xml";

        for(int i=0; i < args.length; i++) {
            String tmp=args[i];
            if(tmp.equalsIgnoreCase("-sender")) {
                sender=true;
                continue;
            }
            if(tmp.equalsIgnoreCase("-props")) {
                props=args[++i];
                continue;
            }
            if(tmp.endsWith("-sleep")) {
                sleep=Integer.parseInt(args[++i]);
                continue;
            }
            if(tmp.endsWith("-min")) {
                min=Integer.parseInt(args[++i]);
                continue;
            }
            if(tmp.endsWith("-max")) {
                max=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }
        new MySpaceTest(sender, props, sleep, min, max).start();
    }

    static void help() {
        System.out.println("MySpaceTest [-sender] [-props <props>] [-sleep <time in ms>] [-min <size>] [-max <size>]");
    }


    private static class MyReceiver extends ReceiverAdapter {
        private final JChannel channel;

        public MyReceiver(JChannel channel) {
            this.channel=channel;
        }

        public void viewAccepted(View new_view) {
            log("view: " + new_view);
        }

        public void receive(Message msg) {
            int len=msg.getLength();
            log("<-- received " + Util.printBytes(len) + " from " + msg.getSrc());
        }

        private static void log(String msg) {
            System.out.println("[" + new Date() + "]: " + msg);
        }
    }
}
