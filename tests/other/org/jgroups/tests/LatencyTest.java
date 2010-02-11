package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id: LatencyTest.java,v 1.7 2010/02/11 08:04:21 belaban Exp $
 */
public class LatencyTest {
    JChannel ch;


    private void start(boolean sender, boolean local, String props) throws Exception {
        if(local) {
            JChannel ch1, ch2;
            ch1=new JChannel(props);
            ch1.connect("x");
            ch2=new JChannel(props);
            ch2.setReceiver(new MyReceiver());
            ch2.connect("x");
            for(int i=0; i < 10; i++) {
                Message msg=new Message();
                msg.setFlag((byte)(Message.DONT_BUNDLE | Message.NO_FC));
                msg.setObject(System.nanoTime());
                ch1.send(msg);
                Util.sleep(1000);
            }
            ch2.close();
            ch1.close();
            return;
        }

        if(sender) {
            ch=new JChannel(props);
            disableBundling(ch);
            ch.connect("x");
            for(int i=0; i < 10; i++) {
                Message msg=new Message();
                msg.setFlag((byte)(Message.DONT_BUNDLE | Message.NO_FC));
                msg.setObject(System.nanoTime());
                ch.send(msg);
                Util.sleep(1000);
            }
            ch.close();
        }
        else {
            ch=new JChannel(props);
            disableBundling(ch);
            ch.setReceiver(new MyReceiver());
            ch.connect("x");
            System.out.println("receiver ready");
            while(true)
                Util.sleep(10000);
        }
    }

    private static void disableBundling(JChannel ch) {
        System.out.println("Disabling message bundling (as this would increase latency)");
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        transport.setEnableBundling(false);
    }


    static class MyReceiver extends ReceiverAdapter {

        public void receive(Message msg) {
            Long timestamp=(Long)msg.getObject();
            long time=System.nanoTime() - timestamp.longValue();
            double time_ms=time / 1000.0 / 1000.0;
            System.out.println("time for message: " + Util.format(time_ms) + " ms");
        }
    }

    public static void main(String[] args) throws Exception {
        boolean sender=false, local=false;
        String props=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equalsIgnoreCase("-sender")) {
                sender=true;
                continue;
            }
            if(args[i].equalsIgnoreCase("-local")) {
                local=true;
                continue;
            }
            if(args[i].equalsIgnoreCase("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }
        new LatencyTest().start(sender, local, props);
    }


    private static void help() {
        System.out.println("JGroupsLatencyTest [-sender] [-local] [-props <properties>]");
    }

}
