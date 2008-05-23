package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.Properties;

/**
 * @author Bela Ban
 * @version $Id: JGroupsLatencyTest.java,v 1.6 2008/05/23 10:46:00 belaban Exp $
 */
public class JGroupsLatencyTest {
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
                ch1.send(new Message(null, null, System.currentTimeMillis()));
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
                ch.send(new Message(null, null, System.currentTimeMillis()));
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
            System.out.println("time for message: " + (System.currentTimeMillis() - timestamp.longValue()) + " ms");
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
        new JGroupsLatencyTest().start(sender, local, props);
    }


    private static void help() {
        System.out.println("JGroupsLatencyTest [-sender] [-local] [-props <properties>]");
    }

}
