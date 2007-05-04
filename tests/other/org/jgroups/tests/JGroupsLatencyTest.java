package org.jgroups.tests;

import org.jgroups.util.Util;
import org.jgroups.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.DatagramPacket;

/**
 * @author Bela Ban
 * @version $Id: JGroupsLatencyTest.java,v 1.1 2007/05/04 11:47:59 belaban Exp $
 */
public class JGroupsLatencyTest {
    JChannel ch;


    private void start(boolean sender, String props) throws Exception {
        ch=new JChannel(props);

        if(sender) {
            ch.connect("x");
            for(int i=0; i < 10; i++) {
                ch.send(new Message(null, null, System.currentTimeMillis()));
                Util.sleep(1000);
            }
            ch.close();
        }
        else {
            System.out.println("receiver ready");
            ch.setReceiver(new MyReceiver());
            ch.connect("x");
            while(true)
                Util.sleep(10000);
        }
    }



    static class MyReceiver extends ReceiverAdapter {

        public void receive(Message msg) {
            Long timestamp=(Long)msg.getObject();
            System.out.println("time for message: " + (System.currentTimeMillis() - timestamp.longValue()) + " ms");
        }
    }

    public static void main(String[] args) throws Exception {
        boolean sender=false;
        String props=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equalsIgnoreCase("-sender")) {
                sender=true;
                continue;
            }
            if(args[i].equalsIgnoreCase("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }
        new JGroupsLatencyTest().start(sender, props);
    }


    private static void help() {
        System.out.println("JGroupsLatencyTest [-sender] [-props <properties>]");
    }

}
