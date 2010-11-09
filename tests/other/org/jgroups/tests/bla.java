package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id: ProgrammaticChat.java,v 1.2 2010/10/20 11:35:10 belaban Exp $
 */
public class bla {



    public static void main(String[] args) throws Exception {
        JChannel ch=new JChannel("/home/bela/udp.xml");

        ch.setReceiver(new ReceiverAdapter() {
            public void viewAccepted(View new_view) {
                System.out.println("view: " + new_view);
    }

            public void receive(Message msg) {
                System.out.println("<< " + new String(msg.getBuffer()) + " [" + msg.getSrc() + "]");
            }
        });

        ch.connect("ChatCluster");


        for(;;) {
            String line=Util.readStringFromStdin(": ");

            byte[] buf=line.getBytes();
            ch.send(null, null, buf);
}
    }



}


