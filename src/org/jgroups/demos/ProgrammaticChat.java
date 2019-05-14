package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;

/**
 * @author Bela Ban
 */
public class ProgrammaticChat {

    public static void main(String[] args) throws Exception {
        String name=null, bind_addr=null;
        for(int i=0; i < args.length; i++) {
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-bind_addr".equals(args[i])) {
                bind_addr=args[++i];
                continue;
            }
            System.out.printf("%s [-h] [-name name] [-bind_addr addr]\n", ProgrammaticChat.class.getSimpleName());
            return;
        }

        InetAddress bind_address=bind_addr != null? Util.getAddress(bind_addr, Util.getIpStackType()) : Util.getLoopback();
        Protocol[] prot_stack={
          new TCP().setBindAddress(bind_address).setBindPort(7800)
            .setDiagnosticsEnabled(true).diagEnableTcp(true).diagEnableUdp(false), // todo: remove when MulticastSocket works
          new TCPPING().initialHosts(Collections.singletonList(new InetSocketAddress(bind_address, 7800))),
          new MERGE3(),
          new FD_SOCK(),
          new FD_ALL(),
          new VERIFY_SUSPECT(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS(),
          new UFC(),
          new MFC(),
          new FRAG2()};
        JChannel ch=new JChannel(prot_stack).name(name);

        ch.setReceiver(new ReceiverAdapter() {
            public void viewAccepted(View new_view) {
                System.out.println("view: " + new_view);
            }

            public void receive(Message msg) {
                System.out.println("<< " + msg.getObject() + " [" + msg.getSrc() + "]");
            }
        });

        ch.connect("ChatCluster");


        for(;;) {
            String line=Util.readStringFromStdin(": ");
            ch.send(null, line);
        }
    }

}


