package org.jgroups.demos;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;

/** Demos RELAY. Create 2 *separate* clusters with RELAY as top protocol. Each RELAY has bridge_props="tcp.xml" (tcp.xml
 * needs to be present). Then start 2 instances in the first cluster and 2 instances in the second cluster. They should
 * find each other, and typing in a window should send the text to everyone, plus we should get 4 responses.
 * @author Bela Ban
 */
public class RelayDemo {
    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String name=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            System.out.println("RelayDemo [-props props] [-name name]");
            return;
        }

        final JChannel ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
        ch.setReceiver(new ReceiverAdapter() {
            public void receive(Message msg) {
                Address sender=msg.getSrc();
                System.out.println("<< " + msg.getObject() + " from " + sender);
                Address dst=msg.getDest();
                if(dst == null) {
                    Message rsp=new Message(msg.getSrc(), null, "this is a response");
                    try {
                        ch.send(rsp);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            public void viewAccepted(View new_view) {
                System.out.println(print(new_view));
            }
        });

        ch.connect("RelayDemo");

        for(;;) {
            String line=Util.readStringFromStdin(": ");
            ch.send(null, line);
        }
    }


    static String print(View view) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        sb.append(view.getClass().getSimpleName() + ": ").append(view.getViewId()).append(": ");
        for(Address mbr: view.getMembers()) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(mbr);
        }
        return sb.toString();
    }
}
