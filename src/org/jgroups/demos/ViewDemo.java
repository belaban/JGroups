
package org.jgroups.demos;


import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;


/**
 * Demos the reception of views using a PullPushAdapter. Just start a number of members, and kill them
 * randomly. The view should always be correct.
 */
public class ViewDemo extends ReceiverAdapter {
    private Channel channel;


    public void viewAccepted(View new_view) {
        System.out.println("** New view: " + new_view);
    }


    /**
     * Called when a member is suspected
     */
    public void suspect(Address suspected_mbr) {
        System.out.println("Suspected(" + suspected_mbr + ')');
    }



    public void start(String props) throws Exception {

        channel=new JChannel(props);
        channel.setReceiver(this);
        channel.connect("ViewDemo");

        while(true) {
            Util.sleep(10000);
        }
    }


    public static void main(String args[]) {
        ViewDemo t=new ViewDemo();
        String props="udp.xml";

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-bind_addr".equals(args[i])) {
                System.setProperty("jgroups.bind_addr", args[++i]);
                continue;
            }
            help();
            return;
        }

        try {
            t.start(props);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("ViewDemo [-props <properties>] [-help] [-use_additional_data <flag>] [-bind_addr <address>]");
    }

}
