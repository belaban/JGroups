package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.PARTITION;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.lang.management.ManagementFactory;
import java.util.Vector;

/**
 * Simple test fo primary partitions
 * @author Bela Ban
 * @version $Id: PrimaryPartitionTest.java,v 1.2 2007/08/27 12:35:14 belaban Exp $
 */
public class PrimaryPartitionTest {

    public static void main(String[] args) throws Exception {
        final JChannel ch=new JChannel("/home/bela/udp.xml");
        ch.getProtocolStack().insertProtocol(new PARTITION(), ProtocolStack.ABOVE, "UDP");
        ch.setReceiver(new ExtendedReceiverAdapter() {
            public void viewAccepted(View new_view) {
                handleView(ch, new_view);
            }
        });
        ch.connect("x");
        JmxConfigurator.registerChannel(ch, ManagementFactory.getPlatformMBeanServer(), "demo", ch.getClusterName(), true);
        while(ch.isConnected())
            Util.sleep(5000);
    }

    private static void handleView(JChannel ch, View new_view) {
        System.out.println("VIEW: " + new_view);
        if(new_view instanceof MergeView) {
            ViewHandler handler=new ViewHandler(ch, (MergeView)new_view);
            handler.start();
        }
    }


    private static class ViewHandler extends Thread {
        JChannel ch;
        MergeView view;

        private ViewHandler(JChannel ch, MergeView view) {
            this.ch=ch;
            this.view=view;
        }

        public void run() {
            Vector<View> subgroups=view.getSubgroups();
            View tmp_view=getPrimaryView(subgroups);
            Address local_addr=ch.getLocalAddress();
            if(!tmp_view.getMembers().contains(local_addr)) {
                System.out.println("I (" + local_addr + ") am not member of the new primary partition (" + tmp_view +
                        "), will re-acquire the state");
                try {
                    ch.getState(null, 30000);
                }
                catch(Exception ex) {
                }
            }
            else {
                System.out.println("I (" + local_addr + ") am member of the new primary partition (" + tmp_view +
                        "), will do nothing");
            }
        }

        private static View getPrimaryView(Vector<View> subgroups) {
            return subgroups.firstElement();
        }
    }
}
