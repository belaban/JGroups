/* $Id$ */
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import java.io.File;

public class MyTest {
    public static void main(String... args) throws Exception {
        final JChannel channel = new JChannel(args[0]);
        channel.setReceiver(new ReceiverAdapter() {
            public void receive(Message msg) {
                System.out.println("received msg from " + msg.getSrc() + ": " + msg.getObject());
            }

            public void viewAccepted(View new_view) {
                boolean isCoord=Util.isCoordinator(new_view, channel.getAddress());
                System.out.println("new_view = " + new_view + ", local=" + channel.getAddress() + ", coord=" + isCoord);
            }
        });
        channel.connect("MyCluster");
        JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "testDomain", channel.getClusterName(), true);
        System.out.println("Started");
    }
}
