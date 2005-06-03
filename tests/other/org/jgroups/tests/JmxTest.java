package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

/**
 * @author Bela Ban
 * @version $Id: JmxTest.java,v 1.1 2005/06/03 08:53:52 belaban Exp $
 */
public class JmxTest {
    MBeanServer server;
    JChannel channel;
    final String channel_name="JGroups:channel=";

    public static void main(String[] args) {
        String props=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            System.out.println("JmxTest1_5 [-props <props>]");
        }

        try {
            new JmxTest().start(props);
            while(true)
                Util.sleep(60000);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void start(String props) throws Exception {
        server=(MBeanServer)MBeanServerFactory.findMBeanServer(null).get(0);
        channel=new JChannel(props);
        channel.connect("DemoChannel");
        JmxConfigurator.registerChannel(channel, server, channel_name + channel.getChannelName() , true);
    }
}
