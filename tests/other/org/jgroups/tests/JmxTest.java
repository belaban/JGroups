package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.util.ArrayList;

/**
 * @author Bela Ban
 * @version $Id: JmxTest.java,v 1.3 2005/06/03 09:00:59 belaban Exp $
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
            System.out.println("JmxTest [-props <props>]");
        }

        try {
            boolean rc=new JmxTest().start(props);
            if(rc == false)
                return;
            while(true)
                Util.sleep(60000);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private boolean start(String props) throws Exception {
        ArrayList servers=MBeanServerFactory.findMBeanServer(null);
        if(servers == null || servers.size() == 0) {
            System.err.println("No MBeanServers found;" +
                               "\nJmxTest needs to be run with an MBeanServer present, or inside JDK 5");
            return false;
        }
        server=(MBeanServer)servers.get(0);
        channel=new JChannel(props);
        channel.connect("DemoChannel");
        JmxConfigurator.registerChannel(channel, server, channel_name + channel.getChannelName() , true);
        return true;
    }
}
