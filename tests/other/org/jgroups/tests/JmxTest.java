package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * @author Bela Ban
 * @version $Id: JmxTest.java,v 1.10 2009/09/18 19:54:55 vlada Exp $
 */
public class JmxTest {
    MBeanServer server;
    JChannel channel;
    final String domain="JGroups";




    private boolean start(String props) throws Exception {
        server=Util.getMBeanServer();
        if(server == null) {
            System.err.println("No MBeanServers found;" +
                    "\nJmxTest needs to be run with an MBeanServer present, or inside JDK 5");
            return false;
        }
        channel=new JChannel(props);
        channel.connect("DemoChannel");
        JmxConfigurator.registerChannel(channel, server, domain, channel.getClusterName() , true);
        return true;
    }


    void doWork() throws Exception {
        server=Util.getMBeanServer();
        if(server == null) {
            System.err.println("No MBeanServers found;" +
                    "\nJmxTest needs to be run with an MBeanServer present, or inside JDK 5");
            return;
        }
        ObjectName channelName=new ObjectName("JGroups:channel=DemoChannel");

        // 1. get view and print it
        View v=(View)server.getAttribute(channelName, "View");
        System.out.println("view: " + v);

        // 2. send a bunch of messages
        System.out.println("sending some messages");
        Message msg;
        for(int i=0; i < 5; i++) {
            msg=new Message(null, null, "hello from " + i);
            server.invoke(channelName, "send", new Object[]{msg}, new String[]{msg.getClass().getName()});
        }

        Util.sleep(500);

        // 3. dump number of messages
        int numMsgs=((Integer)server.getAttribute(channelName, "NumMessages")).intValue();
        System.out.println("channel has " + numMsgs + " messages:");

        String queue=(String)server.invoke(channelName, "dumpQueue", null, null);
        System.out.println(queue);

        System.out.println("messages are:");
        Object obj;
        for(int i=0; i < numMsgs; i++) {
            obj=server.invoke(channelName, "receive", new Object[]{new Long(10)},
                              new String[]{long.class.getName()});
            System.out.println("#" + i + ": " + obj);
        }
    }

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
            boolean rc=false;
            JmxTest test=new JmxTest();
            rc=test.start(props);
            if(rc == false)
                return;
            // test.doWork();
            while(true)
                Util.sleep(60000);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
