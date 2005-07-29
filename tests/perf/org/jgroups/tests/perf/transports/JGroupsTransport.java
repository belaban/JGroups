package org.jgroups.tests.perf.transports;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.tests.perf.Receiver;
import org.jgroups.tests.perf.Transport;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: JGroupsTransport.java,v 1.6 2005/07/29 08:59:37 belaban Exp $
 */
public class JGroupsTransport implements Transport, Runnable {
    Properties config=null;
    JChannel   channel=null;
    Thread     t=null;
    String     props=null;
    String     group_name="PerfGroup";
    Receiver   receiver=null;
    boolean    jmx=false;

    public JGroupsTransport() {

    }

    public Object getLocalAddress() {
        return channel != null? channel.getLocalAddress() : null;
    }

    public void create(Properties properties) throws Exception {
        this.config=properties;
        props=config.getProperty("props");
        jmx=new Boolean(this.config.getProperty("jmx")).booleanValue();
        channel=new JChannel(props);
    }

    public void start() throws Exception {
        channel.connect(group_name);
        if(jmx) {
            ArrayList servers=MBeanServerFactory.findMBeanServer(null);
            if(servers == null || servers.size() == 0) {
                throw new Exception("No MBeanServers found;" +
                                    "\nneeds to be run with an MBeanServer present, or inside JDK 5");
            }
            MBeanServer server=(MBeanServer)servers.get(0);
            JmxConfigurator.registerChannel(channel, server, "PerfTest:channel=" + channel.getChannelName() , true);
        }
        t=new Thread(this, "JGroupsTransport receiver thread");
        t.start();
    }

    public void stop() {
        if(channel != null) {
            channel.shutdown(); // will cause thread to terminate anyways
        }
        t=null;
    }


    public void destroy() {
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }

    public void setReceiver(Receiver r) {
        this.receiver=r;
    }

    public Map dumpStats() {
        return channel != null? channel.dumpStats() : null;
    }

    public void send(Object destination, byte[] payload) throws Exception {
        Message msg=new Message((Address)destination, null, payload);
        channel.send(msg);
    }

    public void run() {
        Object obj;
        Message msg;
        Object sender;
        byte[] payload;

        while(t != null) {
            try {
                obj=channel.receive(0);
                if(obj instanceof Message) {
                    msg=(Message)obj;
                    sender=msg.getSrc();
                    payload=msg.getBuffer();
                    if(receiver != null) {
                        try {
                            receiver.receive(sender, payload);
                        }
                        catch(Throwable tt) {
                            tt.printStackTrace();
                        }
                    }
                }
            }
            catch(ChannelNotConnectedException e) {
                t=null;
            }
            catch(ChannelClosedException e) {
                t=null;
            }
            catch(TimeoutException e) {
                e.printStackTrace();
            }
            catch(Throwable t) {

            }
        }
    }
}
