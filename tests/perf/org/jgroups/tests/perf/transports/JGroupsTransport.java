package org.jgroups.tests.perf.transports;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.tests.perf.Receiver;
import org.jgroups.tests.perf.Transport;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.util.Map;
import java.util.Properties;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: JGroupsTransport.java,v 1.13 2006/06/01 09:26:26 belaban Exp $
 */
public class JGroupsTransport extends org.jgroups.ReceiverAdapter implements Transport  {
    Properties config=null;
    JChannel   channel=null;
    Thread     t=null;
    String     props=null;
    String     group_name="perf";
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
        channel.setReceiver(this);
    }

    public void start() throws Exception {
        channel.connect(group_name);
        if(jmx) {
            MBeanServer server=Util.getMBeanServer();
            if(server == null) {
                throw new Exception("No MBeanServers found;" +
                        "\nneeds to be run with an MBeanServer present, or inside JDK 5");
            }
            JmxConfigurator.registerChannel(channel, server, "jgroups.perf", channel.getChannelName() , true);
        }
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
        if(channel != null)
            channel.send(msg);
    }


    public void receive(Message msg) {
        Address sender=msg.getSrc();
        byte[] payload=msg.getBuffer();
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
