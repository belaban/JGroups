package org.jgroups.jmx;

import org.jgroups.Channel;

import javax.management.MBeanServerFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.ArrayList;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactory.java,v 1.2 2006/04/26 22:14:12 belaban Exp $
 */
public class JChannelFactory implements JChannelFactoryMBean {
    org.jgroups.JChannelFactory factory=new org.jgroups.JChannelFactory();
    MBeanServer server=null;

    public void setMultiplexerConfig(String properties) throws Exception {
        factory.setMultiplexerConfig(properties);
    }

    public String getMultiplexerConfig() {
        return factory.getMultiplexerConfig();
    }

    public String getObjectName() {
        return factory.getObjectName();
    }

    public void setObjectName(String name) {
        factory.setObjectName(name);
    }

    public boolean isExposeChannels() {
        return factory.isExposeChannels();
    }

    public void setExposeChannels(boolean flag) {
        factory.setExposeChannels(flag);
    }

    public boolean isExposeProtocols() {
        return factory.isExposeProtocols();
    }

    public void setExposeProtocols(boolean f) {
        factory.setExposeProtocols(f);
    }


    public Channel createMultiplexerChannel(String stack_name, String id) throws Exception {
        return factory.createMultiplexerChannel(stack_name, id);
    }

    public Channel createMultiplexerChannel(String stack_name, String id, boolean register_for_state_transfer, String substate_id) throws Exception {
        return factory.createMultiplexerChannel(stack_name, id, register_for_state_transfer, substate_id);
    }

    public void create() throws Exception {
        if(factory == null)
            factory=new org.jgroups.JChannelFactory();
        factory.create();

        if(isExposeChannels()) {
            ArrayList servers=MBeanServerFactory.findMBeanServer(null);
            if(servers == null || servers.size() == 0) {
                throw new Exception("No MBeanServer found; JChannelFactory needs to be run with an MBeanServer present, " +
                        "inside JDK 5, or with ExposeChannel set to false");
            }
            server=(MBeanServer)servers.get(0);
            String object_name=getObjectName();
            if(object_name == null)
                object_name="jgroups:name=Multiplexer";
            server.registerMBean(this, new ObjectName(object_name));
        }
    }

    public void start() throws Exception {
        factory.start();
    }

    public void stop() {
        factory.stop();
    }

    public void destroy() {
        factory.destroy();
        try {
            if(isExposeChannels() && server != null) {
                try {
                    JmxConfigurator.unregister(server, getObjectName());
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
        finally {
            factory=null;
        }
    }

    public String dumpConfiguration() {
        return factory.dumpConfiguration();
    }

    public String dumpChannels() {
        return factory.dumpChannels();
    }
}
