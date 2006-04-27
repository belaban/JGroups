package org.jgroups.jmx;

import org.jgroups.Channel;

import javax.management.MBeanServer;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactory.java,v 1.3 2006/04/27 08:44:13 belaban Exp $
 */
public class JChannelFactory implements JChannelFactoryMBean {
    org.jgroups.JChannelFactory factory=new org.jgroups.JChannelFactory();
    MBeanServer server=null;


    public JChannelFactory(org.jgroups.JChannelFactory factory) {
        this.factory=factory;
    }

    public JChannelFactory() {
    }

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
    }

    public void start() throws Exception {
        factory.start();
    }

    public void stop() {
        factory.stop();
    }

    public void destroy() {
        factory.destroy();
        factory=null;
    }

    public String dumpConfiguration() {
        return factory.dumpConfiguration();
    }

    public String dumpChannels() {
        return factory.dumpChannels();
    }
}
