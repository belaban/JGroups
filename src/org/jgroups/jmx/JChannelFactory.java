package org.jgroups.jmx;

import org.jgroups.Channel;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactory.java,v 1.1 2006/04/18 15:26:50 belaban Exp $
 */
public class JChannelFactory implements JChannelFactoryMBean {
    org.jgroups.JChannelFactory factory=new org.jgroups.JChannelFactory();

    public void setMultiplexerConfig(String properties) throws Exception {
        factory.setMultiplexerConfig(properties);
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
