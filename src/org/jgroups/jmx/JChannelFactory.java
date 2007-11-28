package org.jgroups.jmx;

import org.jgroups.Channel;

import javax.management.MBeanRegistration;
import javax.management.ObjectName;
import javax.management.MBeanServer;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactory.java,v 1.7.2.1 2007/11/28 11:39:58 belaban Exp $
 */
public class JChannelFactory implements JChannelFactoryMBean, MBeanRegistration {
    org.jgroups.JChannelFactory factory=new org.jgroups.JChannelFactory();
    private MBeanServer server=null;


    public JChannelFactory(org.jgroups.JChannelFactory factory) {
        this.factory=factory;
    }

    public JChannelFactory() {
    }

    public void setMultiplexerConfig(String properties) throws Exception {
        factory.setMultiplexerConfig(properties);
    }


    public void setMultiplexerConfig(String properties, boolean replace) throws Exception {
        factory.setMultiplexerConfig(properties, replace);
    }

    public String getMultiplexerConfig() {
        return factory.getMultiplexerConfig();
    }

    public String getConfig(String stack_name) throws Exception {
        return factory.getConfig(stack_name);
    }

    public boolean removeConfig(String stack_name) {
        return factory.removeConfig(stack_name);
    }

    public void clearConfigurations() {
        factory.clearConfigurations();
    }

    public String getDomain() {
        return factory.getDomain();
    }

    public void setDomain(String name) {
        factory.setDomain(name);
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
        factory.setServer(server);
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

    public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
        this.server=server;
        if(factory != null)
            factory.setServer(server);
        if (name != null) {//we only create a name if it is empty
            return name;
        }
        //if the objectName is not a pattern then at least one key property MUST exist
        String objectName = getDomain() + " : service=JChannelFactory";
        return ObjectName.getInstance(objectName);
    }

    public void postRegister(Boolean registrationDone) {
    }

    public void preDeregister() throws Exception {
    }

    public void postDeregister() {
    }
}
