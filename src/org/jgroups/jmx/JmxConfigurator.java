package org.jgroups.jmx;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import javax.management.*;
import java.util.Vector;

/**
 * @author Bela Ban
 * @version $Id: JmxConfigurator.java,v 1.5 2005/08/17 07:32:31 belaban Exp $
 */
public class JmxConfigurator {
    static final Log log=LogFactory.getLog(JmxConfigurator.class);

    /**
     * Registers an already created channel with the MBeanServer. Creates an org.jgroups.jmx.JChannel which
     * delegates to the org.jgroups.JChannel and registers it. Optionally, this method will also try to
     * create one MBean proxy for each protocol in the channel's protocol stack, and register it as well.
     * @param channel
     * @param server
     * @param name Has to be a JMX ObjectName, e.g. DefaultDomain:type=MyChannel
     * @param register_protocols
     * @return org.jgroups.jmx.JChannel for the specified org.jgroups.JChannel
     */
    public static org.jgroups.jmx.JChannel registerChannel(org.jgroups.JChannel channel,
                                                           MBeanServer server, String name,
                                                           boolean register_protocols) throws Exception {
        JChannel retval=new JChannel(channel);
        server.registerMBean(retval, new ObjectName(name));
        if(register_protocols)
            registerProtocols(server, channel, name);
        return retval;
    }

    public static void unregisterChannel(MBeanServer server, ObjectName name) throws Exception {
        if(server != null)
            server.unregisterMBean(name);
    }

    /**
     * Takes all protocols of an existing stack, creates corresponding MBean proxies and registers them with
     * the MBean server
     * @param channel
     * @param channel_name
     */
    public static void registerProtocols(MBeanServer server, org.jgroups.JChannel channel, String channel_name) throws Exception {
        ProtocolStack stack=channel.getProtocolStack();
        Vector protocols=stack.getProtocols();
        org.jgroups.stack.Protocol prot;
        org.jgroups.jmx.Protocol p=null;
        for(int i=0; i < protocols.size(); i++) {
            prot=(org.jgroups.stack.Protocol)protocols.get(i);
            try {
                p=findProtocol(prot);
            }
            catch(ClassNotFoundException e) {
                p=null;
            }
            catch(Throwable e) {
                log.error("failed creating a JMX wrapper instance for " + prot, e);
                p=null;
            }
            if(p == null)
                p=new org.jgroups.jmx.Protocol(prot);
            ObjectName prot_name=new ObjectName(channel_name + ",protocol=" + prot.getName());
            server.registerMBean(p, prot_name);
        }
    }

    public static void unregisterProtocols(MBeanServer server, org.jgroups.JChannel channel, String channel_name) {
        ProtocolStack stack=channel.getProtocolStack();
        Vector protocols=stack.getProtocols();
        org.jgroups.stack.Protocol prot;
        ObjectName prot_name=null;
        for(int i=0; i < protocols.size(); i++) {
            prot=(org.jgroups.stack.Protocol)protocols.get(i);
            try {
                prot_name=new ObjectName(channel_name + ",protocol=" + prot.getName());
                server.unregisterMBean(prot_name);
            }
            catch(Throwable e) {
                log.error("failed to unregister " + prot_name, e);
            }
        }
    }


    protected static Protocol findProtocol(org.jgroups.stack.Protocol prot) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Protocol p;
        String prot_name=prot.getClass().getName();
        String clname=prot_name.replaceFirst("org.jgroups.", "org.jgroups.jmx.");
        Class cl=Util.loadClass(clname, JmxConfigurator.class);
        if(cl != null) {
            p=(Protocol)cl.newInstance();
            p.attachProtocol(prot);
            return p;
        }
        return null;
    }
}
