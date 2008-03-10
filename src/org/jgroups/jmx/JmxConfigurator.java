package org.jgroups.jmx;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.annotations.MBean;
import org.jgroups.jmx.Registration;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import javax.management.*;

import java.lang.management.ManagementFactory;
import java.util.Vector;
import java.util.Set;
import java.util.Iterator;

/**
 * @author Bela Ban
 * @version $Id: JmxConfigurator.java,v 1.11 2008/03/10 03:21:14 vlada Exp $
 */
public class JmxConfigurator {
    static final Log log=LogFactory.getLog(JmxConfigurator.class);

    /**
     * Registers an already created channel with the MBeanServer. Creates an
     * org.jgroups.jmx.JChannel which delegates to the org.jgroups.JChannel and
     * registers it. Optionally, this method will also try to create one MBean
     * proxy for each protocol in the channel's protocol stack, and register it
     * as well.
     * 
     * @param channel
     * @param server
     * @param domain
     *                Has to be a JMX ObjectName of the domain, e.g.
     *                DefaultDomain:name=JGroups
     * @param register_protocols
     * @return org.jgroups.jmx.JChannel for the specified org.jgroups.JChannel
     */
    public static void registerChannel(org.jgroups.JChannel channel,
                                       MBeanServer server,
                                       String domain,
                                       String cluster_name,
                                       boolean register_protocols) throws Exception {
        if(cluster_name == null)
            cluster_name=channel != null? channel.getClusterName() : null;
        if(cluster_name == null)
            cluster_name="null";

        if(register_protocols) {
            ProtocolStack stack=channel.getProtocolStack();
            Vector<Protocol> protocols=stack.getProtocols();
            for(Protocol p:protocols) {
                if(p.getClass().isAnnotationPresent(MBean.class)) {
                    Registration.register(p,
                                          ManagementFactory.getPlatformMBeanServer(),
                                          getProtocolRegistrationName(cluster_name, domain, p));
                }
            }
        }
        Registration.register(channel, server, getChannelRegistrationName(channel,
                                                                          domain,
                                                                          cluster_name));
    }
    /**
     * Registers an already created channel with the MBeanServer. Creates an org.jgroups.jmx.JChannel which
     * delegates to the org.jgroups.JChannel and registers it.
     * @param channel
     * @param server
     * @param name The JMX ObjectName 
     * @return org.jgroups.jmx.JChannel for the specified org.jgroups.JChannel
     */
    public static void registerChannel(org.jgroups.JChannel channel,
                                                           MBeanServer server, String name) throws Exception {
        registerChannel(channel,server,"jgroups",name,true);
    }
    
    
    
    public static void unregisterChannel(MBeanServer server, ObjectName name) throws Exception {
        if(server != null)
            server.unregisterMBean(name);
    }

    public static void unregisterChannel(MBeanServer server, String name) throws Exception {
        if(server != null)
            server.unregisterMBean(new ObjectName(name));
    }


    public static void unregisterChannel(org.jgroups.JChannel c,
                                         MBeanServer server,
                                         String clusterName) throws Exception {        
  
        
        ProtocolStack stack=c.getProtocolStack();
        Vector<Protocol> protocols=stack.getProtocols();
        for(Protocol p:protocols) {
            if(p.getClass().isAnnotationPresent(MBean.class)) {
                try {
                    Registration.unregister(p,
                                            ManagementFactory.getPlatformMBeanServer(),
                                            getProtocolRegistrationName(clusterName, "jgroups", p));
                }
                catch(MBeanRegistrationException e) {
                    if(log.isWarnEnabled()) {
                        log.warn("MBean unregistration failed " + e);
                    }
                }
            }
        }
        Registration.unregister(c, server, getChannelRegistrationName(clusterName));
    }

    public static void registerChannelFactory(org.jgroups.JChannelFactory factory,
                                              MBeanServer server,
                                              String name) throws Exception {
        Registration.register(factory, server, name);
    }

    public static void unRegisterChannelFactory(org.jgroups.JChannelFactory factory,
                                                MBeanServer server,
                                                String name) throws Exception {
        Registration.unregister(factory, server, name);
    }
    
    public static void registerProtocols(MBeanServer server, JChannel channel, String objectName) {
        //TODO remove once parallel jmx hierarchy is removed       
    }

    public static void unregisterProtocols(MBeanServer server, JChannel channel, String objectName) {
        //TODO remove once parallel jmx hierarchy is removed       
    }

    /**
     * Unregisters object_name and everything under it
     * 
     * @param object_name
     */
    public static void unregister(MBeanServer server, String object_name) throws Exception {
        Set mbeans=server.queryNames(new ObjectName(object_name), null);
        if(mbeans != null) {
            ObjectName name;
            for(Iterator it=mbeans.iterator();it.hasNext();) {
                name=(ObjectName)it.next();
                server.unregisterMBean(name);
            }
        }
    }

    private static String getChannelRegistrationName(org.jgroups.JChannel c,
                                                     String domain,
                                                     String clusterName) {
        return domain + ":type=channel,cluster=" + clusterName;
    }

    private static String getProtocolRegistrationName(String clusterName, String domain, Protocol p) {
        return domain + ":type=protocol,cluster=" + clusterName + ",protocol=" + p.getName();
    }

    private static String getChannelRegistrationName(String clusterName) {
        return "jgroups:type=channel,cluster=" + clusterName;
    }    
}
