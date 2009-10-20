package org.jgroups.jmx;

import org.jgroups.JChannel;
import org.jgroups.JChannelFactory;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.annotations.MBean;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import javax.management.*;

import java.util.Vector;
import java.util.Set;
import java.util.Iterator;

/**
 * @author Bela Ban, Vladimir Blagojevic
 * @version $Id: JmxConfigurator.java,v 1.17 2009/10/20 13:05:20 belaban Exp $
 */
public class JmxConfigurator {
    static final Log log = LogFactory.getLog(JmxConfigurator.class);

    /**
     * Registers an already created channel with the given MBeanServer. Wraps instance of JChannel
     * with DynamicMBean and delegates all calls to the actual JChannel wrapped.
     * <p>
     * Optionally, this method will also wrap each protocol in the given channel with DynamicMBean
     * and register it as well.
     * 
     * @param channel
     * @param server
     * @param domain
     *            Has to be a JMX ObjectName of the domain, e.g. DefaultDomain:name=JGroups
     * @param register_protocols
     */
    public static void registerChannel(JChannel channel, MBeanServer server, String domain,
                    String cluster_name, boolean register_protocols) throws Exception {
        if (cluster_name == null)
            cluster_name = channel != null ? channel.getClusterName() : null;
        if (cluster_name == null)
            cluster_name = "null";

        if (register_protocols) {
            ProtocolStack stack = channel.getProtocolStack();
            Vector<Protocol> protocols = stack.getProtocols();
            for (Protocol p : protocols) {
                register(p, server, getProtocolRegistrationName(cluster_name, domain, p));
            }
        }
        register(channel, server, getChannelRegistrationName(channel, domain, cluster_name));
    }

    /**
     * Registers an already created channel with the given MBeanServer. Wraps instance of JChannel
     * with DynamicMBean and delegates all calls to the actual JChannel wrapped.
     * <p>
     * This method will also wrap each protocol in the given channel with DynamicMBean and register
     * it as well.
     * 
     * @param channel
     * @param server
     * @param domain
     *            Has to be a JMX ObjectName of the domain, e.g. DefaultDomain:name=JGroups
     */
    public static void registerChannel(JChannel channel, MBeanServer server, String name)
                    throws Exception {
        registerChannel(channel, server, "jgroups", name, true);
    }

    public static void unregisterChannel(MBeanServer server, ObjectName name) throws Exception {
        if (server != null)
            server.unregisterMBean(name);
    }

    public static void unregisterChannel(MBeanServer server, String name) throws Exception {
        if (server != null)
            server.unregisterMBean(new ObjectName(name));
    }

    public static void unregisterChannel(JChannel c, MBeanServer server, String clusterName)
                    throws Exception {

        ProtocolStack stack = c.getProtocolStack();
        Vector<Protocol> protocols = stack.getProtocols();
        for (Protocol p : protocols) {
            if (p.getClass().isAnnotationPresent(MBean.class)) {
                try {
                    unregister(p, server, getProtocolRegistrationName(clusterName, "jgroups", p));
                } catch (MBeanRegistrationException e) {
                    if (log.isWarnEnabled()) {
                        log.warn("MBean unregistration failed " + e);
                    }
                }
            }
        }
        unregister(c, server, getChannelRegistrationName(clusterName));
    }

    public static void registerChannelFactory(JChannelFactory factory, MBeanServer server,
                    String name) throws Exception {
        register(factory, server, name);
    }

    public static void unRegisterChannelFactory(JChannelFactory factory, MBeanServer server,
                    String name) throws Exception {
        unregister(factory, server, name);
    }

    public static void register(Object obj, MBeanServer server, String name)
                    throws MBeanRegistrationException, MalformedObjectNameException {
        internalRegister(obj, server, name);
    }

    public static void unregister(Object obj, MBeanServer server, String name)
                    throws MBeanRegistrationException, MalformedObjectNameException {
        internalUnregister(obj, server, name);
    }

    public DynamicMBean asDynamicMBean(JChannel ch) {
        return new ResourceDMBean(ch);
    }

    public DynamicMBean asDynamicMBean(Protocol p) {
        return new ResourceDMBean(p);
    }

    private static void internalRegister(Object obj, MBeanServer server, String name)
                    throws MalformedObjectNameException, MBeanRegistrationException {

        if (obj == null)
            throw new IllegalArgumentException("Object being registered cannot be null");
        if (server == null)
            throw new IllegalArgumentException("MBean server used for registeration cannot be null");

        try {
            ObjectName objName = getObjectName(obj, name);
            ResourceDMBean res = new ResourceDMBean(obj);
            server.registerMBean(res, objName);
        } catch (InstanceAlreadyExistsException e) {
            if (log.isErrorEnabled()) {
                log.error("register MBean failed " + e.getMessage());
            }
            throw new MBeanRegistrationException(e, "The @MBean objectName is not unique");
        } catch (NotCompliantMBeanException e) {
            if (log.isErrorEnabled()) {
                log.error("register MBean failed " + e.getMessage());
            }
            throw new MBeanRegistrationException(e);
        }

    }

    private static void internalUnregister(Object obj, MBeanServer server, String name)
                    throws MBeanRegistrationException {
        try {
            if (name != null && name.length() > 0) {
                server.unregisterMBean(new ObjectName(name));
            } else if (obj != null) {
                server.unregisterMBean(getObjectName(obj, null));
            } else {
                throw new MBeanRegistrationException(null,
                                "Cannot find MBean name from @MBean or passed in value");
            }
        } catch (InstanceNotFoundException infe) {
            if (log.isErrorEnabled()) {
                log.error("unregister MBean failed " + infe.getMessage());
            }
            throw new MBeanRegistrationException(infe);
        } catch (MalformedObjectNameException e) {
            if (log.isErrorEnabled()) {
                log.error("unregister MBean failed " + e.getMessage());
            }
            throw new MBeanRegistrationException(e);
        }
    }

    private static ObjectName getObjectName(Object obj, String name)
                    throws MalformedObjectNameException {
        MBean resource = obj.getClass().getAnnotation(MBean.class);
        if (name != null && name.length() > 0) {
            return new ObjectName(name);
        } else if (resource.objectName() != null && resource.objectName().length() > 0) {
            return new ObjectName(resource.objectName());
        } else {
            throw new MalformedObjectNameException("Instance " + obj + " of a class "
                            + obj.getClass() + " does not have a valid object name");
        }
    }

    /**
     * Unregisters object_name and everything under it
     * 
     * @param object_name
     */
    public static void unregister(MBeanServer server, String object_name) throws Exception {
        Set<ObjectName> mbeans = server.queryNames(new ObjectName(object_name), null);
        if (mbeans != null) {
            for (Iterator<ObjectName> it = mbeans.iterator(); it.hasNext();) {
                server.unregisterMBean(it.next());
            }
        }
    }

    private static String getChannelRegistrationName(JChannel c, String domain, String clusterName) {
        return domain + ":type=channel,cluster=" + clusterName;
    }

    private static String getProtocolRegistrationName(String clusterName, String domain, Protocol p) {
        return domain + ":type=protocol,cluster=" + clusterName + ",protocol=" + p.getName();
    }

    private static String getChannelRegistrationName(String clusterName) {
        return "jgroups:type=channel,cluster=" + clusterName;
    }
}
