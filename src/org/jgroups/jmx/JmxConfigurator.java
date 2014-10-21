package org.jgroups.jmx;

import org.jgroups.JChannel;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import javax.management.*;
import java.util.List;
import java.util.Set;

/**
 * @author Bela Ban, Vladimir Blagojevic
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

        if(channel == null)
            throw new NullPointerException("channel cannot be null");
        if (cluster_name == null)
            cluster_name=channel.getClusterName();
        if (cluster_name == null)
            cluster_name = "null";

        cluster_name=ObjectName.quote(cluster_name);

        if (register_protocols) {
            ProtocolStack stack = channel.getProtocolStack();
            List<Protocol> protocols = stack.getProtocols();
            for (Protocol p : protocols) {
                if (p.getClass().isAnnotationPresent(MBean.class)) {
                    String jmx_name=getProtocolRegistrationName(cluster_name,domain,p);
                    register(p, server, jmx_name);
                }
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
     * @param name
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

    public static void unregisterChannel(JChannel c, MBeanServer server, String clusterName) throws Exception {
        unregisterChannel(c, server, "jgroups", clusterName);
    }


    public static void unregisterChannel(JChannel c, MBeanServer server, String domain, String clusterName)
                    throws Exception {

        if(clusterName != null)
            clusterName=ObjectName.quote(clusterName);

        ProtocolStack stack = c.getProtocolStack();
        List<Protocol> protocols = stack.getProtocols();
        for (Protocol p : protocols) {
            if (p.getClass().isAnnotationPresent(MBean.class)) {
                try {
                    String obj_name=getProtocolRegistrationName(clusterName, domain, p);
                    unregister(p, server, obj_name);
                } catch (MBeanRegistrationException e) {
                    log.warn("MBean unregistration failed: " + e.getCause());
                }
            }
        }
        unregister(c, server, getChannelRegistrationName(c, domain, clusterName));
    }


    public static void register(Object obj, MBeanServer server, String name)
                    throws MBeanRegistrationException, MalformedObjectNameException {
        internalRegister(obj, server, name);
    }

    public static void unregister(Object obj, MBeanServer server, String name)
                    throws MBeanRegistrationException, MalformedObjectNameException {
        internalUnregister(obj, server, name);
    }


    
    /**
     * Wrap JChannel with DynamicMBean interface. All annotated attributes and methods will be
     * exposed through DynamicMBean API.
     * 
     * @see ManagedAttribute
     * @see ManagedOperation
     * 
     * @param ch channel to be wrapped
     * @return Channel ch wrapped as a DynamicBean 
     */
    public static DynamicMBean wrap(JChannel ch) {
        return new ResourceDMBean(ch);
    }

    /**
     * Wrap Protocol with DynamicMBean interface. All annotated attributes and methods will be
     * exposed through DynamicMBean API.
     * 
     * @see ManagedAttribute
     * @see ManagedOperation
     * 
     * @param p protocol to be wrapped
     * @return Protocol p as a DynamicMBean
     */
    public static DynamicMBean wrap(Protocol p) {
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
            if(server.isRegistered(objName)) {
                log.warn("unregistering already registered MBean: " + objName);
                try {
                    server.unregisterMBean(objName);
                }
                catch(InstanceNotFoundException e) {
                    log.error("failed to unregister MBean " + e.getMessage());
                }
            }

            ResourceDMBean res=new ResourceDMBean(obj);
            server.registerMBean(res, objName);
        }
        catch (InstanceAlreadyExistsException e) {
            throw new MBeanRegistrationException(e, "The @MBean objectName is not unique");
        }
        catch (NotCompliantMBeanException e) {
            throw new MBeanRegistrationException(e);
        }
    }

    private static void internalUnregister(Object obj, MBeanServer server, String name) throws MBeanRegistrationException {
        try {
            ObjectName obj_name=null;
            if(name != null && !name.isEmpty())
                obj_name=new ObjectName(name);
            else if(obj != null)
                obj_name=getObjectName(obj, null);
            else
                throw new MBeanRegistrationException(null, "Cannot find MBean name from @MBean or passed in value");
            if(server.isRegistered(obj_name))
                server.unregisterMBean(obj_name);
        }
        catch (InstanceNotFoundException infe) {
            throw new MBeanRegistrationException(infe);
        }
        catch (MalformedObjectNameException e) {
            throw new MBeanRegistrationException(e);
        }
    }

    private static ObjectName getObjectName(Object obj, String name) throws MalformedObjectNameException {
        MBean resource = obj.getClass().getAnnotation(MBean.class);
        if (name != null && !name.isEmpty()) {
            return new ObjectName(name);
        } else if (resource.objectName() != null && !resource.objectName().isEmpty()) {
            return new ObjectName(resource.objectName());
        } else {
            throw new MalformedObjectNameException(obj + " of class " + obj.getClass() + " has an invalid object name");
        }
    }

  
    /**
     * Unregisters object_name and everything under it
     * @param object_name
     */
    public static void unregister(MBeanServer server, String object_name) throws Exception {
        Set<ObjectName> mbeans = server.queryNames(new ObjectName(object_name), null);
        if(mbeans != null)
            for (ObjectName name: mbeans)
                server.unregisterMBean(name);
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
