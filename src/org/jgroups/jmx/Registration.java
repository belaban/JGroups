package org.jgroups.jmx;

import javax.management.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;

/**
 * Registers annotated object instances with with MBeanServer. 
 * 
 * @author Chris Mills
 * @version $Id: Registration.java,v 1.5 2008/03/10 09:01:19 vlada Exp $
 * 
 * @see ManagedAttribute
 * @see ManagedOperation
 * @see MBean
 * 
 */
public class Registration {

    private static final Log log=LogFactory.getLog(Registration.class);

    public static void register(Object obj, MBeanServer server) throws MBeanRegistrationException,
                                                               MalformedObjectNameException {
        internalRegister(obj, server, "");
    }

    public static void register(Object obj, MBeanServer server, String name) throws MBeanRegistrationException,
                                                                            MalformedObjectNameException {
        internalRegister(obj, server, name);
    }

    public static void unregister(Object obj, MBeanServer server) throws MBeanRegistrationException {
        internalUnregister(obj, server, "");
    }

    public static void unregister(Object obj, MBeanServer server, String name) throws MBeanRegistrationException,
                                                                              MalformedObjectNameException {
        internalUnregister(obj, server, name);
    }

    private static void internalRegister(Object obj, MBeanServer server, String name) throws MalformedObjectNameException,
                                                                                     MBeanRegistrationException {

        if(obj == null)
            throw new IllegalArgumentException("Object being registered cannot be null");
        if(server == null)
            throw new IllegalArgumentException("MBean server used for registeration cannot be null");

        /*if(!obj.getClass().isAnnotationPresent(MBean.class)) {
            throw new MBeanRegistrationException(null,
                                                 "Instance " + obj
                                                         + " of a class "
                                                         + obj.getClass()
                                                         + " is not annotated with the @MBean annotation");
        }*/

        try {
            ObjectName objName=getObjectName(obj, name);
            ResourceDMBean res=new ResourceDMBean(obj);
            server.registerMBean(res, objName);

            if(log.isDebugEnabled()) {
                log.debug("register MBean " + objName + " completed");
            }
        }
        catch(InstanceAlreadyExistsException e) {
            if(log.isErrorEnabled()) {
                log.error("register MBean failed " + e.getMessage());
            }
            throw new MBeanRegistrationException(e, "The @MBean objectName is not unique");
        }
        catch(NotCompliantMBeanException e) {
            if(log.isErrorEnabled()) {
                log.error("register MBean failed " + e.getMessage());
            }
            throw new MBeanRegistrationException(e);
        }        

    }

    private static void internalUnregister(Object obj, MBeanServer server, String name) throws MBeanRegistrationException {
        try {           
            if(name != null && name.length() > 0) {
                server.unregisterMBean(new ObjectName(name));
            }
            else if(obj != null) {
                server.unregisterMBean(getObjectName(obj, null));
            }
            else {
                throw new MBeanRegistrationException(null,
                                                     "Cannot find MBean name from @MBean or passed in value");
            }
            if(log.isDebugEnabled()) {
                log.debug("unregister MBean" + name + " completed");
            }
        }
        catch(InstanceNotFoundException infe) {
            if(log.isErrorEnabled()) {
                log.error("unregister MBean failed " + infe.getMessage());
            }
            throw new MBeanRegistrationException(infe);
        }
        catch(MalformedObjectNameException e) {
            if(log.isErrorEnabled()) {
                log.error("unregister MBean failed " + e.getMessage());
            }
            throw new MBeanRegistrationException(e);
        }
    }

    private static ObjectName getObjectName(Object obj, String name) throws MalformedObjectNameException {
        MBean resource=obj.getClass().getAnnotation(MBean.class);
        if(name != null && name.length() > 0) {
            return new ObjectName(name);
        }
        else if(resource.objectName() != null && resource.objectName().length() > 0) {
            return new ObjectName(resource.objectName());
        }
        else {
            throw new MalformedObjectNameException("Instance " + obj
                                                   + " of a class "
                                                   + obj.getClass()
                                                   + " does not have a valid object name");
        }
    }
}
