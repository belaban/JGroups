package org.jgroups.jmx;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;

import javax.management.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Vector;

/**
 * 
 * A DynamicMBean wrapping an annotated object instance.  
 * 
 * @author Chris Mills
 * @author Vladimir Blagojevic
 * @version $Id: ResourceDMBean.java,v 1.4 2008/03/06 01:28:13 vlada Exp $
 * @see ManagedAttribute
 * @see ManagedOperation
 * @see MBean
 * 
 */
public class ResourceDMBean implements DynamicMBean {
    private static final Class<?>[] primitives= { int.class,
                                              byte.class,
                                              short.class,
                                              long.class,
                                              float.class,
                                              double.class,
                                              boolean.class,
                                              char.class };
        
    private static final String MBEAN_DESCRITION="Dynamic MBean Description";
   
    private final Log log=LogFactory.getLog(ResourceDMBean.class);
    private final Object obj;
    private String description = "";

    public ResourceDMBean(Object obj) {
        if(obj == null) throw new NullPointerException("Cannot make an MBean wrapper for null instance");
        this.obj=obj;       
    }

    public synchronized MBeanInfo getMBeanInfo() { 
        
        Vector<MBeanAttributeInfo> vctAttributes=new Vector<MBeanAttributeInfo>();
        Vector<MBeanOperationInfo> vctOperations=new Vector<MBeanOperationInfo>();

        //set a field to hold the description of the MBean
        MBean mbean=obj.getClass().getAnnotation(MBean.class);
        if(mbean.description() != null && mbean.description().trim().length() > 0) {
            if(log.isDebugEnabled()) {
                log.debug("@MBean description set - " + mbean.description());
            }
            this.description=mbean.description();
            vctAttributes.add(new MBeanAttributeInfo(ResourceDMBean.MBEAN_DESCRITION,
                                                     "java.lang.String",
                                                     "@MBean description",
                                                     true,
                                                     false,
                                                     false));
        }           
        
        //walk annotated class hierarchy and find all fields
        for(Class<?> clazz=obj.getClass();
            clazz != null && clazz.isAnnotationPresent(MBean.class);
            clazz=clazz.getSuperclass()) {
            
            Field[] fields=clazz.getDeclaredFields();
            for(Field field:fields) {
                ManagedAttribute attr=field.getAnnotation(ManagedAttribute.class);
                if(attr != null) {
                    if(Modifier.isFinal(field.getModifiers())) {
                        //field is declared final - no changes allowed
                        if(log.isWarnEnabled() && attr.writable()) {
                            log.warn(field.getName() + " declared final, so cannot be marked as writable - writable is ignored");
                        }
                        vctAttributes.add(new MBeanAttributeInfo(field.getName(),
                                                                 field.getType().getCanonicalName(),
                                                                 attr.description(),
                                                                 attr.readable(),
                                                                 false,
                                                                 false));
                    }
                    else {
                        vctAttributes.add(new MBeanAttributeInfo(field.getName(),
                                                                 field.getType().getCanonicalName(),
                                                                 attr.description(),
                                                                 attr.readable(),
                                                                 attr.writable(),
                                                                 false));
                    }
                    if(log.isInfoEnabled()) {
                        log.info("@Attr found for field " + field.getName());
                    }
                }
            }
        }

        //find all methods
        Method[] methods=obj.getClass().getMethods();
        for(Method method:methods) {
            ManagedAttribute attr=method.getAnnotation(ManagedAttribute.class);
            if(attr != null) {
                if(method.getParameterTypes().length == 0 && method.getReturnType() != java.lang.Void.TYPE) {
                    vctAttributes.add(new MBeanAttributeInfo(method.getName() + "()",
                                                             method.getReturnType().getCanonicalName(),
                                                             attr.description(),
                                                             attr.readable(),
                                                             false,
                                                             false));
                    if(log.isInfoEnabled()) {
                        log.info("@Attr found for method " + method.getName());
                    }
                }
                else {
                    if(log.isWarnEnabled()) {
                        log.warn("Method " + method.getName()
                                 + " must have a valid return type and zero parameters");
                    }
                }
            }
            ManagedOperation op=method.getAnnotation(ManagedOperation.class);
            if(op != null) {
                vctOperations.add(new MBeanOperationInfo(op.description(), method));
                if(log.isInfoEnabled()) {
                    log.info("@Operation found for method " + method.getName());
                }
            }
        }

        MBeanAttributeInfo[] attrInfo=new MBeanAttributeInfo[vctAttributes.size()];
        vctAttributes.toArray(attrInfo);

        MBeanOperationInfo[] opInfo=new MBeanOperationInfo[vctOperations.size()];
        vctOperations.toArray(opInfo);

        return new MBeanInfo(obj.getClass().getCanonicalName(),
                             this.description,
                             attrInfo,
                             null,
                             opInfo,
                             null);     
    }

    public synchronized Object getAttribute(String name) {
        if(log.isDebugEnabled()) {
            log.debug("getAttribute called for " + name);
        }
        Attribute attr=getNamedAttribute(name);
        if(log.isDebugEnabled()) {
            log.debug("getAttribute value found " + attr.getValue());
        }
        return attr.getValue();
    }

    public synchronized void setAttribute(Attribute attribute) {
        if(log.isDebugEnabled()) {
            log.debug("setAttribute called for " + attribute.getName()
                      + " value "
                      + attribute.getValue());
        }
        setNamedAttribute(attribute);
    }

    public synchronized AttributeList getAttributes(String[] names) {
        if(log.isDebugEnabled()) {
            log.debug("getAttributes called");
            for(String name:names) {
                log.debug("Attribute name " + name);
            }
        }
        AttributeList al=new AttributeList();

        for(String name:names) {
            Attribute attr=getNamedAttribute(name);
            if(attr != null) {
                if(log.isDebugEnabled()) {
                    log.debug("Attribute " + name + " found with value " + attr.getValue());
                }
                al.add(attr);
            }
        }

        return al;
    }

    public synchronized AttributeList setAttributes(AttributeList list) {
        if(log.isDebugEnabled()) {
            log.debug("setAttributes called");
        }
        AttributeList results=new AttributeList();
        for(int i=0;i < list.size();i++) {
            Attribute attr=(Attribute)list.get(i);

            if(log.isDebugEnabled()) {
                log.debug("Attribute name " + attr.getName() + " new value is " + attr.getValue());
            }

            if(setNamedAttribute(attr)) {
                results.add(attr);
            }
            else {
                if(log.isWarnEnabled()) {
                    log.debug("Failed to update attribute name " + attr.getName()
                              + " with value "
                              + attr.getValue());
                }
            }
        }
        return results;
    }

    public Object invoke(String name, Object[] args, String[] sig) throws MBeanException,
                                                                  ReflectionException {
        try {
            if(log.isDebugEnabled()) {
                log.debug("Invoke method called on " + name);
            }
            Class<?>[] classes=new Class[sig.length];
            for(int i=0;i < classes.length;i++) {
                classes[i]=getClassForName(sig[i]);
            }
            Method method=this.obj.getClass().getMethod(name, classes);
            return method.invoke(this.obj, args);
        }
        catch(Exception e) {
            throw new MBeanException(e);
        }
    }

    public static Class<?> getClassForName(String name) throws ClassNotFoundException {
        try {
            Class<?> c=Class.forName(name);
            return c;
        }
        catch(ClassNotFoundException cnfe) {
            //Could be a primative - let's check
            for(int i=0;i < primitives.length;i++) {
                if(name.equals(primitives[i].getName())) {
                    return primitives[i];
                }
            }
        }
        throw new ClassNotFoundException("Class " + name + " cannot be found");
    }

    private Attribute getNamedAttribute(String name) {
        try {
            if(name.endsWith("()")) {
                Method method=this.obj.getClass().getMethod(name.substring(0, name.length() - 2),new Class[] {});
                return new Attribute(name, method.invoke(this.obj, new Object[] {}));
            }
            else {
                if(name.equals(ResourceDMBean.MBEAN_DESCRITION)) {
                    return new Attribute(ResourceDMBean.MBEAN_DESCRITION, this.description);
                }
                else {
                    Field field=getFieldInHierarchy(obj.getClass(), name);
                    if(field != null) {
                        if(!field.isAccessible()) {
                            field.setAccessible(true);
                        }
                        return new Attribute(name, field.get(this.obj));
                    }
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean setNamedAttribute(Attribute attribute) {
        try {
            Field field=getFieldInHierarchy(obj.getClass(), attribute.getName());
            if(field != null) {
                if(!field.isAccessible()) {
                    field.setAccessible(true);
                }
                field.set(this.obj, attribute.getValue());
                return true;
            }
            return false;
        }
        catch(Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private Field getFieldInHierarchy(Class<?> clazz, String name) {
        try {
            return clazz.getDeclaredField(name);
        }
        catch(SecurityException e) {
            return null;
        }
        catch(NoSuchFieldException e) {
            Class<?> superClazz=clazz.getSuperclass();
            if(superClazz != null && superClazz.isAnnotationPresent(MBean.class)) {
                return getFieldInHierarchy(superClazz, name);
            }
            else {
                return null;
            }
        }
    }
}
