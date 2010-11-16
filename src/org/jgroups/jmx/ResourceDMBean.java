package org.jgroups.jmx;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.util.Util;

/**
 * 
 * A DynamicMBean wrapping an annotated object instance.
 * 
 * @author Chris Mills
 * @author Vladimir Blagojevic
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
    private String description="";

    private final MBeanAttributeInfo[] attrInfo;
    private final MBeanOperationInfo[] opInfo;

    private final HashMap<String,AttributeEntry> atts=new HashMap<String,AttributeEntry>();
    private final List<MBeanOperationInfo> ops=new ArrayList<MBeanOperationInfo>();

    public ResourceDMBean(Object instance) {
        if(instance == null)
            throw new NullPointerException("Cannot make an MBean wrapper for null instance");
        
        this.obj=instance;
        findDescription();
        findFields();
        findMethods();

        attrInfo=new MBeanAttributeInfo[atts.size()];
        int i=0;
        MBeanAttributeInfo info=null;
        for(AttributeEntry entry:atts.values()) {
            info=entry.getInfo();
            attrInfo[i++]=info;
        }

        opInfo=new MBeanOperationInfo[ops.size()];
        ops.toArray(opInfo);
    }

    Object getObject() {
        return obj;
    }

    private void findDescription() {
        MBean mbean=getObject().getClass().getAnnotation(MBean.class);
        if(mbean != null && mbean.description() != null && mbean.description().trim().length() > 0) {
            description=mbean.description();
            MBeanAttributeInfo info=new MBeanAttributeInfo(ResourceDMBean.MBEAN_DESCRITION,
                                                           "java.lang.String",
                                                           "MBean description",
                                                           true,
                                                           false,
                                                           false);
            try {
                atts.put(ResourceDMBean.MBEAN_DESCRITION,
                         new FieldAttributeEntry(info,getClass().getDeclaredField("description")));
            }
            catch(NoSuchFieldException e) {
               //this should not happen unless somebody removes description field 
               log.warn("Could not reflect field description of this class. Was it removed?");               
            }
        }
    }

    public synchronized MBeanInfo getMBeanInfo() {

        return new MBeanInfo(getObject().getClass().getCanonicalName(),
                             description,
                             attrInfo,
                             null,
                             opInfo,
                             null);
    }

    public synchronized Object getAttribute(String name) {
        if(name == null || name.length() == 0)
            throw new NullPointerException("Invalid attribute requested " + name);
        Attribute attr=getNamedAttribute(name);
        return attr.getValue();
    }

    public synchronized void setAttribute(Attribute attribute) {
        if(attribute == null || attribute.getName() == null)
            throw new NullPointerException("Invalid attribute requested " + attribute);
                
        setNamedAttribute(attribute);
    }

    public synchronized AttributeList getAttributes(String[] names) {       
        AttributeList al=new AttributeList();
        for(String name:names) {
            Attribute attr=getNamedAttribute(name);
            if(attr != null) {
                al.add(attr);
            }
            else {
                log.warn("Did not find attribute " + name);
            }
        }
        return al;
    }

    public synchronized AttributeList setAttributes(AttributeList list) {        
        AttributeList results=new AttributeList();
        for(int i=0;i < list.size();i++) {
            Attribute attr=(Attribute)list.get(i);

            if(setNamedAttribute(attr)) {
                results.add(attr);
            }
            else {
                if(log.isWarnEnabled()) {
                    log.warn("Failed to update attribute name " + attr.getName() + " with value " + attr.getValue());
                }
            }
        }
        return results;
    }

    public Object invoke(String name, Object[] args, String[] sig) throws MBeanException,
                                                                  ReflectionException {
        try {
            Class<?>[] classes=new Class[sig.length];
            for(int i=0;i < classes.length;i++) {
                classes[i]=getClassForName(sig[i]);
            }
            Method method=getObject().getClass().getMethod(name, classes);
            return method.invoke(getObject(), args);
        }
        catch(Exception e) {
            throw new MBeanException(e);
        }
    }

    public static Class<?> getClassForName(String name) throws ClassNotFoundException {
        try {
            return Class.forName(name);
        }
        catch(ClassNotFoundException cnfe) {
            //Could be a primitive - let's check
            for(int i=0;i < primitives.length;i++) {
                if(name.equals(primitives[i].getName())) {
                    return primitives[i];
                }
            }
        }
        throw new ClassNotFoundException("Class " + name + " cannot be found");
    }

    private void findMethods() {
        //find all methods but don't include methods from Object class
        List<Method> methods = new ArrayList<Method>(Arrays.asList(getObject().getClass().getMethods()));
        List<Method> objectMethods = new ArrayList<Method>(Arrays.asList(Object.class.getMethods()));
        methods.removeAll(objectMethods);

        for(Method method:methods) {
            //does method have @ManagedAttribute annotation?
            if(method.isAnnotationPresent(ManagedAttribute.class) || method.isAnnotationPresent(Property.class)) {
                exposeManagedAttribute(method);
            }
            //or @ManagedOperation
            else if (method.isAnnotationPresent(ManagedOperation.class) || isMBeanAnnotationPresentWithExposeAll()){
                exposeManagedOperation(method);
            }
        }
    }

    /** find all methods but don't include methods from Object class */
    /*private void findMethods() {
        for(Class<?> clazz=getObject().getClass();clazz != null; clazz=clazz.getSuperclass()) {
            if(clazz.equals(Object.class))
                break;

            Method[] methods=clazz.getDeclaredMethods();
            for(Method method: methods) {
                //does method have @ManagedAttribute annotation?
                if(method.isAnnotationPresent(ManagedAttribute.class) || method.isAnnotationPresent(Property.class)) {
                    exposeManagedAttribute(method);
                }
                //or @ManagedOperation
                else if (method.isAnnotationPresent(ManagedOperation.class) || isMBeanAnnotationPresentWithExposeAll()){
                    exposeManagedOperation(method);
                }
            }
        }
    }*/

    private void exposeManagedOperation(Method method) {
        ManagedOperation op=method.getAnnotation(ManagedOperation.class);                
        String attName=method.getName();
        if(isSetMethod(method) || isGetMethod(method)) {
            attName=attName.substring(3);
        }
        else if(isIsMethod(method)) {
            attName=attName.substring(2);
        }
        //expose unless we already exposed matching attribute field
        boolean isAlreadyExposed=atts.containsKey(attName);
        if(!isAlreadyExposed) {
            ops.add(new MBeanOperationInfo(op != null? op.description() : "", method));
        }
    }
    
    private void exposeManagedAttribute(Method method){
        String methodName=method.getName();
        if(!methodName.startsWith("get") && !methodName.startsWith("set") && !methodName.startsWith("is")) {
            if(log.isWarnEnabled())
                log.warn("method name " + methodName + " doesn't start with \"get\", \"set\", or \"is\""
                        + ", but is annotated with @ManagedAttribute: will be ignored");
            return;
        }
        ManagedAttribute attr = method.getAnnotation(ManagedAttribute.class);
        Property prop=method.getAnnotation(Property.class);

        boolean expose_prop=prop != null && prop.exposeAsManagedAttribute();
        boolean expose=attr != null || expose_prop;
        if(!expose)
            return;

        // Is name field of @ManagedAttributed used?
        String attributeName=attr != null? attr.name() : null;
        if(attributeName != null && attributeName.trim().length() > 0)
            attributeName=attributeName.trim();
        else
            attributeName=null;
            
        String descr=attr != null ? attr.description() : prop != null? prop.description() : null;

        boolean writeAttribute=false;
        MBeanAttributeInfo info=null;
        if(isSetMethod(method)) { // setter
            attributeName=(attributeName==null)?methodName.substring(3):attributeName;
            info=new MBeanAttributeInfo(attributeName,
                                        method.getParameterTypes()[0].getCanonicalName(),
                                        descr,
                                        true,
                                        true,
                                        false);
            writeAttribute=true;
        }
        else { // getter
            if(method.getParameterTypes().length == 0 && method.getReturnType() != java.lang.Void.TYPE) {
                boolean hasSetter=atts.containsKey(attributeName);
                //we found is method
                if(methodName.startsWith("is")) {
                    attributeName=(attributeName==null)?methodName.substring(2):attributeName;
                    info=new MBeanAttributeInfo(attributeName,
                                                method.getReturnType().getCanonicalName(),
                                                descr,
                                                true,
                                                hasSetter,
                                                true);
                }
                else {
                    // this has to be get
                    attributeName=(attributeName==null)?methodName.substring(3):attributeName;
                    info=new MBeanAttributeInfo(attributeName,
                                                method.getReturnType().getCanonicalName(),
                                                descr,
                                                true,
                                                hasSetter,
                                                false);
                }
            }
            else {
                if(log.isWarnEnabled()) {
                    log.warn("Method " + method.getName() + " must have a valid return type and zero parameters");
                }
                //silently skip this method
                return;
            }
        }
            
        AttributeEntry ae=atts.get(attributeName);
        //is it a read method?
        if(!writeAttribute) {
            //we already have annotated field as read
            if(ae instanceof FieldAttributeEntry && ae.getInfo().isReadable()) {
                log.warn("not adding annotated method " + method + " since we already have read attribute");
            }
            //we already have annotated set method
            else if(ae instanceof MethodAttributeEntry) {
                MethodAttributeEntry mae=(MethodAttributeEntry)ae;
                if(mae.hasSetMethod()) {
                    atts.put(attributeName,
                             new MethodAttributeEntry(mae.getInfo(), mae.getSetMethod(), method));
                }
            } //we don't have such entry
            else {
                atts.put(attributeName, new MethodAttributeEntry(info, findSetter(obj.getClass(), attributeName), method));
            }
        } //is it a set method?
        else {
            if(ae instanceof FieldAttributeEntry) {
                //we already have annotated field as write
                if(ae.getInfo().isWritable()) {
                    log.warn("Not adding annotated method " + methodName + " since we already have writable attribute");
                }
                else {
                    //we already have annotated field as read
                    //lets make the field writable
                    Field f = ((FieldAttributeEntry)ae).getField();
                    MBeanAttributeInfo i=new MBeanAttributeInfo(ae.getInfo().getName(),
                                                                f.getType().getCanonicalName(),
                                                                descr,
                                                                true,
                                                                !Modifier.isFinal(f.getModifiers()),
                                                                false);
                    atts.put(attributeName,new FieldAttributeEntry(i,f));
                }
            }
            //we already have annotated getOrIs method
            else if(ae instanceof MethodAttributeEntry) {
                MethodAttributeEntry mae=(MethodAttributeEntry)ae;
                if(mae.hasIsOrGetMethod()) {
                    atts.put(attributeName,
                             new MethodAttributeEntry(info,
                                                      method,
                                                      mae.getIsOrGetMethod()));
                }
            } // we don't have such entry
            else {
                atts.put(attributeName, new MethodAttributeEntry(info, method, findGetter(obj.getClass(), attributeName)));
            }
        }
    }

    
    private static boolean isSetMethod(Method method) {
        return(method.getName().startsWith("set") && 
               method.getParameterTypes().length == 1 && 
               method.getReturnType() == java.lang.Void.TYPE);
    }

    private static boolean isGetMethod(Method method) {
        return(method.getParameterTypes().length == 0 && 
               method.getReturnType() != java.lang.Void.TYPE && 
               method.getName().startsWith("get"));
    }

    private static boolean isIsMethod(Method method) {
        return(method.getParameterTypes().length == 0 && 
              (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class) && 
               method.getName().startsWith("is"));
    }

    public static Method findGetter(Class clazz, String name) {
        try {
            return clazz.getMethod("get" + name);
        }
        catch(NoSuchMethodException e) {
        }

        try {
            return clazz.getMethod("is" + name);
        }
        catch(NoSuchMethodException ex) {
        }
        return null;
    }


    public static Method findSetter(Class clazz, String name) {
        try {
            return clazz.getMethod("set" + name);
        }
        catch(NoSuchMethodException e) {
        }

        return null;
    }


    private void findFields() {
        //traverse class hierarchy and find all annotated fields
        for(Class<?> clazz=getObject().getClass();clazz != null; clazz=clazz.getSuperclass()) {

            Field[] fields=clazz.getDeclaredFields();
            for(Field field:fields) {
                ManagedAttribute attr=field.getAnnotation(ManagedAttribute.class);
                Property prop=field.getAnnotation(Property.class);
                boolean expose_prop=prop != null && prop.exposeAsManagedAttribute();
                boolean expose=attr != null || expose_prop;

                if(expose) {
                    String fieldName = Util.attributeNameToMethodName(field.getName());
                    String descr=attr != null? attr.description() : prop.description();
                    boolean writable=attr != null? attr.writable() : prop.writable();

                    MBeanAttributeInfo info=new MBeanAttributeInfo(fieldName,
                                                                   field.getType().getCanonicalName(),
                                                                   descr,
                                                                   true,
                                                                   !Modifier.isFinal(field.getModifiers()) && writable,
                                                                   false);

                    atts.put(fieldName, new FieldAttributeEntry(info, field));
                }
            }
        }
    }

    private Attribute getNamedAttribute(String name) {
        Attribute result=null;
        if(name.equals(ResourceDMBean.MBEAN_DESCRITION)) {
            result=new Attribute(ResourceDMBean.MBEAN_DESCRITION, this.description);
        }
        else {
            AttributeEntry entry=atts.get(name);
            if(entry != null) {
                try {
                    result=new Attribute(name, entry.invoke(null));
                }
                catch(Exception e) {
                    log.warn("Exception while reading value of attribute " + name, e);
                }
            }
            else {
                log.warn("Did not find queried attribute with name " + name);
            }
        }
        return result;
    }

    private boolean setNamedAttribute(Attribute attribute) {
        boolean result=false;
        AttributeEntry entry=atts.get(attribute.getName());
        if(entry != null) {
            try {
                entry.invoke(attribute);
                result=true;
            }
            catch(Exception e) {
                log.warn("Exception while writing value for attribute " + attribute.getName(), e);
            }            
        }
        else {
            log.warn("Could not invoke set on attribute " + attribute.getName()
                     + " with value "
                     + attribute.getValue());
        }
        return result;
    }
    
    

    private boolean isMBeanAnnotationPresentWithExposeAll(){
        Class<? extends Object> c=getObject().getClass();
    	return c.isAnnotationPresent(MBean.class) && c.getAnnotation(MBean.class).exposeAll();
    }
       
    private class MethodAttributeEntry implements AttributeEntry {

        final MBeanAttributeInfo info;

        final Method isOrGetmethod;

        final Method setMethod;

        public MethodAttributeEntry(final MBeanAttributeInfo info,
                                    final Method setMethod,
                                    final Method isOrGetMethod) {
            super();
            this.info=info;
            this.setMethod=setMethod;
            this.isOrGetmethod=isOrGetMethod;
        }

        public Object invoke(Attribute a) throws Exception {
            if(a == null && isOrGetmethod != null)
                return isOrGetmethod.invoke(getObject());
            else if(a != null && setMethod != null)
                return setMethod.invoke(getObject(), a.getValue());
            else
                return null;
        }

        public MBeanAttributeInfo getInfo() {
            return info;
        }

        public boolean hasIsOrGetMethod() {
            return isOrGetmethod != null;
        }

        public boolean hasSetMethod() {
            return setMethod != null;
        }

        public Method getIsOrGetMethod() {
            return isOrGetmethod;
        }

        public Method getSetMethod() {
            return setMethod;
        }
    }

    private class FieldAttributeEntry implements AttributeEntry {

        private final MBeanAttributeInfo info;

        private final Field field;

        public FieldAttributeEntry(final MBeanAttributeInfo info,final Field field) {
            super();
            this.info=info;
            this.field=field;
            if(!field.isAccessible()) {
                field.setAccessible(true);
            }
        }
        
        public Field getField(){
            return field;
        }

        public Object invoke(Attribute a) throws Exception {
            if(a == null) {
                return field.get(getObject());
            }
            else {
                field.set(getObject(), a.getValue());
                return null;
            }
        }

        public MBeanAttributeInfo getInfo() {
            return info;
        }

    }

    private interface AttributeEntry {
        public Object invoke(Attribute a) throws Exception;

        public MBeanAttributeInfo getInfo();
    }

}
