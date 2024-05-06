package org.jgroups.jmx;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Average;
import org.jgroups.util.Util;

import javax.management.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * 
 * A DynamicMBean wrapping an annotated object instance and exposing attributes annotated with @ManagedAttribute and
 * operations annotated with @ManagedOperation.
 * 
 * @author Chris Mills
 * @author Vladimir Blagojevic
 * @author Bela Ban
 * @see org.jgroups.annotations.ManagedAttribute
 * @see org.jgroups.annotations.ManagedOperation
 * @see org.jgroups.annotations.MBean
 *
 */
public class ResourceDMBean implements DynamicMBean {
    protected static final Class<?>[]              primitives= {int.class, byte.class, short.class, long.class,
                                                                float.class, double.class, boolean.class, char.class };

    protected static final List<Method>            OBJECT_METHODS; // all methods of java.lang.Object
    protected final boolean                        expose_all;
    protected final Log                            log=LogFactory.getLog(ResourceDMBean.class);
    protected final Object                         obj;
    protected List<Object>                         components;
    protected final MBeanAttributeInfo[]           attrInfo;
    protected final MBeanOperationInfo[]           opInfo;
    protected final HashMap<String,AttributeEntry> atts=new HashMap<>();
    protected final List<MBeanOperationInfo>       ops=new ArrayList<>();

    static {OBJECT_METHODS=new ArrayList<>(Arrays.asList(Object.class.getMethods()));}

    protected static final Predicate<AccessibleObject> FILTER=obj -> obj.isAnnotationPresent(ManagedAttribute.class) ||
      (obj.isAnnotationPresent(Property.class) && obj.getAnnotation(Property.class).exposeAsManagedAttribute());

    public ResourceDMBean(Object instance) {
        this(instance, null);
    }

    public ResourceDMBean(Object instance, Predicate<AccessibleObject> filter) {
        if(instance == null)
            throw new NullPointerException("Cannot make an MBean wrapper for null instance");
        this.obj=instance;
        Class<? extends Object> c=obj.getClass();
        expose_all=c.isAnnotationPresent(MBean.class) && c.getAnnotation(MBean.class).exposeAll();

        findFields(instance, filter, null);
        findMethods(instance, filter, null);
        fixFields(instance);

        List<Object> objects=Util.getComponents(instance);
        if(objects != null) {
            for(Object inst: objects) {
                if(inst != null) {
                    if(components == null)
                        components=new ArrayList<>();
                    String prefix=Util.methodNameToAttributeName(inst.getClass().getSimpleName());
                    components.add(inst);
                    findFields(inst, filter, prefix);
                    findMethods(inst, filter, prefix);
                    fixFields(inst);
                }
            }
        }

        attrInfo=new MBeanAttributeInfo[atts.size()];
        int i=0;
        MBeanAttributeInfo info=null;
        for(AttributeEntry entry: atts.values()) {
            info=entry.info;
            attrInfo[i++]=info;
        }

        opInfo=new MBeanOperationInfo[ops.size()];
        ops.toArray(opInfo);
    }


    public MBeanInfo getMBeanInfo() {
        return new MBeanInfo(obj.getClass().getCanonicalName(), "DynamicMBean", attrInfo, null, opInfo, null);
    }

    public void forAllAttributes(BiConsumer<String,AttributeEntry> c) {
        if(c == null)
            return;
        for(Map.Entry<String,AttributeEntry> e: atts.entrySet()) {
            c.accept(e.getKey(), e.getValue());
        }
    }

    public Object getAttribute(String name) {
        if(name == null || name.isEmpty())
            throw new NullPointerException("Invalid attribute requested " + name);
        Attribute attr=getNamedAttribute(name);
        return attr != null? attr.getValue() : null;
    }

    public void setAttribute(Attribute attribute) {
        if(attribute == null || attribute.getName() == null)
            throw new NullPointerException("Invalid attribute requested " + attribute);
        setNamedAttribute(attribute);
    }

    public AttributeList getAttributes(String[] names) {
        AttributeList al=new AttributeList();
        for(String name: names) {
            Attribute attr=getNamedAttribute(name);
            if(attr != null)
                al.add(attr);
            else
                log.warn("Did not find attribute " + name);
        }
        return al;
    }

    public AttributeList setAttributes(AttributeList list) {
        AttributeList results=new AttributeList();
        for(int i=0;i < list.size();i++) {
            Attribute attr=(Attribute)list.get(i);
            if(setNamedAttribute(attr))
                results.add(attr);
            else {
                if(log.isWarnEnabled())
                    log.warn("Failed to update attribute name " + attr.getName() + " with value " + attr.getValue());
            }
        }
        return results;
    }

    public Object invoke(String name, Object[] args, String[] sig) throws MBeanException, ReflectionException {
        try {
            Class<?>[] classes=new Class[sig.length];
            for(int i=0;i < classes.length;i++)
                classes[i]=getClassForName(sig[i]);
            Method method=null;
            if(components != null) {
                for(Object o: components) {
                    try {
                        method=o.getClass().getMethod(name, classes);
                    }
                    catch(Throwable t) {
                        continue;
                    }
                    return method.invoke(o, args);
                }
            }
            method=obj.getClass().getMethod(name, classes);
            return method.invoke(obj, args);
        }
        catch(Exception e) {
            throw new MBeanException(e);
        }
    }


    public static boolean isSetMethod(Method method) {
        return method.getParameterCount() == 1;
    }

    public static boolean isGetMethod(Method method) {
        return method.getParameterTypes().length == 0 && method.getReturnType() != Void.TYPE;
    }

    public static boolean isIsMethod(Method method) {
        return method.getParameterTypes().length == 0 &&
          (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class);
    }


    public static void dumpStats(Object obj, final Map<String,Object> map) {
        dumpStats(obj, "", map);
    }

    public static void dumpStats(Object obj, String prefix, final Map<String,Object> map) {
        BiConsumer<Field,Object> field_func=(f,o) -> {
            String attr_name=null;
            try {
                f.setAccessible(true);
                Object value=f.get(o);
                attr_name=Util.getNameFromAnnotation(f);
                if(attr_name != null && !attr_name.trim().isEmpty())
                    attr_name=attr_name.trim();
                else
                    attr_name=f.getName();
                if(prefix != null && !prefix.isEmpty())
                    attr_name=prefix + "." + attr_name;
                map.put(attr_name, prettyPrint(value, f));
            }
            catch(Exception e) {
                throw new RuntimeException(String.format("could not read value of attribute \"%s\"", attr_name), e);
            }
        };
        BiConsumer<Method,Object> getter_func=(m,o) -> {
            String method_name=null;
            if(!ResourceDMBean.isGetMethod(m))
                return;
            try {
                Object value=m.invoke(o);
                method_name=Util.getNameFromAnnotation(m);
                if(method_name != null && !method_name.trim().isEmpty())
                    method_name=method_name.trim();
                else {
                    String field_name=Util.methodNameToAttributeName(m.getName());
                    method_name=Util.attributeNameToMethodName(field_name);
                }
                String attributeName=Util.methodNameToAttributeName(method_name);
                if(prefix != null && !prefix.isEmpty())
                    attributeName=prefix + "." + attributeName;
                map.put(attributeName, prettyPrint(value, m));
            }
            catch(Exception e) {
                throw new RuntimeException(String.format("could not invoke getter method \"%s\"", method_name),e);
            }
        };
        Util.forAllFieldsAndMethods(obj, FILTER, field_func, getter_func);
    }


    @SuppressWarnings("MalformedFormatString")
    public static String prettyPrint(Object val, AccessibleObject a) {
        if(val == null) return "null";
        Class<?> cl=val.getClass();
        AttributeType type=getType(a);
        if(type != null) {
            switch(type) {
                case BYTES:
                    return Util.printBytes(((Number)val).doubleValue());
                case TIME:
                    TimeUnit unit=getTimeUnit(a);
                    return Util.printTime(((Number)val).doubleValue(), unit);
                case SCALAR:
                    if(isNumeric(cl))
                        return String.format("%,d", val);
                    if(cl.equals(LongAdder.class))
                        return String.format("%,d", ((LongAdder)val).sum());
                    if(val instanceof Number)
                        return String.format("%,d", ((Number)val).longValue());
                    break;
            }
        }
        if(isNumeric(cl))
            return String.format("%d", val);
        if(isFractional(cl))
            return String.format("%,.2f", val);
        if(cl.equals(String[].class))
            return Arrays.toString((String[])val);
        return val.toString();
    }

    public static boolean isNumeric(Class<?> cl) {
        return cl.equals(short.class) || cl.equals(Short.class) || cl.equals(int.class) || cl.equals(Integer.class)
          || cl.equals(long.class) || cl.equals(Long.class);
    }

    public static boolean isFractional(Class<?> cl) {
        return cl.equals(float.class) || cl.equals(Float.class) || cl.equals(double.class) || cl.equals(Double.class);
    }

    public static boolean isNumber(Class<?> cl) {
        return isNumeric(cl) || isFractional(cl) || Number.class.isAssignableFrom(cl) || Average.class.isAssignableFrom(cl);
    }


    protected static AttributeType getType(AccessibleObject ao) {
        Property prop=ao.getAnnotation(Property.class);
        if(prop != null)
            return prop.type();
        ManagedAttribute attr=ao.getAnnotation(ManagedAttribute.class);
        return attr != null? attr.type() : null;
    }

    protected static TimeUnit getTimeUnit(AccessibleObject ao) {
        Property prop=ao.getAnnotation(Property.class);
        if(prop != null)
            return prop.unit();
        ManagedAttribute attr=ao.getAnnotation(ManagedAttribute.class);
        return attr != null? attr.unit() : null;
    }


    protected static Class<?> getClassForName(String name) throws ClassNotFoundException {
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

    protected void findMethods(Object instance, Predicate<AccessibleObject> filter, String prefix) {
        // find all methods but don't include methods from Object class
        List<Method> methods = new ArrayList<>(Arrays.asList(instance.getClass().getMethods()));
        methods.removeAll(OBJECT_METHODS);

        for(Method method: methods) {

            // does method have @ManagedAttribute annotation?
            if(method.isAnnotationPresent(ManagedAttribute.class) || method.isAnnotationPresent(Property.class)) {
                if(filter != null && !filter.test(method))
                    continue;
                exposeManagedAttribute(method, instance, prefix);
            }
            //or @ManagedOperation
            else if (method.isAnnotationPresent(ManagedOperation.class) || expose_all){
                ManagedOperation op=method.getAnnotation(ManagedOperation.class);
                ops.add(new MBeanOperationInfo(op != null? op.description() : "",method));
            }
        }
    }

    /** Provides field-based getter and/or setters for all attributes in attrs if not present */
    protected void fixFields(Object instance) {
        for(AttributeEntry attr: atts.values()) {
            if(attr.getter == null)
                attr.getter=findGetter(instance, attr.name);
            if(attr.setter == null)
                attr.setter=findSetter(instance, attr.name);
            if(attr.setter == null)
                attr.setter=new NoopAccessor();
        }
    }



    protected void exposeManagedAttribute(Method method, Object instance, String prefix) {
        String           methodName=method.getName();
        ManagedAttribute attr_annotation=method.getAnnotation(ManagedAttribute.class);
        Property         prop=method.getAnnotation(Property.class);

        boolean expose_prop=prop != null && prop.exposeAsManagedAttribute();
        boolean expose=attr_annotation != null || expose_prop;
        if(!expose)
            return;

        boolean writable=(prop != null && prop.writable()) || (attr_annotation != null && attr_annotation.writable());

        // Is name of @ManagedAttributed or @Property used?
        String attr_name=attr_annotation != null? attr_annotation.name() : prop != null? prop.name() : null;
        if(attr_name != null && !attr_name.trim().isEmpty())
            attr_name=attr_name.trim();
        else {
            // getFooBar() --> foo_bar
            attr_name=Util.methodNameToAttributeName(methodName);
            if(!atts.containsKey(attr_name)) {

                // hmm, maybe we need to look for an attribute fooBar
                String tmp=Util.methodNameToJavaAttributeName(methodName);
                if(atts.containsKey(tmp))
                    attr_name=tmp;
            }
        }

        String descr=attr_annotation != null ? attr_annotation.description() : prop != null? prop.description() : null;
        AttributeEntry attr=atts.get(prefix(prefix,attr_name));
        if(attr != null) {
            if(isSetMethod(method)) {
                if(attr.setter != null) {
                    if(log.isWarnEnabled())
                        log.warn("setter for \"" + attr_name + "\" is already defined (new method=" + method.getName() + ")");
                }
                else
                    attr.setter=new MethodAccessor(method, instance);
            }
            else {
                if(attr.getter != null) {
                    if(log.isWarnEnabled())
                        log.warn("getter for \"" + attr_name + "\" is already defined (new method=" + method.getName() + ")");
                }
                else
                    attr.getter=new MethodAccessor(method, instance);
            }
        }
        else { // create a new entry in atts
            boolean is_setter=isSetMethod(method);
            String type=is_setter? method.getParameterTypes()[0].getCanonicalName() : method.getReturnType().getCanonicalName();
            MBeanAttributeInfo info=new MBeanAttributeInfo(prefix(attr_name, prefix), type, descr, true, writable, methodName.startsWith("is"));
            AttributeEntry entry=new AttributeEntry(method, Util.methodNameToAttributeName(methodName), info);
            if(is_setter)
                entry.setter(new MethodAccessor(method, instance));
            else
                entry.getter(new MethodAccessor(method, instance));
            atts.put(prefix(attr_name, prefix), entry);
        }
    }

    protected static String prefix(String s, String prefix) {
        return prefix == null? s : prefix + "." + s;
    }

    /** Finds an accessor for an attribute. Tries to find getAttrName(), isAttrName(), attrName() methods. If not
     * found, tries to use reflection to get the value of attr_name. If still not found, creates a NullAccessor. */
    protected static Accessor findGetter(Object target, String attr_name) {
        final String name=Util.attributeNameToMethodName(attr_name);
        Class<?> clazz=target.getClass();
        Method   method=Util.findMethod(target, Arrays.asList("get" + name, "is" + name, toLowerCase(name)));

        if(method != null && (isGetMethod(method) || isIsMethod(method)))
            return new MethodAccessor(method, target);


         // 4. Find a field last_name
        Field field=Util.getField(clazz, attr_name);
        if(field != null)
            return new FieldAccessor(field, target);
        return new NoopAccessor();
    }

    /** Finds an accessor for an attribute. Tries to find setAttrName(), attrName() methods. If not
     * found, tries to use reflection to set the value of attr_name. If still not found, creates a NullAccessor. */
    public static Accessor findSetter(Object target, String attr_name) {
        final String name=Util.attributeNameToMethodName(attr_name);
        final String fluent_name=toLowerCase(name);
        Class<?> clazz=target.getClass();
        Class<?> field_type=null;
        Field field=Util.getField(clazz, attr_name);
        field_type=field != null? field.getType() : null;
        String setter_name="set" + name;

        if(field_type != null) {
            Method method=Util.findMethod(target, Arrays.asList(fluent_name, setter_name), field_type);
            if(method != null && isSetMethod(method))
                return new MethodAccessor(method, target);
        }

        // Find all methods but don't include methods from Object class
        List<Method> methods=new ArrayList<>(Arrays.asList(clazz.getMethods()));
        methods.removeAll(OBJECT_METHODS);

        for(Method method: methods) {
            String method_name=method.getName();
            if((method_name.equals(name) || method_name.equals(fluent_name) || method_name.equals(setter_name)) && isSetMethod(method))
                return new MethodAccessor(method, target);
        }

        // Find a field last_name
        if(field != null)
            return new FieldAccessor(field, target);
        return null;
    }

    /** Returns a string with the first letter being lowercase */
    protected static String toLowerCase(String input) {
        if(Character.isUpperCase(input.charAt(0)))
            return input.substring(0, 1).toLowerCase() + input.substring(1);
        return input;
    }


    protected void findFields(Object instance, Predicate<AccessibleObject> filter, String prefix) {
        // traverse class hierarchy and find all annotated fields
        for(Class<?> clazz=instance.getClass(); clazz != null && clazz != Object.class; clazz=clazz.getSuperclass()) {
            Field[] fields=clazz.getDeclaredFields();
            for(Field field: fields) {
                ManagedAttribute annotation=field.getAnnotation(ManagedAttribute.class);
                Property prop=field.getAnnotation(Property.class);
                boolean expose_prop=prop != null && prop.exposeAsManagedAttribute();
                boolean expose=annotation != null || expose_prop;

                if(expose) {
                    if(filter != null && !filter.test(field))
                        continue;

                    String fieldName=annotation != null? annotation.name() : (prop != null? prop.name() : null);
                    if(fieldName != null && fieldName.trim().isEmpty())
                        fieldName=field.getName();
                    if(prefix != null)
                        fieldName=prefix + "." + fieldName;

                    String descr=annotation != null? annotation.description() : prop.description();
                    boolean writable=annotation != null? annotation.writable() : prop.writable();

                    MBeanAttributeInfo info=new MBeanAttributeInfo(fieldName,
                                                                   field.getType().getCanonicalName(),
                                                                   descr,
                                                                   true,
                                                                   !Modifier.isFinal(field.getModifiers()) && writable,
                                                                   false);

                    atts.put(fieldName, new AttributeEntry(field, field.getName(), info));
                }
            }
        }
    }


    protected Attribute getNamedAttribute(String name) {
        AttributeEntry entry=atts.get(name);
        if(entry != null) {
            try {
                return new Attribute(name, entry.getter.invoke(null));
            }
            catch(Exception e) {
                log.warn(Util.getMessage("AttrReadFailure"), name, e);
            }
        }
        else {
            log.warn(Util.getMessage("MissingAttribute"), name);
        }
        return null;
    }


    protected boolean setNamedAttribute(Attribute attribute) {
        AttributeEntry entry=atts.get(attribute.getName());
        if(entry != null) {
            try {
                entry.setter.invoke(attribute.getValue());
                return true;
            }
            catch(Throwable e) {
                log.warn(Util.getMessage("AttrWriteFailure"), attribute.getName(), e);
            }            
        }
        else {
            log.warn(Util.getMessage("MissingAttribute"), attribute.getName());
        }
        return false;
    }
    
    

    public static class AttributeEntry {
        protected final AccessibleObject   type; // method of field
        /** The name of the field or method. Can be different from the key in atts when name in @Property or
         * @ManagedAttribute  was used */
        protected final String             name;
        protected final MBeanAttributeInfo info;
        protected Accessor                 getter;
        protected Accessor                 setter;

        public AttributeEntry(AccessibleObject type, String name, MBeanAttributeInfo info) {
            this(type, name, info, null, null);
        }

        public AttributeEntry(AccessibleObject type, String name, MBeanAttributeInfo info, Accessor getter, Accessor setter) {
            this.type=type;
            this.name=name;
            this.info=info;
            this.getter=getter;
            this.setter=setter;
        }

        public AccessibleObject   type()                      {return type;}
        public String             name()                      {return name;}
        public MBeanAttributeInfo info()                      {return info;}
        public Accessor           getter()                    {return getter;}
        public AttributeEntry     getter(Accessor new_getter) {this.getter=new_getter; return this;}
        public Accessor           setter()                    {return setter;}
        public AttributeEntry     setter(Accessor new_setter) {this.setter=new_setter; return this;}


        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("AttributeEntry[" + name);
            if(getter != null)
                sb.append(", getter=" + getter);
            if(setter() != null)
                sb.append(", setter=" + setter);
            sb.append("]");
            return sb.toString();
        }
    }


    public interface Accessor {
        /** Invokes a getter or setter. For the getter, new_val must be ignored (null) */
        Object invoke(Object new_val) throws Exception;
    }


    public static class MethodAccessor implements Accessor {
        protected final Method method;
        protected final Object target;

        public MethodAccessor(Method method, Object target) {
            this.method=method;
            this.target=target;
        }

        public Method getMethod() {return method;}

        public Object invoke(Object new_val) throws Exception {
            return new_val != null? method.invoke(target, new_val) : method.invoke(target);
        }

        public String toString() {return "method[" + method.getName() + "()]";}
    }

    public static class FieldAccessor implements Accessor {
        protected final Field                       field;
        protected final Object                      target;
        protected static final MethodHandles.Lookup LOOKUP=MethodHandles.lookup();
        protected       MethodHandle                mh;

        public FieldAccessor(Field field, Object target) {
            this.field=field;
            this.target=target;
            if(!field.isAccessible())
                field.setAccessible(true);
            try {
                mh=LOOKUP.unreflectGetter(field);
            }
            catch(IllegalAccessException e) {
                // todo: log warning?
                // throw new RuntimeException(e);
            }
        }

        public Field getField() {return field;}

        public Object invoke(Object new_val) throws Exception {
            if(new_val == null) {
                if(mh != null) {
                    try {
                        return mh.invoke(target);
                    }
                    catch(Throwable e) {
                        ;
                    }
                }
                return field.get(target);
            }
            else {
                field.set(target, new_val);
                return null;
            }
        }

        public String toString() {return "field[" + field.getName() + "]";}
    }


    public static class NoopAccessor implements Accessor {
        public Object invoke(Object new_val) throws Exception {return null;}

        public String toString() {return "NoopAccessor";}
    }


}
