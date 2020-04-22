package org.jgroups.stack;

import org.jgroups.JChannel;
import org.jgroups.JChannelProbeHandler;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.jmx.AdditionalJmxObjects;
import org.jgroups.jmx.ResourceDMBean;
import org.jgroups.util.Util;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * A {@link org.jgroups.stack.DiagnosticsHandler.ProbeHandler} that does not use reflection. Can be used instead of the
 * default ProbeHandler for commands "jmx" and "op"
 * @author Bela Ban
 * @since  4.1.0
 */
public class NonReflectiveProbeHandler extends JChannelProbeHandler {
    // List of fields and field getters. Can be used to get field values, invoke field getters to get field values
    // and set fields (via Field.set()).
    protected final Map<String,Map<String,ResourceDMBean.Accessor>> attrs=new LinkedHashMap<>();

    // List of fields and corresponding setters. Can be used to set values of fields
    protected final Map<String,Map<String,ResourceDMBean.Accessor>> setters=new LinkedHashMap<>();

    // List of operations
    protected final Map<String,Map<String,ResourceDMBean.MethodAccessor>> operations=new LinkedHashMap<>();

    protected static final Predicate<AccessibleObject> FILTER=obj -> obj.isAnnotationPresent(ManagedAttribute.class) ||
      (obj.isAnnotationPresent(Property.class) && obj.getAnnotation(Property.class).exposeAsManagedAttribute()) ||
      obj.isAnnotationPresent(ManagedOperation.class);

    public NonReflectiveProbeHandler(JChannel ch) {
        super(ch);
    }

    public NonReflectiveProbeHandler initialize(Protocol[] protocols) {
        return initialize(Arrays.asList(protocols));
    }

    public NonReflectiveProbeHandler initialize(Collection<Protocol> prots) {
        for(Protocol prot: prots) {
            String prot_name=prot.getName();
            Map<String,ResourceDMBean.Accessor> m=attrs.computeIfAbsent(prot_name, k -> new TreeMap<>());

            BiConsumer<Field,Object> field_func=(f,o) -> m.put(f.getName(), new ResourceDMBean.FieldAccessor(f, o));

            BiConsumer<Method,Object> method_func=(method,obj) -> { // getter
                if(method.isAnnotationPresent(ManagedOperation.class)) {
                    Map<String,ResourceDMBean.MethodAccessor> tmp=operations.computeIfAbsent(prot_name, k -> new TreeMap<>());
                    tmp.put(method.getName(), new ResourceDMBean.MethodAccessor(method, obj));
                }
                else if(ResourceDMBean.isGetMethod(method)) {
                    String method_name=Util.getNameFromAnnotation(method);
                    String attributeName=Util.methodNameToAttributeName(method_name);
                    m.put(attributeName, new ResourceDMBean.MethodAccessor(method, obj));
                }
                else if(ResourceDMBean.isSetMethod(method)) { // setter
                    Map<String,ResourceDMBean.Accessor> tmp=setters.computeIfAbsent(prot_name, k -> new TreeMap<>());
                    String method_name=Util.getNameFromAnnotation(method);
                    String attributeName=Util.methodNameToAttributeName(method_name);
                    tmp.put(attributeName, new ResourceDMBean.MethodAccessor(method, obj));
                }
            };

            Util.forAllFieldsAndMethods(prot, FILTER, field_func, method_func);
            if(prot instanceof AdditionalJmxObjects) {
                Object[] objects=((AdditionalJmxObjects)prot).getJmxObjects();
                if(objects != null) {
                    for(Object obj: objects)
                        if(obj != null)
                            Util.forAllFieldsAndMethods(obj, FILTER, field_func, method_func);
                }
            }
        }
        return this;
    }

    public String dump() {
        StringBuilder sb=new StringBuilder("attrs\n-----\n");
        for(Map.Entry<String,Map<String,ResourceDMBean.Accessor>> e: attrs.entrySet()) {
            sb.append(e.getKey() + ":\n");
            Map<String,ResourceDMBean.Accessor> val=e.getValue();
            val.forEach((key, value) -> sb.append(key + ": " + value).append("\n"));
        }
        sb.append("\nsetters\n--------\n");
        for(Map.Entry<String,Map<String,ResourceDMBean.Accessor>> e: setters.entrySet()) {
            sb.append(e.getKey() + ":\n");
            Map<String,ResourceDMBean.Accessor> val=e.getValue();
            val.forEach((key, value) -> sb.append(key + ": " + value).append("\n"));
        }
        sb.append("\n");
        sb.append("\noperations\n--------\n");
        for(Map.Entry<String,Map<String,ResourceDMBean.MethodAccessor>> e: operations.entrySet()) {
            sb.append(e.getKey() + ":\n");
            Map<String,ResourceDMBean.MethodAccessor> val=e.getValue();
            val.forEach((key, value) -> sb.append(key + ": " + value).append("\n"));
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    protected Map<String,Map<String,Object>> dumpAttrsAllProtocols() {
        Map<String,Map<String,Object>> retval=new HashMap<>();
        for(Map.Entry<String,Map<String,ResourceDMBean.Accessor>> e: attrs.entrySet()) {
            String protocol_name=e.getKey();
            Map<String,ResourceDMBean.Accessor> val=e.getValue();
            Map<String,Object> map=new TreeMap<>();
            for(Map.Entry<String,ResourceDMBean.Accessor> en: val.entrySet()) {
                try {
                    Object v=en.getValue().invoke(null);
                    if(v instanceof Double)
                        v=String.format("%.2f", v);
                    map.put(en.getKey(), v != null? v.toString() : "null");
                }
                catch(Exception ex) {
                    log.error("failed getting value for attribute %s.%s: %s", protocol_name, en.getKey(), ex);
                }
            }
            if(!map.isEmpty())
                retval.put(protocol_name, map);
        }
        return retval;
    }

    @Override
    protected Map<String,Map<String,Object>> dumpAttrsSelectedProtocol(String protocol_name, List<String> attrs) {
        Map<String,ResourceDMBean.Accessor> map=this.attrs.get(protocol_name);
        if(map == null)
            return null;
        Map<String,Map<String,Object>> retval=new HashMap<>();
        Map<String,Object> tmp;
        retval.put(protocol_name, tmp=new TreeMap<>());

        for(Map.Entry<String,ResourceDMBean.Accessor> e: map.entrySet()) {
            String attr_name=e.getKey();
            if(attrs == null || attrs.stream().anyMatch(attr_name::startsWith)) {
                try {
                    Object v=e.getValue().invoke(null);
                    tmp.put(attr_name, v);
                }
                catch(Exception ex) {
                    log.error("failed getting value for attribute %s.%s: %s", protocol_name, attr_name, ex);
                }
            }
        }
        return retval;
    }

    protected void handleAttrWrite(String protocol_name, String attr_name, String attr_value) {
        Object converted_value=null;
        // Try to find a suitable setter first:
        Map<String,ResourceDMBean.Accessor> m=setters.get(protocol_name);
        if(m != null) {
            ResourceDMBean.Accessor setter=m.get(attr_name);
            if(setter != null) {
                Class<?> type=((ResourceDMBean.MethodAccessor)setter).getMethod().getParameterTypes()[0];
                converted_value=Util.convert(attr_value, type);
                invoke(protocol_name, setter, attr_name, converted_value);
                return;
            }
        }

        // try to find a field
        m=attrs.get(protocol_name);
        if(m == null)
            throw new RuntimeException(String.format("protocol %s not found", protocol_name));
        ResourceDMBean.Accessor setter=m.get(attr_name);
        if(setter == null)
            throw new RuntimeException(String.format("attribute %s not found in protocol %s", attr_name, protocol_name));
        if(setter instanceof ResourceDMBean.FieldAccessor) {
            converted_value=Util.convert(attr_value, ((ResourceDMBean.FieldAccessor)setter).getField().getType());
            invoke(protocol_name, setter, attr_name, converted_value);
        }
    }

    @Override
    protected Method findMethod(Protocol prot, String method_name, String[] args) throws Exception {
        Map<String,ResourceDMBean.MethodAccessor> map=operations.get(prot.getName());
        if(map == null) {
            log.error("protocol %s not found for method %s", prot.getName(), method_name);
            return null;
        }
        ResourceDMBean.MethodAccessor accessor=map.get(method_name);
        if(accessor == null) {
            log.error("method %s not found", method_name);
            return null;
        }
        return accessor.getMethod();
    }

    protected static void invoke(String protocol_name, ResourceDMBean.Accessor setter, String attr, Object value) {
        try {
            setter.invoke(value);
        }
        catch(Exception e) {
            throw new RuntimeException(String.format("setting %s=%s failed in protocol %s: %s", attr, value, protocol_name, e));
        }
    }
}
