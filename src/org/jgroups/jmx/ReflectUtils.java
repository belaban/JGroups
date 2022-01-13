package org.jgroups.jmx;

import org.jgroups.annotations.ManagedOperation;
import org.jgroups.blocks.MethodCall;
import org.jgroups.util.Ref;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Methods to get/set attributes and invoke operations of a given instance.
 * @author Bela Ban
 * @since  5.2
 */
public class ReflectUtils {

    /** Handles an attribute read or write */
    public static void handleAttributeAccess(Map<String,String> map, String attrs, Object target) {
        if(target == null)
            return;
        List<String> list=attrs != null? Util.parseStringList(attrs, ",") : null;

        // check if there are any attribute-sets in the list
        if(list != null) {
            for(Iterator<String> it=list.iterator(); it.hasNext(); ) {
                String tmp=it.next();
                int index=tmp.indexOf('=');
                if(index > -1) { // an attribute write
                    it.remove();
                    String attrname=tmp.substring(0, index);
                    String attrvalue=tmp.substring(index + 1);
                    try {
                        handleAttrWrite(target, attrname, attrvalue);
                    }
                    catch(Exception e) {
                        throw new IllegalArgumentException(String.format("failed writing: %s", e));
                    }
                }
            }
        }
        Map<String,Map<String,Object>> tmp_stats=dumpSelectedAttributes(target, list);
        convert(tmp_stats, map);
    }


    /**
     * Reads all attributes and values of the given target (annotated with
     * {@link org.jgroups.annotations.ManagedAttribute} and filters them if a non-null list of attributes is given
     * @param target The object to read the attributes from
     * @param attrs A list of attributes to match against. Example: <pre>bind</pre> with match <pre>bind_addr</pre>,
     *              <pre>bind_port</pre> etc. If <pre>null</pre>, all attributes will be read.
     * @return A map of attributes and values, keyed by the object's class
     */
    public static Map<String,Map<String,Object>> dumpSelectedAttributes(Object target, List<String> attrs) {
        Map<String,Map<String,Object>> retval=new HashMap<>();
        Map<String,Object> tmp=new TreeMap<>();
        dumpStats(target, tmp);
        if(attrs != null && !attrs.isEmpty()) {
            // weed out attrs not in list
            for(Iterator<String> it=tmp.keySet().iterator(); it.hasNext(); ) {
                String attrname=it.next();
                boolean found=false;
                for(String attr : attrs) {
                    if(attrname.startsWith(attr)) {
                        found=true;
                        break; // found
                    }
                }
                if(!found)
                    it.remove();
            }
        }
        String pname=target.getClass().getSimpleName();
        retval.put(pname, tmp);
        return retval;
    }

    public static void dumpStats(Object obj, Map<String,Object> map) {
        ResourceDMBean.dumpStats(obj, map); // dumps attrs and operations into tmp
        Util.forAllComponents(obj, (o,prefix) -> ResourceDMBean.dumpStats(o, prefix, map));
    }


    /** Prints all operations of clazz annotated with {@link ManagedOperation} */
    public static String listOperations(Class<?> clazz) {
        if(clazz == null)
            return "";
        StringBuilder sb=new StringBuilder(clazz.getSimpleName()).append(":\n");
        Method[] methods=Util.getAllDeclaredMethodsWithAnnotations(clazz, ManagedOperation.class);
        for(Method m: methods)
            sb.append("  ").append(methodToString(m)).append("\n");

        Util.forAllComponentTypes(clazz, (cl, prefix) -> {
            Method[] meths=Util.getAllDeclaredMethodsWithAnnotations(cl, ManagedOperation.class);
            for(Method m: meths)
                sb.append("  ").append(prefix).append(".").append(methodToString(m)).append("\n");
        });
        return sb.toString();
    }

    /**
     * Invokes an operation and puts the return value into map
     * @param map
     * @param operation Protocol.OperationName[args], e.g. STABLE.foo[arg1 arg2 arg3]
     * @param target The target object on which to invoke the operation
     */
    public static void invokeOperation(Map<String,String> map, String operation, Object target) throws Exception {
        if(target == null)
            throw new IllegalArgumentException("target object must not be null");
        int args_index=operation.indexOf('[');
        String method_name;
        if(args_index != -1)
            method_name=operation.substring(0, args_index).trim();
        else
            method_name=operation.trim();

        String[] args=null;
        if(args_index != -1) {
            int end_index=operation.indexOf(']');
            if(end_index == -1)
                throw new IllegalArgumentException("] not found");
            List<String> str_args=Util.parseCommaDelimitedStrings(operation.substring(args_index + 1, end_index));
            Object[] strings=str_args.toArray();
            args=new String[strings.length];
            for(int i=0; i < strings.length; i++)
                args[i]=(String)strings[i];
        }

        Ref<Object> t=new Ref<>(target);
        Ref<Method> method=new Ref<>(Util.findMethod(target.getClass(), method_name, args));
        if(!method.isSet()) {
            final String[] arguments=args;
            // check if any of the components in this class (if it has any) has the method
            Util.forAllComponents(target, (o,prefix) -> {
                if(!method.isSet() && method_name.startsWith(prefix + ".")) {
                    String m=method_name.substring(prefix.length() +1);
                    try {
                        Method meth=Util.findMethod(o.getClass(), m, arguments);
                        if(meth != null) {
                            method.set(meth);
                            t.set(o);
                        }
                    }
                    catch(Exception e) {
                    }
                }
            });
        }
        if(!method.isSet())
            throw new IllegalArgumentException(String.format("method %s not found in %s", method_name,
                                                             target.getClass().getSimpleName()));
        Object retval=invoke(method.get(), t.get(), args);
        if(retval != null)
            map.put(target.getClass().getSimpleName() + "." + method_name, retval.toString());
    }

    public static Object invoke(Method method, Object target, String[] args) throws Exception {
        MethodCall call=new MethodCall(method);
        Object[] converted_args=null;
        if(args != null) {
            converted_args=new Object[args.length];
            Class<?>[] types=method.getParameterTypes();
            for(int i=0; i < args.length; i++)
                converted_args[i]=Util.convert(args[i], types[i]);
        }
        return call.invoke(target, converted_args);
    }


    protected static void convert(Map<String,Map<String,Object>> in, Map<String,String> out) {
        if(in != null)
            in.entrySet().stream().filter(e -> e.getValue() != null).forEach(e -> out.put(e.getKey(), e.getValue().toString()));
    }


    public static void handleAttrWrite(Object target, String attr_name, String attr_value) throws Exception {
        Ref<Exception> ex=new Ref<>(null);
        // first try attributes at the protocol level
        try {
            _handleAttrWrite(target, attr_name, attr_value);
            return;
        }
        catch(Exception e) {
            ex.set(e);
        }

        // if none are found, try components next
        Util.forAllComponents(target, (comp,prefix) -> {
            if(attr_name.startsWith(prefix + ".")) {
                String actual_attrname=attr_name.substring(prefix.length()+1);
                try {
                    _handleAttrWrite(comp, actual_attrname, attr_value);
                    ex.set(null);
                }
                catch(Exception e) {
                    ex.set(e);
                }
            }
        });

        if(ex.isSet())
            throw ex.get();
    }

    public static void _handleAttrWrite(Object target, String attr_name, String attr_value) {

        // 1. Try to find a field first
        Exception e1=null, e2=null;
        Field field=Util.getField(target.getClass(), attr_name);
        if(field != null) {
            Object value=Util.convert(attr_value, field.getType());
            if(value != null) {
                try {
                    Util.setField(field, target, value);
                    return;
                }
                catch(Exception ex) {
                    e1=ex;
                }
            }
        }

        // 2. Try to find a setter for X, e.g. x(type-of-x) or setX(type-of-x)
        ResourceDMBean.Accessor setter=ResourceDMBean.findSetter(target, attr_name);
        if(setter != null) {
            try {
                Class<?> type=setter instanceof ResourceDMBean.FieldAccessor?
                  ((ResourceDMBean.FieldAccessor)setter).getField().getType() :
                  setter instanceof ResourceDMBean.MethodAccessor?
                    ((ResourceDMBean.MethodAccessor)setter).getMethod().getParameterTypes()[0] : null;
                Object converted_value=Util.convert(attr_value, type);
                setter.invoke(converted_value);
                return;
            }
            catch(Exception ex) {
                e2=ex;
            }
        }

        // at this point, we could neither find a field (and set it successfully) nor find a setter
        // (and invoke it successfully)

        String s=String.format("failed setting %s to %s: %s", attr_name, attr_value,
                               (e1 != null || e2 != null)? e1 + " " + e2 : "field or setter not found");
        throw new IllegalArgumentException(s);
    }


    protected static String methodToString(Method m) {
        StringBuilder sb=new StringBuilder(m.getName());
        sb.append('(');
        StringJoiner sj = new StringJoiner(",");
        for(Class<?> parameterType : m.getParameterTypes())
            sj.add(parameterType.getTypeName());
        sb.append(sj);
        sb.append(')');
        return sb.toString();
    }

}
