package org.jgroups;

import org.jgroups.blocks.MethodCall;
import org.jgroups.jmx.AdditionalJmxObjects;
import org.jgroups.jmx.ResourceDMBean;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class JChannelProbeHandler implements DiagnosticsHandler.ProbeHandler {
    protected final JChannel ch;
    protected final Log log;

    public JChannelProbeHandler(JChannel ch) {
        this.ch=ch;
        log=LogFactory.getLog(ch.getClass());
    }

    public Map<String, String> handleProbe(String... keys) {
        Map<String, String> map=new TreeMap<>();
        for(String key: keys) {
            if(key.startsWith("jmx")) {
                handleJmx(map, key);
                continue;
            }
            if(key.startsWith("reset-stats")) {
                resetAllStats();
                continue;
            }
            if(key.startsWith("invoke") || key.startsWith("op")) {
                int index=key.indexOf('=');
                if(index != -1) {
                    try {
                        handleOperation(map, key.substring(index+1));
                    }
                    catch(Throwable throwable) {
                        log.error(Util.getMessage("OperationInvocationFailure"), key.substring(index+1), throwable);
                    }
                }
            }
        }
        return map;
    }

    public String[] supportedKeys() {
        return new String[]{"reset-stats", "jmx", "op=<operation>[<args>]"};
    }

    protected JChannel resetAllStats() {
        List<Protocol> prots=ch.getProtocolStack().getProtocols();
        prots.forEach(Protocol::resetStatistics);
        return ch.resetStats();
    }

    protected void handleJmx(Map<String, String> map, String input) {
        Map<String, Object> tmp_stats;
        int index=input.indexOf('=');
        if(index > -1) {
            List<String> list=null;
            String protocol_name=input.substring(index +1);
            index=protocol_name.indexOf('.');
            if(index > -1) {
                String rest=protocol_name;
                protocol_name=protocol_name.substring(0, index);
                String attrs=rest.substring(index +1); // e.g. "num_sent,msgs,num_received_msgs"
                list=Util.parseStringList(attrs, ",");

                // check if there are any attribute-sets in the list
                for(Iterator<String> it=list.iterator(); it.hasNext();) {
                    String tmp=it.next();
                    index=tmp.indexOf('=');
                    if(index != -1) {
                        String attrname=tmp.substring(0, index);
                        String attrvalue=tmp.substring(index+1);
                        Object target=ch.getProtocolStack().findProtocol(protocol_name);
                        Field field=target != null? Util.getField(target.getClass(), attrname) : null;
                        if(field == null && target instanceof AdditionalJmxObjects) {
                            Object[] objs=((AdditionalJmxObjects)target).getJmxObjects();
                            if(objs != null && objs.length > 0) {
                                for(Object o: objs) {
                                    field=o != null? Util.getField(o.getClass(), attrname) : null;
                                    if(field != null) {
                                        target=o;
                                        break;
                                    }
                                }
                            }
                        }

                        if(field != null) {
                            Object value=Util.convert(attrvalue, field.getType());
                            if(value != null) {
                                if(target instanceof Protocol)
                                    ((Protocol)target).setValue(attrname, value);
                                else
                                    Util.setField(field, target, value);
                            }
                        }
                        else {
                            // try to find a setter for X, e.g. x(type-of-x) or setX(type-of-x)
                            ResourceDMBean.Accessor setter=ResourceDMBean.findSetter(target, attrname);  // Util.getSetter(prot.getClass(), attrname);
                            if(setter == null && target instanceof AdditionalJmxObjects) {
                                Object[] objs=((AdditionalJmxObjects)target).getJmxObjects();
                                if(objs != null && objs.length > 0) {
                                    for(Object o: objs) {
                                        setter=o != null? ResourceDMBean.findSetter(target, attrname) : null;
                                        if(setter!= null)
                                            break;
                                    }
                                }
                            }

                            if(setter != null) {
                                try {
                                    Class<?> type=setter instanceof ResourceDMBean.FieldAccessor?
                                      ((ResourceDMBean.FieldAccessor)setter).getField().getType() :
                                      setter instanceof ResourceDMBean.MethodAccessor?
                                        ((ResourceDMBean.MethodAccessor)setter).getMethod().getParameterTypes()[0] : null;
                                    Object converted_value=Util.convert(attrvalue, type);
                                    setter.invoke(converted_value);
                                }
                                catch(Exception e) {
                                    log.error("unable to invoke %s() on %s: %s", setter, protocol_name, e);
                                }
                            }
                            else
                                log.warn(Util.getMessage("FieldNotFound"), attrname, protocol_name);
                        }

                        it.remove();
                    }
                }
            }
            tmp_stats=ch.dumpStats(protocol_name, list);
            if(tmp_stats != null) {
                for(Map.Entry<String,Object> entry : tmp_stats.entrySet()) {
                    Map<String,Object> tmp_map=(Map<String,Object>)entry.getValue();
                    String key=entry.getKey();
                    map.put(key, tmp_map != null? tmp_map.toString() : null);
                }
            }
        }
        else {
            tmp_stats=ch.dumpStats();
            if(tmp_stats != null) {
                for(Map.Entry<String,Object> entry : tmp_stats.entrySet()) {
                    Map<String,Object> tmp_map=(Map<String,Object>)entry.getValue();
                    String key=entry.getKey();
                    map.put(key, tmp_map != null? tmp_map.toString() : null);
                }
            }
        }
    }

    /**
     * Invokes an operation and puts the return value into map
     * @param map
     * @param operation Protocol.OperationName[args], e.g. STABLE.foo[arg1 arg2 arg3]
     */
    protected void handleOperation(Map<String, String> map, String operation) throws Exception {
        int index=operation.indexOf('.');
        if(index == -1)
            throw new IllegalArgumentException("operation " + operation + " is missing the protocol name");
        String prot_name=operation.substring(0, index);
        Protocol prot=ch.getProtocolStack().findProtocol(prot_name);
        if(prot == null)
            return; // less drastic than throwing an exception...


        int args_index=operation.indexOf('[');
        String method_name;
        if(args_index != -1)
            method_name=operation.substring(index +1, args_index).trim();
        else
            method_name=operation.substring(index+1).trim();

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

        Object target=prot;
        Method method=MethodCall.findMethod(target.getClass(), method_name, args);
        if(method == null) {
            if(prot instanceof AdditionalJmxObjects) {
                for(Object obj: ((AdditionalJmxObjects)prot).getJmxObjects()) {
                    method=MethodCall.findMethod(obj.getClass(), method_name, args);
                    if(method != null) {
                        target=obj;
                        break;
                    }
                }
            }
            if(method == null) {
                log.warn(Util.getMessage("MethodNotFound"), ch.getAddress(), target.getClass().getSimpleName(), method_name);
                return;
            }
        }

        MethodCall call=new MethodCall(method);
        Object[] converted_args=null;
        if(args != null) {
            converted_args=new Object[args.length];
            Class<?>[] types=method.getParameterTypes();
            for(int i=0; i < args.length; i++)
                converted_args[i]=Util.convert(args[i], types[i]);
        }
        Object retval=call.invoke(target, converted_args);
        if(retval != null)
            map.put(prot_name + "." + method_name, retval.toString());
    }

}
