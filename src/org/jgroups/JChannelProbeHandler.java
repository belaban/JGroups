package org.jgroups;

import org.jgroups.blocks.MethodCall;
import org.jgroups.jmx.AdditionalJmxObjects;
import org.jgroups.jmx.ResourceDMBean;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class JChannelProbeHandler implements DiagnosticsHandler.ProbeHandler {
    protected final JChannel      ch;
    protected final Log           log;

    public JChannelProbeHandler(JChannel ch) {
        this.ch=ch;
        log=LogFactory.getLog(ch.getClass());
    }

    public Map<String, String> handleProbe(String... keys) {
        Map<String,String> map=new TreeMap<>();
        for(String key : keys) {
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
                        handleOperation(map, key.substring(index + 1));
                    }
                    catch(Throwable throwable) {
                        log.error(Util.getMessage("OperationInvocationFailure"), key.substring(index + 1), throwable);
                    }
                }
                continue;
            }
            if(key.startsWith("threads")) {
                ThreadMXBean bean=ManagementFactory.getThreadMXBean();
                boolean cpu_supported=bean.isThreadCpuTimeSupported();
                boolean contention_supported=bean.isThreadContentionMonitoringSupported();
                int max_name=0;
                long[] ids=bean.getAllThreadIds();
                List<ThreadEntry> entries=new ArrayList<>(ids.length);

                for(long id : ids) {
                    ThreadInfo info=bean.getThreadInfo(id);
                    if(info == null) continue;
                    String thread_name=info.getThreadName();
                    max_name=Math.max(max_name, thread_name.length());
                    Thread.State state=info.getThreadState();
                    long blocked=info.getBlockedCount();
                    long blocked_time=contention_supported? info.getBlockedTime() : -1;
                    long waited=info.getWaitedCount();
                    long waited_time=contention_supported? info.getWaitedTime() : -1;
                    double cpu_time=cpu_supported? bean.getThreadCpuTime(id) : -1;
                    if(cpu_time > 0)
                        cpu_time/=1_000_000;
                    double user_time=cpu_supported? bean.getThreadUserTime(id) : -1;
                    if(user_time > 0)
                        user_time/=1_000_000;

                    ThreadEntry entry=new ThreadEntry(state, thread_name, blocked, waited, blocked_time, waited_time,
                                                      cpu_time, user_time);
                    entries.add(entry);
                }

                int index=key.indexOf('=');
                if(index >= 0) {
                    Comparator<ThreadEntry> comp=Comparator.comparing(e -> e.thread_name);
                    String val=key.substring(index+1);
                    if(val.startsWith("state"))
                        comp=Comparator.comparing(e -> e.state);
                    else if(val.startsWith("cpu"))
                        comp=Comparator.comparing((ThreadEntry e) -> e.cpu_time).reversed();
                    else if(val.startsWith("user"))
                        comp=Comparator.comparing((ThreadEntry e) -> e.user_time).reversed();
                    else if(val.startsWith("block"))
                        comp=Comparator.comparing((ThreadEntry e) -> e.blocks).reversed();
                    else if(val.startsWith("btime"))
                        comp=Comparator.comparing((ThreadEntry e) -> e.block_time).reversed();
                    else if(val.startsWith("wait"))
                        comp=Comparator.comparing((ThreadEntry e) -> e.waits).reversed();
                    else if(val.startsWith("wtime"))
                        comp=Comparator.comparing((ThreadEntry e) -> e.wait_time).reversed();
                    entries.sort(comp);
                }

                // see if we need to limit the displayed data
                index=key.indexOf('=', index+1);
                int limit=0;
                if(index >= 0) {
                    String val=key.substring(index+1);
                    limit=Integer.valueOf(val);
                }


                max_name=Math.min(max_name, 50)+1;
                String title="\n[%s]   \t%-" + max_name+"s: %10s %10s %6s %9s %10s %10s\n";
                String line="[%s]\t%-"+max_name+"s: %,8.0f %,8.0f %,10d %,9.0f %,10d %,10.0f\n";

                StringBuilder sb=new StringBuilder(String.format(title,
                                                                 "state", "thread-name", "cpu (ms)", "user (ms)",
                                                                 "block", "btime (ms)", "wait", "wtime (ms)"));
                Stream<ThreadEntry> stream=entries.stream();
                if(limit > 0)
                    stream=stream.limit(limit);
                stream.forEach(e -> sb.append(e.print(line)));
                map.put(key, sb.toString());
                continue;
            }
            if(key.equals("enable-cpu")) {
                map.put(key, enable(1, true));
                continue;
            }
            if(key.startsWith("enable-cont")) {
                map.put(key, enable(2, true));
                continue;
            }
            if(key.equals("disable-cpu")) {
                map.put(key, enable(1, false));
                continue;
            }
            if(key.startsWith("disable-cont")) {
                map.put(key, enable(2, false));
            }
        }
        return map;
    }

    public String[] supportedKeys() {
        return new String[]{"reset-stats", "jmx", "op=<operation>[<args>]",
          "threads[=<filter>[=<limit>]]", "enable-cpu", "enable-contention", "disable-cpu", "disable-contention"};
    }


    protected static String enable(int type, boolean flag) {
        ThreadMXBean bean=ManagementFactory.getThreadMXBean();
        boolean supported=false;
        if(type == 1) { // cpu
            supported=bean.isThreadCpuTimeSupported();
            if(supported)
                bean.setThreadCpuTimeEnabled(flag);
        }
        else if(type == 2) {
            supported=bean.isThreadContentionMonitoringSupported();
            if(supported)
                bean.setThreadContentionMonitoringEnabled(flag);
        }
        String tmp=type == 1? "CPU" : "contention";
        return String.format("%s monitoring supported: %b, %s monitoring enabled: %b", tmp, supported, tmp, supported && flag);
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

   protected static class ThreadEntry {
       protected final Thread.State state;
       protected final String       thread_name;
       protected final long         blocks, waits;
       protected final double       block_time, wait_time;  // ms
       protected final double       cpu_time, user_time;    // ms

       public ThreadEntry(Thread.State state, String thread_name, long blocks, long waits, double block_time, double wait_time,
                          double cpu_time, double user_time) {
           this.state=state;
           this.thread_name=thread_name;
           this.blocks=blocks;
           this.waits=waits;
           this.block_time=block_time;
           this.wait_time=wait_time;
           this.cpu_time=cpu_time;
           this.user_time=user_time;
       }

       public String toString() {
           StringBuilder sb=new StringBuilder(String.format("[%s] %s:", state, thread_name));
           sb.append(String.format(" blocks=%d (%.2f ms) waits=%d (%.2f ms)", blocks, block_time, waits, wait_time));
           sb.append(String.format(" sys=%.2f ms user=%.2f ms\n", cpu_time, user_time));
           return sb.toString();
       }

       protected String print(String format) {
           return String.format(format, state, thread_name, cpu_time, user_time, blocks, block_time, waits, wait_time);
       }


   }

}
