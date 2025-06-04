package org.jgroups.util;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Message;
import org.jgroups.protocols.PerfHeader;
import org.jgroups.stack.DiagnosticsHandler;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Helper for profiling of bundler methods. Other helper classes can subclass this one
 * @author Bela Ban
 * @since  5.2.7
 */
public class BundlerHelper extends Helper {
    protected BundlerHelper(Rule rule) {
        super(rule);
    }

    protected static DiagnosticsHandler        diag_handler;
    protected static final short               PROT=1567; // to get/add PerfHeaders
    protected static final BundlerProbeHandler ph=new BundlerProbeHandler();
    protected static final Map<String,Average> map=new ConcurrentHashMap<>();

    public static void activated() {
        if(diag_handler == null) {
            try {
                diag_handler=createDiagHandler();
                boolean already_present=diag_handler.getProbeHandlers().contains(ph);
                if(!already_present)
                    diag_handler.registerProbeHandler(ph);
                diag_handler.start();
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void diagCreated(DiagnosticsHandler diag) {
        if(diag_handler != null)
            diag_handler.stop();
        diag_handler=diag;
        if(diag != null && diag.isEnabled()) {
            boolean already_present=diag.getProbeHandlers().contains(ph);
            if(!already_present)
                diag.registerProbeHandler(ph);
        }
    }

    public void setStartTime(Message msg) {
        msg.putHeader(PROT, new PerfHeader(System.nanoTime()));
    }

    public void computeTime(String key, Message msg) {
        PerfHeader hdr=msg.getHeader(PROT);
        if(hdr == null) {
            System.err.printf("PerfHeader not found in message %s\n", msg);
            return;
        }
        long time=System.nanoTime() - hdr.startTime();
        addToMap(key, time);
    }

    public void computeTime(String key, List<Message> list) {
        for(Message msg: list)
            computeTime(key, msg);
    }

    public void computeTime(String key, Message[] list) {
        for(Message msg: list)
            computeTime(key, msg);
       }

    protected static void addToMap(String key, long time) {
        Average avg=map.get(key);
        if(avg == null)
            avg=map.computeIfAbsent(key, __ -> new AverageMinMax(1024).unit(TimeUnit.NANOSECONDS));
        avg.add(time);
    }

    protected static DiagnosticsHandler createDiagHandler() throws Exception {
        DiagnosticsHandler ret=new DiagnosticsHandler();
        ret.printHeaders(b -> String.format("%s [ip=%s, %s]\n", ret.getLocalAddress(),
                                            localAddress(),
                                            Util.JAVA_VERSION.isEmpty()? "" : String.format("java %s", Util.JAVA_VERSION)));
        return ret;
    }

    protected static String localAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch(UnknownHostException e) {
            return "n/a";
        }
    }


    protected static class BundlerProbeHandler implements DiagnosticsHandler.ProbeHandler {

        public BundlerProbeHandler() {
        }

        public Map<String,String> handleProbe(String... keys) {
            Map<String,String> m=new HashMap<>();
            for(String key: keys) {
                if("bundler-perf".equals(key)) {
                    for(Map.Entry<String,Average> e: map.entrySet())
                        m.put(e.getKey(), e.getValue().toString());
                    continue;
                }
                if("bundler-perf-reset".equals(key))
                    map.clear();
            }
            return m;
        }

        public String[] supportedKeys() {
            return new String[]{"bundler-perf", "bundler-perf-reset"};
        }
    }

}
