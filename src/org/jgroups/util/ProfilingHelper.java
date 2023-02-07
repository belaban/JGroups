package org.jgroups.util;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.stack.DiagnosticsHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generic helper for profiling of methods. Other helper classes can subclass this one
 * @author Bela Ban
 * @since  5.2.7
 */
public class ProfilingHelper extends Helper {
    protected ProfilingHelper(Rule rule) {
        super(rule);
    }

    @SuppressWarnings("StaticCollection")
    protected static final Map<String,Profiler> profilers=new ConcurrentHashMap<>();

    protected static final ProfilingProbeHandler ph=new ProfilingProbeHandler();


    @SuppressWarnings("MethodMayBeStatic")
    public void diagCreated(DiagnosticsHandler diag) {
        if(diag != null && diag.isEnabled()) {
            boolean already_present=diag.getProbeHandlers().contains(ph);
            if(!already_present)
                diag.registerProbeHandler(ph);
        }
    }


    @SuppressWarnings("MethodMayBeStatic")
    public void start(String profiler_name) {
        Profiler p=profilers.computeIfAbsent(profiler_name, n -> new Profiler());
        p.start();
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void stop(String profiler_name) {
        Profiler p=profilers.computeIfAbsent(profiler_name, n -> new Profiler());
        p.stop();
    }


    protected static class ProfilingProbeHandler implements DiagnosticsHandler.ProbeHandler {

        public ProfilingProbeHandler() {
        }


        public Map<String,String> handleProbe(String... keys) {
            Map<String,String> m=new HashMap<>();
            for(String key: keys) {
                if("prof".equals(key)) {
                    for(Map.Entry<String,Profiler> e: profilers.entrySet())
                        m.put(e.getKey(), e.getValue().toString());
                    continue;
                }
                if("prof-reset".equals(key)) {
                    profilers.clear();
                    continue;
                }
                Profiler p=profilers.get(key);
                if(p != null)
                    m.put(key, p.toString());
            }
            return m;
        }

        public String[] supportedKeys() {
            List<String> keys=new ArrayList<>(profilers.keySet());
            keys.add("prof");
            keys.add("prof-reset");
            return keys.toArray(new String[]{});
        }

    }



}
