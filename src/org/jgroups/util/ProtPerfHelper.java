package org.jgroups.util;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Message;
import org.jgroups.protocols.ProtPerfHeader;
import org.jgroups.protocols.TP;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.jgroups.protocols.ProtPerfHeader.ID;
import static org.jgroups.util.Util.printTime;

public class ProtPerfHelper extends Helper {
    protected ProtPerfHelper(Rule rule) {
        super(rule);
    }

    protected static final ProtPerfProbeHandler ph=new ProtPerfProbeHandler();
    protected static final String               DEFAULT="default";


    @SuppressWarnings("MethodMayBeStatic")
    public void diagCreated(DiagnosticsHandler diag, TP transport) {
        if(diag != null && diag.isEnabled()) {
            boolean already_present=diag.getProbeHandlers().contains(ph);
            if(!already_present) {
                diag.registerProbeHandler(ph);
                ph.addOrdering(transport);
            }
        }
    }


    @SuppressWarnings("MethodMayBeStatic")
    public void downTime(Message msg, Protocol prot) {
        ProtPerfHeader hdr=getOrAddHeader(msg);
        if(prot != null && hdr.startDown() > 0) {
            long time=System.nanoTime() - hdr.startDown(); // ns
            if(time > 0)
                ph.add(getClusterName(prot), prot.getName(), time, true);
        }
        hdr.startDown(System.nanoTime());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void downTime(String cluster, Message msg, String name) {
        ProtPerfHeader hdr=getOrAddHeader(msg);
        if(hdr.startDown() > 0) {
            long time=System.nanoTime() - hdr.startDown(); // ns
            if(time > 0)
                ph.add(cluster, name, time, true);
        }
        hdr.startDown(System.nanoTime());
    }

    public void downTime(String cluster, List<Message> list, String name) {
        for(Message msg: list)
            downTime(cluster, msg, name);
    }


    @SuppressWarnings("MethodMayBeStatic")
    public void downTime(String cluster, String name, long time) {
        ph.add(cluster, name, time, true);
    }



    @SuppressWarnings("MethodMayBeStatic")
    public void upTime(Message msg, Protocol prot) {
        ProtPerfHeader hdr=getOrAddHeader(msg);
        if(prot != null && hdr.startUp() > 0) {
            long time=System.nanoTime() - hdr.startUp(); // ns
            if(time > 0)
                ph.add(getClusterName(prot), prot.getName(), time, false);
        }
        hdr.startUp(System.nanoTime());
    }


    @SuppressWarnings("MethodMayBeStatic")
    public void upTime(MessageBatch batch, Protocol prot) {
        if(prot != null && batch.timestamp() > 0) {
            long time=System.nanoTime() - batch.timestamp(); // ns
            if(time > 0)
                ph.add(getClusterName(prot), prot.getName(), time, false);
        }
        batch.timestamp(System.nanoTime());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setTime(Message msg, boolean down) {
        ProtPerfHeader hdr=getOrAddHeader(msg);
        long time=System.nanoTime();
        if(down)
            hdr.startDown(time);
        else
            hdr.startUp(time);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setTime(Message msg, long time, boolean down) {
        ProtPerfHeader hdr=getOrAddHeader(msg);
        if(down)
            hdr.startDown(time);
        else
            hdr.startUp(time);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setTime(MessageBatch batch) {
        batch.timestamp(System.nanoTime());
    }

    protected static String getClusterName(Protocol p) {
        return p == null? null : p.getTransport().getClusterName();
    }

    protected static ProtPerfHeader getOrAddHeader(Message msg) {
        ProtPerfHeader hdr=msg.getHeader(ID);
        if(hdr != null)
            return hdr;
        msg.putHeader(ID, hdr=new ProtPerfHeader());
        return hdr;
    }


    protected static class ProtPerfProbeHandler implements DiagnosticsHandler.ProbeHandler {
        protected final Map<String,List<String>>      ordering;
        protected final Map<String,Map<String,Entry>> map;

        public ProtPerfProbeHandler() {
            ordering=Util.createConcurrentMap(20);
            map=Util.createConcurrentMap(4);
        }

        public void addOrdering(TP transport) {
            List<Protocol> protocols=transport.getProtocolStack().getProtocols();
            List<String>   prot_names=protocols.stream().map(Protocol::getName).collect(Collectors.toList());
            ordering.putIfAbsent(transport.getClusterName(), prot_names);
        }

        public Map<String,String> handleProbe(String... keys) {
            Map<String,String> m=null;
            for(String key: keys) {
                String value=null;
                String cluster=clusterSuffix(key);
                if(key.startsWith("perf-keys"))
                    value=map.keySet().toString();
                else if(key.startsWith("perf-down-detailed"))
                    value=dumpStats(cluster, true, false, true);
                else if(key.startsWith("perf-down"))
                    value=dumpStats(cluster, true, false, false);
                else if(key.startsWith("perf-up-detailed"))
                    value=dumpStats(cluster, false, true, true);
                else if(key.startsWith("perf-up"))
                    value=dumpStats(cluster, false, true, false);
                else if(key.startsWith("perf-reset"))
                    clearStats();
                else if(key.startsWith("perf"))
                    value=dumpStats(cluster, true, true, true);
                if(value != null) {
                    if(m == null)
                        m=new HashMap<>();
                    m.put(key, value);
                }
            }
            return m;
        }

        public String[] supportedKeys() {
            return new String[]{"perf", "perf-down", "perf-up", "perf-down-detailed", "perf-up-detailed",
              "perf-reset", "perf-help"};
        }

        // perf-down=<clustername>: returns '<clustername>'
        protected static String clusterSuffix(String key) {
            int index=key.indexOf('=');
            if(index < 0) return null;
            return key.substring(index+1);
        }

        protected void add(String cluster, String name, long value, boolean down) {
            if(cluster == null)
                cluster=DEFAULT;
            Map<String,Entry> m=map.computeIfAbsent(cluster, k -> Util.createConcurrentMap(20));
            Entry e=m.computeIfAbsent(name, cl -> new Entry());
            e.add(value, down);
        }

        protected String dumpStats(String cluster, boolean down, boolean up, boolean detailed) {
            if(cluster == null)
                return dumpAllStacks(down, up, detailed);
            Map<String,Entry> m=map.get(cluster);
            return m != null ? dumpStats(cluster, m, down, up, detailed) : String.format("cluster '%s' not found", cluster);
        }

        protected String dumpAllStacks(boolean down, boolean up, boolean detailed) {
            return map.entrySet().stream()
              .map(e -> String.format("%s:\n%s\n", e.getKey(), dumpStats(e.getKey(), e.getValue(), down, up, detailed)))
              .collect(Collectors.joining("\n"));
        }

        protected String dumpStats(String cluster, Map<String,Entry> m, boolean down, boolean up, boolean detailed) {
            String format=String.format("%%-20s | %%%ds", detailed? 25 : 12);
            double avg_down_sum=0, avg_up_sum=0;
            List<String> order=ordering.get(cluster);
            if(order != null) {
                List<String> other_keys=new ArrayList<>(m.keySet());
                other_keys.removeAll(order);
                other_keys.remove(ProtocolStack.class.getSimpleName());
                StringBuilder sb=new StringBuilder("\n");
                for(String name: order) {
                    Entry e=m.get(name);
                    if(e != null) {
                        if(down)
                            avg_down_sum+=e.avg_down.average();
                        else
                            avg_up_sum+=e.avg_up.average();
                    }
                    sb.append(String.format(format, name + ":", e == null? "n/a" : e.toString(down,up,detailed)));
                    sb.append("\n");
                }
                if(!other_keys.isEmpty()) {
                    for(String name: other_keys) {
                        Entry e=m.get(name);
                        if(e != null) {
                            if(down) {
                                if(e.avg_down.count() == 0)
                                    continue;
                                avg_down_sum+=e.avg_down.average();
                            }
                            else {
                                if(e.avg_up.count() == 0)
                                    continue;
                                avg_up_sum+=e.avg_up.average();
                            }
                        }
                        sb.append(String.format(format, "  " + name + ":", e == null? "  n/a" : e.toString(down,up,detailed)));
                        sb.append("\n");
                    }
                }
                sb.append("-".repeat(34)).append("\n");
                sb.append(String.format(format, "TOTAL:",
                                        down? printTime(avg_down_sum, NANOSECONDS) : printTime(avg_up_sum, NANOSECONDS)));
                sb.append("\n");
                return sb.toString();
            }
            else
                return m.entrySet().stream()
                  .map(e -> String.format(format, e.getKey() + ":", e.getValue().toString(down, up, detailed)))
                  .collect(Collectors.joining("\n"));
        }


        protected void clearStats() {
            map.values().forEach(v -> v.values().forEach(Entry::clear));
        }
    }


    protected static class Entry {
        protected final AverageMinMax avg_down=new AverageMinMax().unit(NANOSECONDS);
        protected final AverageMinMax avg_up=new AverageMinMax().unit(NANOSECONDS);

        protected void add(long value, boolean down) {
            if(down)
                avg_down.add(value);
            else
                avg_up.add(value);
        }

        protected void clear() {
            avg_down.clear();
            avg_up.clear();
        }

        public String toString() {
            return String.format("down: %s up: %s", avg_down, avg_up);
        }

        public String toString(boolean down, boolean up, boolean detailed) {
            StringBuilder sb=new StringBuilder();
            if(down)
                sb.append(print(avg_down, detailed));
            if(up) {
                if(down)
                    sb.append(" ");
                sb.append(print(avg_up, detailed));
            }
            return sb.toString();
        }

        public static String print(AverageMinMax avg, boolean detailed) {
            return detailed?
              avg.toString() :
              Util.printTime(avg.average(), NANOSECONDS);
        }
    }
}
