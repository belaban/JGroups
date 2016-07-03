package org.jgroups.util;

import org.jgroups.stack.DiagnosticsHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Wraps {@link AverageMinMax} and provides an impl of {@link org.jgroups.stack.DiagnosticsHandler.ProbeHandler}.
 * Note that this class in unsynchronized and needs external synchronization.
 * @author Bela Ban
 * @since  4.0
 */
public class Profiler implements DiagnosticsHandler.ProbeHandler {
    protected final AverageMinMax avg=new AverageMinMax();
    protected final String        name;
    protected final TimeUnit      unit;

    /**
     * Creates a Profiler instance which will reply to key 'name'.
     * @param name The name under which the profiler will register itself
     * @param unit The unit of measurement - only nanos, micros and millis are supported
     */
    public Profiler(String name, TimeUnit unit) {
        this.name=name;
        this.unit=unit;
    }

    public Profiler add(long time) {avg.add(time); return this;}
    public long     min()          {return avg.min();}
    public long     max()          {return avg.max();}
    public double   average()      {return avg.average();}
    public long     count()        {return avg.count();}
    public Profiler clear()        {avg.clear(); return this;}

    public String   toString()     {
        return String.format("min/avg/max %s = %d / %.2f / %d",
                             toString(unit), avg.min(), avg.average(), avg.max());
    }

    public Map<String,String> handleProbe(String... keys) {
        Map<String,String> map=null;
        for(String key: keys) {
            if(Objects.equals(key, name + ".reset"))
                avg.clear();
            else if(Objects.equals(key, name)) {
                if(map == null) map=new HashMap<>();
                map.put(name, String.format("cnt: min/avg/max = %d: %d / %.2f / %d %s",
                                            avg.count(), avg.min(), avg.average(), avg.max(), toString(unit)));
            }
        }
        return map;
    }

    public String[] supportedKeys() {return new String[]{name, name + ".reset"};}

    protected static String toString(TimeUnit unit) {
        switch(unit) {
            case SECONDS:      return "s";
            case MILLISECONDS: return "ms";
            case MICROSECONDS: return "us";
            case NANOSECONDS:  return "ns";
        }
        return unit.toString();
    }
}
