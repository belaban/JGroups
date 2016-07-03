package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps track of stats for sync and async unicasts and multicasts
 * @author Bela Ban
 * @since  3.6.8
 */
public class RpcStats {
    protected final AtomicInteger                    sync_unicasts=new AtomicInteger(0);
    protected final AtomicInteger                    async_unicasts=new AtomicInteger(0);
    protected final AtomicInteger                    sync_multicasts=new AtomicInteger(0);
    protected final AtomicInteger                    async_multicasts=new AtomicInteger(0);
    protected final AtomicInteger                    sync_anycasts=new AtomicInteger(0);
    protected final AtomicInteger                    async_anycasts=new AtomicInteger(0);
    protected volatile ConcurrentMap<Address,Result> stats;

    public enum Type {MULTICAST, UNICAST, ANYCAST}

    public RpcStats(boolean extended_stats) {
        extendedStats(extended_stats);
    }

    public int unicasts(boolean sync)    {return sync? sync_unicasts.get()   : async_unicasts.get();}
    public int multicasts(boolean sync)  {return sync? sync_multicasts.get() : async_multicasts.get();}
    public int anycasts(boolean sync)    {return sync? sync_anycasts.get()   : async_anycasts.get();}

    public boolean  extendedStats()          {return stats != null;}
    public RpcStats extendedStats(boolean f) {
        if(f) {
            if(stats == null)
                stats=new ConcurrentHashMap<>();
        }
        else
            stats=null;
        return this;
    }

    public void reset() {
        if(stats != null)
            stats.clear();
        for(AtomicInteger ai: Arrays.asList(sync_unicasts, async_unicasts, sync_multicasts, async_multicasts, sync_anycasts, async_anycasts))
            ai.set(0);
    }

    public void add(Type type, Address dest, boolean sync, long time) {
        update(type, sync);
        addToResults(dest, sync, time);
    }

    public void addAnycast(boolean sync, long time, Collection<Address> dests) {
        update(Type.ANYCAST, sync);
        if(dests != null)
            for(Address dest: dests)
                addToResults(dest, sync, time);
    }

    public void retainAll(Collection<Address> members) {
        ConcurrentMap<Address,Result> map;
        if(members == null || (map=stats) == null)
            return;
        map.keySet().retainAll(members);
    }


    public String printOrderByDest() {
        if(stats == null) return "(no stats)";
        StringBuilder sb=new StringBuilder("\n");
        for(Map.Entry<Address,Result> entry: stats.entrySet()) {
            Address dst=entry.getKey();
            sb.append(String.format("%s: %s\n", dst == Global.NULL_ADDRESS? "<all>" : dst, entry.getValue()));
        }
        return sb.toString();
    }

    public String toString() {
        return String.format("sync mcasts: %d, async mcasts: %d, sync ucasts: %d, async ucasts: %d, sync acasts: %d, async acasts: %d",
                             sync_multicasts.get(), async_multicasts.get(), sync_unicasts.get(), async_unicasts.get(),
                             sync_anycasts.get(), async_anycasts.get());
    }

    protected void update(Type type, boolean sync) {
        switch(type) {
            case MULTICAST:
                if(sync)
                    sync_multicasts.incrementAndGet();
                else
                    async_multicasts.incrementAndGet();
                break;
            case UNICAST:
                if(sync)
                    sync_unicasts.incrementAndGet();
                else
                    async_unicasts.incrementAndGet();
                break;
            case ANYCAST:
                if(sync)
                    sync_anycasts.incrementAndGet();
                else
                    async_anycasts.incrementAndGet();
                break;
        }
    }

    protected void addToResults(Address dest, boolean sync, long time) {
        ConcurrentMap<Address,Result> map=stats;
        if(map == null)
            return;
        if(dest == null) dest=Global.NULL_ADDRESS;
        Result res=map.get(dest);
        if(res == null) {
            Result tmp=map.putIfAbsent(dest, res=new Result());
            if(tmp != null)
                res=tmp;
        }
        res.add(sync, time);
    }


    protected static class Result {
        protected long                sync, async;
        protected final AverageMinMax avg=new AverageMinMax();
        protected long                sync()  {return sync;}
        protected long                async() {return async;}
        protected long                min()   {return avg.min();}
        protected long                max()   {return avg.max();}
        protected synchronized double avg()   {return avg.average();}

        protected synchronized void add(boolean sync, long time) {
            if(sync)
                this.sync++;
            else
                this.async++;
            if(time > 0)
                avg.add(time);
        }

        public String toString() {
            double avg_us=avg()/1000.0;     // convert nanos to microsecs
            double min_us=avg.min()/1000.0; // us
            double max_us=avg.max()/1000.0; // us
            return String.format("async: %d, sync: %d, round-trip min/avg/max (us): %.2f / %.2f / %.2f", async, sync, min_us, avg_us, max_us);
        }
    }
}
