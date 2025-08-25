package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.RTTHeader;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Keeps track of stats for sync and async unicasts and multicasts
 * @author Bela Ban
 * @since  3.6.8
 */
public class RpcStats {
    protected final LongAdder               sync_unicasts=new LongAdder();
    protected final LongAdder               async_unicasts=new LongAdder();
    protected final LongAdder               sync_multicasts=new LongAdder();
    protected final LongAdder               async_multicasts=new LongAdder();
    protected final LongAdder               sync_anycasts=new LongAdder();
    protected final LongAdder               async_anycasts=new LongAdder();
    protected volatile Map<Address,Result>  stats;
    protected volatile Map<Address,RTTStat> rtt_stats;

    protected static final Function<Address,RTTStat> FUNC=__ -> new RTTStat();
    protected static final Function<Address,Result> FUNC2=__ -> new Result();

    public enum Type {MULTICAST, UNICAST, ANYCAST}

    public RpcStats(boolean extended_stats) {
        extendedStats(extended_stats);
    }

    public long unicasts(boolean sync)    {return sync? sync_unicasts.sum()   : async_unicasts.sum();}
    public long multicasts(boolean sync)  {return sync? sync_multicasts.sum() : async_multicasts.sum();}
    public long anycasts(boolean sync)    {return sync? sync_anycasts.sum()   : async_anycasts.sum();}

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
        if(rtt_stats != null)
            rtt_stats.clear();
        for(LongAdder a: List.of(sync_unicasts, async_unicasts, sync_multicasts, async_multicasts, sync_anycasts, async_anycasts))
            a.reset();
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

    public void addRTTStats(Address sender, RTTHeader hdr) {
        if(hdr == null)
            return;
        if(this.rtt_stats == null)
            this.rtt_stats=new ConcurrentHashMap<>();
        Address key=sender == null? Global.NULL_ADDRESS : sender;
        RTTStat rtt_stat=rtt_stats.computeIfAbsent(key, FUNC);
        rtt_stat.add(hdr);
    }

    public void retainAll(Collection<Address> members) {
        Map<Address,Result> map;
        if(members == null || (map=stats) == null)
            return;
        map.keySet().retainAll(members);
        if(rtt_stats != null)
            rtt_stats.keySet().retainAll(members);
    }


    public String printStatsByDest() {
        if(stats == null) return "(no stats)";
        StringBuilder sb=new StringBuilder("\n");
        for(Map.Entry<Address,Result> entry: stats.entrySet()) {
            Address dst=entry.getKey();
            sb.append(String.format("%s: %s\n", dst == Global.NULL_ADDRESS? "<all>" : dst, entry.getValue()));
        }
        return sb.toString();
    }


    public String printRTTStatsByDest() {
        if(rtt_stats == null) return "(no RTT stats)";
        StringBuilder sb=new StringBuilder("\n");
        for(Map.Entry<Address,RTTStat> entry: rtt_stats.entrySet()) {
            Address dst=entry.getKey();
            sb.append(String.format("%s:\n%s\n", dst == Global.NULL_ADDRESS? "<all>" : dst, entry.getValue()));
        }
        return sb.toString();
    }

    public String toString() {
        return String.format("sync mcasts: %d, async mcasts: %d, sync ucasts: %d, async ucasts: %d, sync acasts: %d, async acasts: %d",
                             sync_multicasts.sum(), async_multicasts.sum(), sync_unicasts.sum(), async_unicasts.sum(),
                             sync_anycasts.sum(), async_anycasts.sum());
    }

    protected void update(Type type, boolean sync) {
        switch(type) {
            case MULTICAST:
                if(sync)
                    sync_multicasts.increment();
                else
                    async_multicasts.increment();
                break;
            case UNICAST:
                if(sync)
                    sync_unicasts.increment();
                else
                    async_unicasts.increment();
                break;
            case ANYCAST:
                if(sync)
                    sync_anycasts.increment();
                else
                    async_anycasts.increment();
                break;
        }
    }

    protected void addToResults(Address dest, boolean sync, long time) {
        Map<Address,Result> map=stats;
        if(map == null)
            return;
        if(dest == null)
            dest=Global.NULL_ADDRESS;
        Result res=map.computeIfAbsent(dest, FUNC2);
        res.add(sync, time);
    }


    protected static class Result {
        protected long                sync, async;
        protected final AverageMinMax avg=new AverageMinMax().unit(NANOSECONDS);
        protected long                sync()  {return sync;}
        protected long                async() {return async;}
        protected double              min()   {return avg.min();}
        protected double              max()   {return avg.max();}
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
            return String.format("async: %,d, sync: %,d, round-trip %s", async, sync, avg);
        }
    }

    protected static class RTTStat {
        // RTT for a sync request
        protected final Average total_time=new AverageMinMax().unit(NANOSECONDS);
        // send until the req is serialized
        protected final Average down_req_time=new AverageMinMax().unit(NANOSECONDS);
        // serialization of req until deserialization
        protected final Average network_req_time=new AverageMinMax().unit(NANOSECONDS);
        // serialization of rsp until deserialization
        protected final Average network_rsp_time=new AverageMinMax().unit(NANOSECONDS);
        // deserialization of req until dispatching to app
        protected final Average req_up_time=new AverageMinMax().unit(NANOSECONDS);
        // deserialization of rsp until after dispatching to app
        protected final Average rsp_up_time=new AverageMinMax().unit(NANOSECONDS);
        // time between reception of req and sending of rsp
        protected final Average processing_time=new AverageMinMax().unit(NANOSECONDS);

        protected void add(RTTHeader hdr) {
            if(hdr == null)
                return;
            long tmp;
            if((tmp=hdr.totalTime()) > 0)
                total_time.add(tmp);
            if((tmp=hdr.downRequest()) > 0)
                down_req_time.add(tmp);
            if((tmp=hdr.networkRequest()) > 0)
                network_req_time.add(tmp);
            if((tmp=hdr.networkResponse()) > 0)
                network_rsp_time.add(tmp);
            if((tmp=hdr.upReq()) > 0)
                req_up_time.add(tmp);
            if((tmp=hdr.processingTime()) > 0)
                processing_time.add(tmp);
            if((tmp=hdr.upRsp()) > 0)
                rsp_up_time.add(tmp);
        }

        public String toString() {
            return String.format("""
                                     total: %s
                                     down-req: %s
                                     network req: %s
                                     network rsp: %s
                                     up-req: %s
                                     up-rsp: %s
                                     processing time: %s
                                   """,
                                 total_time.toString(NANOSECONDS),
                                 down_req_time.toString(NANOSECONDS),
                                 network_req_time.toString(NANOSECONDS), network_rsp_time.toString(NANOSECONDS),
                                 req_up_time.toString(NANOSECONDS), rsp_up_time.toString(NANOSECONDS),
                                 processing_time.toString(NANOSECONDS));
        }
    }
}
