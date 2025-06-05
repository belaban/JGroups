
package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;


/**
 * Protocol which lists how many messages have been received by size: https://issues.redhat.com/browse/JGRP-2893
 * <br/>
 * Don't use this layer in a production stack since the costs are high; only use for debugging/diagnosis.
 * @author Bela Ban June 13 2001
 * @author Bela Ban June 2025
 */
public class SIZE extends Protocol {
    protected final Map<Long,LongAdder> up_map=new ConcurrentHashMap<>();
    protected final Map<Long,LongAdder> down_map=new ConcurrentHashMap<>();

    @ManagedAttribute(description="Number of down samples",type=AttributeType.SCALAR)
    public long numDownSamples() {return count(down_map);}
    @ManagedAttribute(description="Number of up samples",type=AttributeType.SCALAR)
    public long numUpSamples()   {return count(up_map);}

    public Map<Long,LongAdder> upMap()   {return up_map;}
    public Map<Long,LongAdder> downMap() {return down_map;}

    public Object down(Message msg) {
        int len=msg.length();

        String cmd=getInfo(msg, "command");

        addSample(len, down_map);
        return down_prot.down(msg);
    }

    public Object up(Message msg) {
        int len=msg.length();
        addSample(len, up_map);
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            int len=msg.length();
            addSample(len, up_map);
        }
        up_prot.up(batch);
    }

    @ManagedOperation(description="Clears all samples")
    public void clear() {
        down_map.clear(); up_map.clear();
    }

    @ManagedOperation(description="Dumps the down samples")
    public String dumpDownSamples() {
        return "\n" +_dump(down_map);
    }

    @ManagedOperation(description="Dumps the up samples")
    public String dumpUpSamples() {
        return "\n" + _dump(up_map);
    }

    @ManagedOperation(description="Dumps all (down and up) samples")
    public String dump() {
        return String.format("down:\n%s\n\nup:\n%s\n", _dump(down_map), _dump(up_map));
    }

    @Override
    public void resetStats() {
        super.resetStats();
        up_map.values().forEach(LongAdder::reset);
        down_map.values().forEach(LongAdder::reset);
    }

    protected static void addSample(long size, Map<Long,LongAdder> map) {
        LongAdder la=map.get(size);
        if(la == null)
            la=map.computeIfAbsent(size, __ -> new LongAdder());
        la.increment();
    }

    protected static long count(Map<Long,LongAdder> m) {
        long c=0;
        for(LongAdder la: m.values())
            c+=la.sum();
        return c;
    }

    protected static String _dump(Map<Long,LongAdder> m) {
        SortedMap<Long,LongAdder> sm=new ConcurrentSkipListMap<>(m);
        return sm.entrySet().stream().map(e -> String.format("%,d: %,d", e.getKey(), e.getValue().sum()))
          .collect(Collectors.joining("\n"));
    }

    protected static String getInfo(Message msg, String key) {
        InfoHeader hdr=msg.getHeader(InfoHeader.ID);
        return hdr != null? hdr.get(key) : null;
    }
}
