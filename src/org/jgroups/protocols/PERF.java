package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Average;
import org.jgroups.util.MessageBatch;

import java.util.concurrent.TimeUnit;

/**
 * Protocol measuring latency between stacks. On {@link #down(org.jgroups.Message)}, a header is added to the
 * message with the ID of the PERF protocol, and the start time is set in the header.
 * On {@link #up(org.jgroups.Message)}, the time difference is computed and a rolling average is updated in PERF.<br/>
 * Note that we can have several measurements by inserting PERF protocols with different IDs (Protocol.id) into the stack.
 * </br>
 * If PERF is used to measure latency between nodes running on different physical boxes, it is important that the clocks
 * are synchronized, or else latency cannot be computed correctly (and may even be negative).
 * @author Bela Ban
 * @since  3.5
 */
@MBean(description="Measures latency between PERF instances")
public class PERF extends Protocol {
    protected Average avg;

    @Property(description="Number of samples to maintain for rolling average")
    protected int     avg_size=1024;

    @ManagedAttribute(description="Average latency in ns",type=AttributeType.TIME,unit=TimeUnit.NANOSECONDS)
    public double latencyInNs() {return avg.average();}

    @ManagedAttribute(description="Average latency in ms",type=AttributeType.TIME,unit=TimeUnit.MILLISECONDS)
    public double latencyInMs() {return avg.average() / 1000000.0;}

    public void init() throws Exception {
        super.init();
        avg=new Average(avg_size);
    }

    public void resetStats() {
        super.resetStats();
        avg.clear();
    }

    public Object down(Message msg) {
        msg.putHeader(id, new PerfHeader(System.nanoTime()));
        return down_prot.down(msg);
    }

    public Object up(Message msg) {
        PerfHeader hdr=msg.getHeader(id);
        if(hdr == null)
            log.error("%s: no perf header found", local_addr);
        else {
            long time=System.nanoTime() - hdr.start_time;
            if(time <= 0)
                log.error("%d: time is <= 0", time);
            else
                avg.add(time);
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            PerfHeader hdr=msg.getHeader(id);
            if(hdr == null)
                log.error("%s: no perf header found", local_addr);
            else {
                long time=System.nanoTime() - hdr.start_time;
                if(time <= 0)
                    log.error("%d: time is <= 0", time);
                else
                    avg.add(time);
            }
        }

        super.up(batch);
    }
}
