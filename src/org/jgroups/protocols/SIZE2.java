package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.DistributionSampler;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Records number of messages sent/received based on size
 * @author Bela Ban
 * @since  5.4.9
 */
@MBean(description="Records number of messages sent/received based on size")
public class SIZE2 extends Protocol {
    protected DistributionSampler up_sampler, down_sampler;

    @Property(description="List of up sizes (different buckets), e.g. \"1,100,101,500,501,1000,1001,100000\". " +
      "This example creates buckets for messages sizes 1-100, 101-500, 501-1000. The last buckets simply catches " +
      "all other samples, assuming a message can never be greater than  100'000 bytes")
    protected String up_sizes;

    @Property(description="List of down sizes. See description of up_sizes")
    protected String down_sizes;

    @Property(description="When true, use Message.size(), otherwise Message.length()")
    protected boolean use_size;

    @Property(description="Whether to throw an exception when a bucket is missing, or silently drop the samplle")
    protected boolean exception_on_missing_bucket;

    @ManagedAttribute(description="Total number of down samples",type=AttributeType.SCALAR)
    public long numDownSamples() {
        return down_sampler != null? down_sampler.total() : 0;
    }

    @ManagedAttribute(description="Total number of up samples",type=AttributeType.SCALAR)
    public long numUpSamples() {
        return up_sampler != null? up_sampler.total() : 0;
    }

    @Override
    public void init() throws Exception {
        super.init();
        if(up_sizes != null) {
            List<String> list=Util.parseCommaDelimitedStrings(up_sizes);
            List<Long> l=new ArrayList<>(list.size());
            for(String s: list)
                l.add(Long.parseLong(s));
            up_sampler=new DistributionSampler(l).exceptionOnMissingBucket(exception_on_missing_bucket);
        }
        if(down_sizes != null) {
            List<String> list=Util.parseCommaDelimitedStrings(down_sizes);
            List<Long> l=new ArrayList<>(list.size());
            for(String s: list)
                l.add(Long.parseLong(s));
            down_sampler=new DistributionSampler(l).exceptionOnMissingBucket(exception_on_missing_bucket);
        }
    }

    @Override
    public void resetStats() {
        super.resetStats();
        if(down_sampler != null)
            down_sampler.reset();
        if(up_sampler != null)
            up_sampler.reset();
    }

    @ManagedOperation(description="Dumps all down samples")
    public String dumpDownSamples() {
        return down_sampler != null? down_sampler.toString() : "n/a";
    }

    @ManagedOperation(description="Dumps all up samples")
    public String dumpUpSamples() {
        return up_sampler != null? up_sampler.toString() : "n/a";
    }

    @ManagedOperation(description="Dumps all (down and up) samples")
    public String dump() {
        return String.format("down:\n%s\nup:\n%s\n", dumpDownSamples(), dumpUpSamples());
    }

    @Override
    public Object down(Message msg) {
        if(down_sampler != null)
            addSample(msg, down_sampler);
        return down_prot.down(msg);
    }

    @Override
    public CompletableFuture<Object> down(Message msg, boolean async) {
        if(down_sampler != null)
            addSample(msg, down_sampler);
        return down_prot.down(msg, async);
    }

    @Override
    public Object up(Message msg) {
        if(up_sampler != null)
            addSample(msg, up_sampler);
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        if(up_sampler != null) {
            for(Message msg: batch)
                addSample(msg, up_sampler);
        }
        up_prot.up(batch);
    }

    protected void addSample(Message msg, DistributionSampler ds) {
        int sample=use_size? msg.size() : msg.getLength();
        ds.add(sample);
    }
}
