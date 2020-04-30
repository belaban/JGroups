package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.TimeService;
import org.jgroups.util.Util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Failure detection based on simple heartbeat protocol. Every member periodically multicasts a heartbeat.
 * Every member also maintains a table of all members (minus itself). When data or a heartbeat from P is received,
 * we reset the timestamp for P to the current time. Periodically, we check for expired members, and suspect those.</p>
 * Reduced number of messages exchanged on suspect event: https://jira.jboss.org/browse/JGRP-1241
 * 
 * @author Bela Ban
 */
public class FD_ALL extends FailureDetection {

    @Property(description="Interval at which the HEARTBEAT timeouts are checked",type=AttributeType.TIME)
    protected long                                   timeout_check_interval=2000;

    @Property(description="Uses TimeService to get the current time rather than calling System.nanoTime().")
    protected boolean                                use_time_service=true;

    // Map of addresses and timestamps of last updates (ns)
    protected final ConcurrentMap<Address, Long>     timestamps=Util.createConcurrentMap();
    protected TimeService                            time_service;


    protected Map<Address,?>              getTimestamps()                 {return timestamps;}
    public long                           getTimeoutCheckInterval()       {return timeout_check_interval;}
    public <T extends FailureDetection> T setTimeoutCheckInterval(long i) {this.timeout_check_interval=i; return (T)this;}

    @ManagedOperation(description="Prints timestamps")
    public String printTimestamps() {
        return _printTimestamps();
    }


    public void init() throws Exception {
        super.init();
        time_service=getTransport().getTimeService();
        if(time_service == null)
            log.warn("%s: time service is not available, using System.nanoTime() instead", local_addr);
        else {
            if(time_service.interval() > timeout) {
                log.warn("%s: interval of time service (%d) is greater than timeout (%d), disabling time service",
                         local_addr, time_service.interval(), timeout);
                use_time_service=false;
            }
        }
        if(interval > timeout)
            log.warn("interval (%d) is bigger than timeout (%d); this will lead to false suspicions", interval, timeout);
    }


    @Override protected void update(Address sender, boolean log_msg, boolean skip_if_exists) {
        if(sender != null && !sender.equals(local_addr)) {
            if(skip_if_exists)
                timestamps.putIfAbsent(sender, getTimestamp());
            else
                timestamps.put(sender, getTimestamp());
        }
        if(log_msg && log.isTraceEnabled())
            log.trace("%s: received heartbeat from %s", local_addr, sender);
    }


    protected long getTimestamp() {
        return use_time_service && time_service != null? time_service.timestamp() : System.nanoTime();
    }

    protected <T> boolean needsToBeSuspected(Address mbr, T value) {
        long val=(long)value;
        long diff=TimeUnit.MILLISECONDS.convert(getTimestamp() - val, TimeUnit.NANOSECONDS);
        if(diff > timeout) {
            log.debug("%s: haven't received a heartbeat from %s for %s ms, adding it to suspect list",
                      local_addr, mbr, diff);
            return true;
        }
        return false;
    }

    protected String getTimeoutCheckerInfo() {
        return FD_ALL.class.getSimpleName() + ": " + getClass().getSimpleName() + " (interval=" + timeout_check_interval + " ms)";
    }

    protected String _printTimestamps() {
        StringBuilder sb=new StringBuilder();
        long current_time=getTimestamp();
        for(Iterator<Entry<Address,Long>> it=timestamps.entrySet().iterator(); it.hasNext();) {
            Entry<Address,Long> entry=it.next();
            sb.append(entry.getKey()).append(": ");
            sb.append(TimeUnit.SECONDS.convert (current_time - entry.getValue(), TimeUnit.NANOSECONDS)).append(" secs old\n");
        }
        return sb.toString();
    }
}
