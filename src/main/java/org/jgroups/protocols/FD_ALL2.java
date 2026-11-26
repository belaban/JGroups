package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.Util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Failure detection based on simple heartbeat protocol. Every member periodically (interval ms) multicasts a heartbeat.
 * Every member also maintains a table of all members (minus itself). When data or a heartbeat from P is received,
 * we set the flag associated with P to true. Periodically, we check for expired members, and suspect those whose flag
 * is false (no heartbeat or message received within timeout ms).
 *
 * @author  Bela Ban
 * @version 3.5
 */
public class FD_ALL2 extends FailureDetection {

    // Map of addresses and timestamps of last updates
    protected final Map<Address,AtomicBoolean> timestamps=Util.createConcurrentMap();

    protected Map<Address,?> getTimestamps()                {return timestamps;}
    protected long           getTimeoutCheckInterval()      {return timeout;}

    @ManagedOperation(description="Prints timestamps")
    public String printTimestamps() {
        return _printTimestamps();
    }

    @Override protected void update(Address sender, boolean log_msg, boolean skip_if_exists) {
        if(sender != null && !sender.equals(local_addr)) {
            AtomicBoolean heartbeat_received=timestamps.get(sender);
            if(heartbeat_received != null) {
                if(!skip_if_exists)
                    heartbeat_received.compareAndSet(false, true);
            }
            else
                timestamps.putIfAbsent(sender, new AtomicBoolean(true));
        }
        if(log_msg && log.isTraceEnabled())
            log.trace("%s: received heartbeat from %s", local_addr, sender);
    }



    protected <T> boolean needsToBeSuspected(Address mbr, T value) {
        AtomicBoolean val=(AtomicBoolean)value;
        if(!val.compareAndSet(true, false)) {
            log.debug("%s: haven't received a heartbeat from %s in timeout period (%d ms), adding it to suspect list",
                      local_addr, mbr, timeout);
            return true;
        }
        return false;
    }

    protected String getTimeoutCheckerInfo() {
        return FD_ALL2.class.getSimpleName() + ": " + getClass().getSimpleName() + " (timeout=" + timeout + " ms)";
    }

    protected String _printTimestamps() {
        StringBuilder sb=new StringBuilder();
        for(Iterator<Entry<Address,AtomicBoolean>> it=timestamps.entrySet().iterator(); it.hasNext();) {
            Entry<Address,AtomicBoolean> entry=it.next();
            sb.append(entry.getKey()).append(": received=").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }
}
