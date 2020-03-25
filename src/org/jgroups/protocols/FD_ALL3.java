package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.Util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Failure detection based on simple heartbeat protocol. Every member periodically (interval ms) multicasts a heartbeat.
 * Every member maintains a table of all members (minus itself) and a bitmap with timeout/interval bits, initially
 * all set to 1. On each interval, the TimeoutChecker task advances the index and sets the bit at the index to 0.<br/>
 * When all bits are 0, a member will be suspected.<br/>
 * On reception of a message or heartbeat from P, P's bitmap at index is set to 1.<br/>
 * JIRA: https://issues.redhat.com/browse/JGRP-2451
 *
 * @author  Dan Berindei
 * @author  Bela Ban
 * @version 5.0.0
 */
public class FD_ALL3 extends FailureDetection {

    // Map of addresses and timestamps of last updates
    protected final Map<Address,Bitmap> timestamps=Util.createConcurrentMap();

    @ManagedAttribute(description="The number of bits for each member (timeout / interval)")
    protected int                       num_bits;


    protected Map<Address,?> getTimestamps()                {return timestamps;}
    protected long           getTimeoutCheckInterval()      {return interval;}

    @ManagedOperation(description="Prints timestamps")
    public String printTimestamps() {
        return _printTimestamps();
    }


    public void init() throws Exception {
        super.init();
        if(interval >= timeout)
            throw new IllegalStateException("interval needs to be smaller than timeout");
        num_bits=computeBits();
    }

    @Override public void start() throws Exception {
        super.start();
        if(interval >= timeout)
            throw new IllegalStateException("interval needs to be smaller than timeout");
    }

    @Override public FD_ALL3 setTimeout(long t) {
        super.setTimeout(t);
        num_bits=computeBits();
        return this;
    }

    @Override public FD_ALL3 setInterval(long i) {
        super.setInterval(i);
        num_bits=computeBits();
        return this;
    }

    @Override protected void update(Address sender, boolean log_msg, boolean skip_if_exists) {
        if(sender != null && !sender.equals(local_addr)) {
            Bitmap bm=timestamps.get(sender);
            if(bm != null) {
                if(!skip_if_exists)
                    bm.set();
            }
            else
                timestamps.putIfAbsent(sender, new Bitmap(num_bits));
        }
        if(log_msg && log.isTraceEnabled())
            log.trace("%s: received heartbeat from %s", local_addr, sender);
    }

    protected int computeBits() {
        return timeout % interval == 0? (int)(timeout / interval) : (int)((timeout / interval)+1);
    }

    protected <T> boolean needsToBeSuspected(Address mbr, T value) {
        Bitmap bm=(Bitmap)value;
        boolean suspect=bm.needsToSuspect();
        bm.advance();
        if(suspect) {
            log.debug("%s: haven't received a heartbeat from %s in timeout period (%d ms), adding it to suspect list",
                      local_addr, mbr, timeout);
            return true;
        }
        return false;
    }

    protected String getTimeoutCheckerInfo() {
        return FD_ALL3.class.getSimpleName() + ": " + getClass().getSimpleName() + " (timeout=" + timeout + " ms)";
    }

    protected String _printTimestamps() {
        StringBuilder sb=new StringBuilder();
        for(Iterator<Entry<Address,Bitmap>> it=timestamps.entrySet().iterator(); it.hasNext();) {
            Entry<Address,Bitmap> entry=it.next();
            sb.append(entry.getKey()).append(": bitmap=").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    public static class Bitmap {
        protected volatile int             index;
        protected final AtomicIntegerArray bits;

        public Bitmap(int size) {
            bits=new AtomicIntegerArray(size);
            for(int i=0; i < size; i++)
                bits.set(i, 1);
        }

        public int getIndex() {return index;}

        /** Returns true if all bits are 0, false otherwise */
        public boolean needsToSuspect() {
            for(int i=0; i < bits.length(); i++) {
                if(bits.get(i) == 1)
                    return false;
            }
            return true;
        }

        /** Advances the index (mod length) and sets the bits[index] to 0 */
        public Bitmap advance() {
            int new_index=(index +1) % bits.length();
            bits.set(new_index, 0);
            index=new_index; // make the change visible to readers
            return this;
        }

        public Bitmap set() {
            bits.set(index, 1);
            return this;
        }

        public String toString() {
            if(bits.length() < Util.MAX_LIST_PRINT_SIZE)
                return toStringDetailed();
            int set=0;
            for(int i=0; i < bits.length(); i++) {
                if(bits.get(i) == 1)
                    set++;
            }
            return String.format("[%d set %d not-set] (index: %d)", set, bits.length() - set, index);
        }

        public String toStringDetailed() {
            return String.format("%s (index=%d)", bits, index);
        }
    }
}
