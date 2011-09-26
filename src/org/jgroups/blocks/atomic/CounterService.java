package org.jgroups.blocks.atomic;

import org.jgroups.JChannel;
import org.jgroups.protocols.COUNTER;

/**
 * Provides a distributed counter (similar to AtomicInteger) which can be atomically updated across a cluster.
 * @author Bela Ban
 * @since 3.0.0
 */
public class CounterService {
    protected JChannel ch;
    protected COUNTER counter_prot;

    public CounterService(JChannel ch) {
        setChannel(ch);
    }

    public void setChannel(JChannel ch) {
        this.ch=ch;
        counter_prot=(COUNTER)ch.getProtocolStack().findProtocol(COUNTER.class);
        if(counter_prot == null)
            throw new IllegalStateException("Channel configuration must include the COUNTER protocol");
    }

    /**
     * Returns an existing counter, or creates a new one if none exists
     * @param name Name of the counter, different counters have to have different names
     * @param initial_value The initial value of a new counter if there is no existing counter. Ignored
     * if the counter already exists
     * @return The counter implementation
     */
    public Counter getOrCreateCounter(String name, int initial_value) {
        return null;
    }

  
    /**
     * Deletes a counter instance (on the coordinator)
     * @param name The name of the counter. No-op if the counter doesn't exist
     */
    public void deleteCounter(String name) {

    }




}
