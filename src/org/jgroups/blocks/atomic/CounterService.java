package org.jgroups.blocks.atomic;

import java.util.concurrent.CompletionStage;

import org.jgroups.JChannel;
import org.jgroups.protocols.COUNTER;
import org.jgroups.util.CompletableFutures;

/**
 * Provides a distributed counter (similar to AtomicLong) which can be atomically updated across a cluster.
 * @author Bela Ban
 * @since 3.0.0
 */
public class CounterService {
    protected COUNTER  counter_prot;

    public CounterService(JChannel ch) {
        setChannel(ch);
    }

    public void setChannel(JChannel ch) {
        counter_prot=ch.getProtocolStack().findProtocol(COUNTER.class);
        if(counter_prot == null)
            throw new IllegalStateException("channel configuration must include the COUNTER protocol");
    }

    /**
     * Returns an existing counter, or creates a new one if none exists
     * @param name Name of the counter, different counters have to have different names
     * @param initial_value The initial value of a new counter if there is no existing counter. Ignored
     * if the counter already exists
     * @return The counter implementation
     * @deprecated since 5.2. Use {@link #getOrCreateSyncCounter(String, long)} instead.
     */
    @Deprecated
    public Counter getOrCreateCounter(String name, long initial_value) {
        return counter_prot.getOrCreateCounter(name, initial_value);
    }

    public SyncCounter getOrCreateSyncCounter(String name, long initial_value) {
        return CompletableFutures.join(getOrCreateAsyncCounter(name, initial_value).thenApply(AsyncCounter::sync));
    }

    /**
     * Returns an existing counter, or creates a new one if none exists
     *
     * @param name          Name of the counter, different counters have to have different names
     * @param initial_value The initial value of a new counter if there is no existing counter. Ignored
     *                      if the counter already exists
     * @return A {@link CompletionStage} which is completed with the counter implementation.
     */
    public CompletionStage<AsyncCounter> getOrCreateAsyncCounter(String name, long initial_value) {
        return counter_prot.getOrCreateAsyncCounter(name, initial_value);
    }

    /**
     * Deletes a counter instance (on the coordinator)
     * @param name The name of the counter. No-op if the counter doesn't exist
     */
    public void deleteCounter(String name) {
        counter_prot.deleteCounter(name);
    }


    public String printCounters() {
        return counter_prot.printCounters();
    }


    public String dumpPendingRequests() {return counter_prot.dumpPendingRequests();}


}
