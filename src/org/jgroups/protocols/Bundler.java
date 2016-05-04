package org.jgroups.protocols;

import org.jgroups.Message;

/**
 * Pluggable way to collect messages and send them as batches
 * @author Bela Ban
 * @since  4.0
 */
public interface Bundler {
    /**
     * Called after creation of the bundler
     * @param transport the transport, for further reference
     */
    void init(TP transport);

    /** Called after {@link #init(TP)} */
    void start();
    void stop();
    void send(Message msg) throws Exception;
}
