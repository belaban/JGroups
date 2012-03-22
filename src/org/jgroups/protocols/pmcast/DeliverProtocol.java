package org.jgroups.protocols.pmcast;

import org.jgroups.Message;

/**
 * The interface that the Total Order Multicast protocol must implement. This is invoked by the deliver thread
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public interface DeliverProtocol {

    /**
     * deliver a message
     * @param message   message to deliver
     */
    void deliver(Message message);
}
