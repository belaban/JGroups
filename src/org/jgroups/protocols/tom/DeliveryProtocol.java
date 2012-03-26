package org.jgroups.protocols.tom;

import org.jgroups.Message;

/**
 * The interface that the Total Order Anycast protocol must implement. This is invoked by the delivery thread
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public interface DeliveryProtocol {

    /**
     * deliver a message
     * @param message   message to deliver
     */
    void deliver(Message message);
}
