package org.jgroups.blocks;

/**
 * A handback object shipped as a parameter to {@link AsyncRequestHandler#handle(org.jgroups.Message,Response)}.
 * Encapsulates information needed to send a response, e.g. the request ID, the sender etc.
 * @author Bela Ban
 * @since  3.3
 */
public interface Response {

    /**
     * Sends a response, usually called from a thread spawned by
     * {@link AsyncRequestHandler#handle(org.jgroups.Message,Response)}
     * @param reply The reply to be sent back, ie. as result to a synchronous RPC. Can be null, e.g.
     *              when the method has a void return type.
     * @param is_exception If {@link AsyncRequestHandler#handle(org.jgroups.Message,Response)} threw an exception,
     *                     it must be caught, returned as the reply and is_exception must be true. If reply is a regular
     *                     object, is_exception is false
     */
    void send(Object reply, boolean is_exception);
}
