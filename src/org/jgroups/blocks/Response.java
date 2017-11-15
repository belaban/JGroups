package org.jgroups.blocks;

import org.jgroups.Message;

/**
 * A handback object shipped as a parameter to {@link RequestHandler#handle(Message,Response)}.
 * Encapsulates information needed to send a response, e.g. the request ID, the sender etc.
 * @author Bela Ban
 * @since  3.3
 */
public interface Response {

    /**
     * Sends a response, usually called from a thread spawned by
     * {@link AsyncRequestHandler#handle(Message,Response)}
     * @param reply The reply to be sent back, ie. as result to a synchronous RPC. Can be null, e.g.
     *              when the method has a void return type.
     * @param is_exception If {@link AsyncRequestHandler#handle(Message,Response)} threw an exception,
     *                     it must be caught, returned as the reply and is_exception must be true. If reply is a regular
     *                     object, is_exception is false
     */
    void send(Object reply, boolean is_exception);


    /**
     * Similar to {@link #send(Object,boolean)}, but passes a message instead of an object. The message needs to contain
     * the marshalled response, so message creation, setting of flags and marshalling is the responsibility of the caller.
     * <p/>The reason for this additional method is to give the caller more control over the response message.<p/>
     * This method may be removed, should we find that it's not really needed
     * @param reply The reply message
     * @param is_exception Whether the payload of this message is an exception or a real reply object
     */
    void send(Message reply, boolean is_exception);
}
