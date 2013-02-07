package org.jgroups.blocks;

import org.jgroups.Message;

/**
 * Provides a way to invoke requests asynchronously
 * @author Bela Ban
 * @since  3.3
 * @todo Move into {@link RequestHandler} in 4.0
 */
public interface AsyncRequestHandler extends RequestHandler {

    /**
     * Invokes a request. This should be done <em>asynchronously</em>, e.g. by dispatching this to a thread pool.
     * When done, if a response is needed (e.g. in case of a sync RPC), {@link Response#send(Object,boolean)} should
     * be called.
     * @param request The request
     * @param response The response implementation. Contains information needed to send the reply (e.g. a request ID).
     *                 If no response is required, e.g. because this is an asynchronous RPC, then response will be null.
     * @throws Exception If an exception is thrown (e.g. in case of an issue submitting the request to a thread pool,
     * the exception will be taken as return value and will be sent as a response. In this case,
     * {@link Response#send(Object,boolean)} must not be called
     */
    void handle(Message request, Response response) throws Exception;
}
