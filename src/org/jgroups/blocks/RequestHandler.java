
package org.jgroups.blocks;


import org.jgroups.Message;


public interface RequestHandler {

    /**
     * Processes a request <em>synchronously</em>, ie. on the thread invoking this handler
     * @param msg the message containing the request
     * @return the object, rceeived as result, or null (void method)
     */
    Object handle(Message msg) throws Exception;

    /**
     * Processes a request <em>asynchronously</em>. This could be done (for example) by dispatching this to a thread pool.
     * When done, if a response is needed (e.g. in case of a sync RPC), {@link Response#send(Object,boolean)} should
     * be called.
     * @param request The request
     * @param response The response implementation. Contains information needed to send the reply (e.g. a request ID).
     *                 If no response is required, e.g. because this is an asynchronous RPC, then response will be null.
     * @throws Exception If an exception is thrown (e.g. in case of an issue submitting the request to a thread pool,
     * the exception will be taken as return value and will be sent as a response. In this case,
     * {@link Response#send(Object,boolean)} must not be called
     */
    default void handle(Message request, Response response) throws Exception {
        throw new UnsupportedOperationException();
    }
}
