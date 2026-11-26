package org.jgroups.blocks;

import org.jgroups.Address;

/**
 * Interface defining when a group request is done. This allows for termination of a group request based on
 * logic implemented by the caller. Example: caller uses mode GET_FIRST plus a RspFilter implementation. Here, the
 * request will not return (assuming timeout is 0) when the first response has been received, but when the filter
 * passed
 * @author Bela Ban
 */
public interface RspFilter {


    /**
     * Determines whether a response from a given sender should be added to the response list of the request
     * @param response The response (usually a serializable value), may also be a Throwable
     * @param sender The sender of response
     * @return True if we should add the response to the response list ({@link org.jgroups.util.RspList}) of a request,
     * otherwise false. In the latter case, we don't add the response to the response list.
     */
    boolean isAcceptable(Object response, Address sender);

    /**
     * Right after calling {@link #isAcceptable(Object, org.jgroups.Address)}, this method is called to see whether
     * we are done with the request and can unblock the caller
     * @return False if the request is done, otherwise true
     */
    boolean needMoreResponses();
}
