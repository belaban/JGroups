package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.util.ByteArray;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


/**
 * Abstract class for a unicast or multicast request
 *
 * @author Bela Ban
 */
public abstract class Request<T> extends CompletableFuture<T> {
    protected long                    req_id;
    protected final RequestCorrelator corr;         // either use RequestCorrelator or ...
    protected final RequestOptions    options;
    protected long                    start_time;   // in ns, to compute RTT for blocking RPCs


    public Request(RequestCorrelator corr, RequestOptions options) {
        this.corr=corr;
        this.options=options;
    }

    public Request<T> requestId(long req_id) {
        this.req_id=req_id;
        return this;
    }

    public long requestId() {
        return req_id;
    }

    public Request setResponseFilter(RspFilter filter) {
        options.rspFilter(filter);
        return this;
    }


    public T execute(ByteArray data, boolean block_for_results) throws Exception {
        if(corr == null)
            return null;

        sendRequest(data);
        if(!block_for_results || options.mode() == ResponseMode.GET_NONE)
            return null;
        long timeout=options.timeout();
        return timeout > 0? waitForCompletion(options.timeout(), TimeUnit.MILLISECONDS) : waitForCompletion();
    }

    public abstract void       sendRequest(ByteArray data) throws Exception;

    public abstract void       receiveResponse(Object response_value, Address sender, boolean is_exception);

    public abstract void       viewChange(View new_view);

    public abstract void       siteUnreachable(String site);

    public abstract void       transportClosed();

    /** Blocks until all responses have been received and returns result or throws exception */
    public abstract T          waitForCompletion(long timeout, TimeUnit unit) throws Exception;
    public abstract T          waitForCompletion() throws Exception;



    public boolean cancel(boolean mayInterruptIfRunning) {
        try {
            return super.cancel(mayInterruptIfRunning);
        }
        finally {
            corrDone();
        }
    }


    public String toString() {
        return String.format("%s, mode=%s", getClass().getSimpleName(), options.mode());
    }


    protected void corrDone() {
        if(corr!=null && this.req_id > 0)
            corr.done(this.req_id);
    }
}
