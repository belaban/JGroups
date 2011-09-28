package org.jgroups.blocks.mux;

import org.jgroups.Address;
import org.jgroups.blocks.RspFilter;

/**
 * Response filter that reject any {@link NoMuxHandler} responses.
 * @author Paul Ferraro
 */
public class NoMuxHandlerRspFilter implements RspFilter {

    private final RspFilter filter;

    public NoMuxHandlerRspFilter() {
        this.filter = null;
    }
    
    public NoMuxHandlerRspFilter(RspFilter filter) {
        this.filter = filter;
    }

    public static RspFilter createInstance(RspFilter filter) {
        if(filter instanceof NoMuxHandlerRspFilter)
            return filter;
        return new NoMuxHandlerRspFilter(filter) ;
    }

    public RspFilter getFilter() {
        return filter;
    }

    @Override
    public boolean isAcceptable(Object response, Address sender) {
        return !(response instanceof NoMuxHandler) && ((filter == null) || filter.isAcceptable(response, sender));
    }

    @Override
    public boolean needMoreResponses() {
        return (filter == null) || filter.needMoreResponses();
    }
}
