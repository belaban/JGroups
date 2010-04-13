package org.jgroups.blocks.mux;

import org.jgroups.Address;
import org.jgroups.blocks.RspFilter;

/**
 * Response filter that reject any {@link NoMuxHandler} responses.
 * @author Paul Ferraro
 * @version $Id: NoMuxHandlerRspFilter.java,v 1.1 2010/04/13 17:57:07 ferraro Exp $
 */
public class NoMuxHandlerRspFilter implements RspFilter {

    private final RspFilter filter;

    public NoMuxHandlerRspFilter() {
        this.filter = null;
    }
    
    public NoMuxHandlerRspFilter(RspFilter filter) {
        this.filter = filter;
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
