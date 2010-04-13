package org.jgroups.blocks.mux;

import java.util.Collection;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RspCollector;
import org.jgroups.conf.ClassConfigurator;

/**
 * A request correlator that adds a mux header to incoming and outgoing messages.
 * @author Bela Ban
 * @author Paul Ferraro
 * @version $Id: MuxRequestCorrelator.java,v 1.1 2010/04/13 17:57:07 ferraro Exp $
 */
public class MuxRequestCorrelator extends RequestCorrelator {

    private final org.jgroups.Header header;
    
    public MuxRequestCorrelator(short id, Object transport, RequestHandler handler, Address localAddr) {
        super(ClassConfigurator.getProtocolId(RequestCorrelator.class), transport, handler, localAddr);
        this.header = new MuxHeader(id);
    }

    @Override
    public void sendRequest(long requestId, Collection<Address> dest_mbrs, Message msg, RspCollector coll, boolean use_anycasting) throws Exception {
        msg.putHeader(MuxHeader.ID, header);
        super.sendRequest(requestId, dest_mbrs, msg, coll, use_anycasting);
    }

    @Override
    protected void prepareResponse(Message rsp) {
        rsp.putHeader(MuxHeader.ID, header);
        super.prepareResponse(rsp);
    }
}
