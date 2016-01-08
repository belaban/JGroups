package org.jgroups.blocks.mux;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;

import java.util.Collection;

/**
 * A request correlator that adds a mux header to incoming and outgoing messages.
 * @author Bela Ban
 * @author Paul Ferraro
 * @author Brian Stansberry
 */
public class MuxRequestCorrelator extends RequestCorrelator {

    protected final static short MUX_ID = ClassConfigurator.getProtocolId(MuxRequestCorrelator.class);
    private final org.jgroups.Header header;
    
    public MuxRequestCorrelator(short id, Protocol transport, RequestHandler handler, Address localAddr) {
        super(ClassConfigurator.getProtocolId(RequestCorrelator.class), transport, handler, localAddr);
        this.header = new MuxHeader(id);
    }

    @Override
    public void sendRequest(Collection<Address> dest_mbrs, Message msg, Request req, RequestOptions options) throws Exception {
        msg.putHeader(MUX_ID, header);
        super.sendRequest(dest_mbrs, msg, req, options);
    }

    @Override
    public void sendUnicastRequest(Address target, Message msg, Request req) throws Exception {
        msg.putHeader(MUX_ID, header);
        super.sendUnicastRequest(target, msg, req);
    }

    @Override
    protected void prepareResponse(Message rsp) {
        rsp.putHeader(MUX_ID, header);
        super.prepareResponse(rsp);
    }
}
