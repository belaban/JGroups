package org.jgroups.blocks.mux;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.UpHandler;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RspFilter;
import org.jgroups.stack.Protocol;
import org.jgroups.util.FutureListener;
import org.jgroups.util.RspList;

import java.util.Collection;

/**
 * A multiplexed message dispatcher.
 * When used in conjunction with a MuxUpHandler, allows multiple dispatchers to use the same channel.
 * <br/>
 * Usage:<br/>
 * <code>
 * Channel c = new JChannel(...);<br/>
 * c.setUpHandler(new MuxUpHandler());<br/>
 * <br/>
 * MessageDispatcher d1 = new MuxMessageDispatcher((short) 1, c, ...);<br/>
 * MessageDispatcher d2 = new MuxMessageDispatcher((short) 2, c, ...);<br/>
 * <br/>
 * c.connect(...);<br/>
 * </code>
 * @author Paul Ferraro
 */
public class MuxMessageDispatcher extends MessageDispatcher {

    private final short scope_id;
    
    public MuxMessageDispatcher(short scopeId) {
        this.scope_id = scopeId;
    }

    public MuxMessageDispatcher(short scopeId, Channel channel, MessageListener messageListener, MembershipListener membershipListener, RequestHandler handler) {
        this(scopeId);
        
        setMessageListener(messageListener);
        setMembershipListener(membershipListener);
        setChannel(channel);
        setRequestHandler(handler);
        start();
    }

    private Muxer<UpHandler> getMuxer() {
        UpHandler handler = channel.getUpHandler();
        return ((handler != null) && (handler instanceof MuxUpHandler)) ? (MuxUpHandler) handler : null;
    }

    @Override
    protected RequestCorrelator createRequestCorrelator(Protocol transport, RequestHandler handler, Address localAddr) {
        // We can't set the scope of the request correlator here since this method is called from start()
        // triggered in the MessageDispatcher constructor, when this.scope is not yet defined
        return new MuxRequestCorrelator(scope_id, transport, handler, localAddr);
    }

    @Override
    public void start() {
        super.start();
        Muxer<UpHandler> muxer = this.getMuxer();
        if (muxer != null) {
            muxer.add(scope_id, this.getProtocolAdapter());
        }
   }

    @Override
    public void stop() {
        Muxer<UpHandler> muxer = this.getMuxer();
        if (muxer != null) {
            muxer.remove(scope_id);
        }
        super.stop();
    }

    @Override
    protected <T> GroupRequest<T> cast(Collection<Address> dests, Message msg, RequestOptions options,
                                       boolean blockForResults, FutureListener<RspList<T>> listener) throws Exception {
        RspFilter filter = options.getRspFilter();
        return super.cast(dests, msg, options.setRspFilter(NoMuxHandlerRspFilter.createInstance(filter)), blockForResults, listener);
    }
}