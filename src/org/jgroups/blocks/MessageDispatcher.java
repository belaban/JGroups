
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.blocks.mux.Muxer;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;


/**
 * Provides synchronous and asynchronous message sending with request-response
 * correlation; i.e., matching responses with the original request.
 * It also offers push-style message reception (by internally using the PullPushAdapter).
 * <p>
 * Channels are simple patterns to asynchronously send a receive messages.
 * However, a significant number of communication patterns in group communication
 * require synchronous communication. For example, a sender would like to send a
 * message to the group and wait for all responses. Or another application would
 * like to send a message to the group and wait only until the majority of the
 * receivers have sent a response, or until a timeout occurred.  MessageDispatcher
 * offers a combination of the above pattern with other patterns.
 * <p>
 * Used on top of channel to implement group requests. Client's {@code handle()}
 * method is called when request is received. Is the equivalent of RpcProtocol on
 * the application instead of protocol level.
 *
 * @author Bela Ban
 */
public class MessageDispatcher implements AsyncRequestHandler, ChannelListener, Closeable {
    protected Channel                               channel;
    protected RequestCorrelator                     corr;
    protected MessageListener                       msg_listener;
    protected MembershipListener                    membership_listener;
    protected RequestHandler                        req_handler;
    protected boolean                               async_dispatching;
    protected boolean                               wrap_exceptions=true;
    protected ProtocolAdapter                       prot_adapter;
    protected volatile Collection<Address>          members=new HashSet<>();
    protected Address                               local_addr;
    protected final Log                             log=LogFactory.getLog(MessageDispatcher.class);
    protected boolean                               hardware_multicast_supported=false;
    protected final Set<ChannelListener>            channel_listeners=new CopyOnWriteArraySet<>();
    protected final RpcStats                        rpc_stats=new RpcStats(false);


    public MessageDispatcher() {
    }

    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2) {
        this.channel=channel;
        prot_adapter=new ProtocolAdapter();
        if(channel != null) {
            local_addr=channel.getAddress();
            channel.addChannelListener(this);
        }
        setMessageListener(l);
        setMembershipListener(l2);
        if(channel != null)
            installUpHandler(prot_adapter, true);
        start();
    }


    public MessageDispatcher(Channel channel, RequestHandler req_handler) {
        this(channel, null, null, req_handler);
    }

    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, RequestHandler req_handler) {
        this(channel, l, l2);
        setRequestHandler(req_handler);
    }

    public RpcStats          rpcStats()                {return rpc_stats;}
    public MessageDispatcher extendedStats(boolean fl) {rpc_stats.extendedStats(fl); return this;}
    public boolean           extendedStats()           {return rpc_stats.extendedStats();}
    public boolean           asyncDispatching()        {return async_dispatching;}

    public MessageDispatcher asyncDispatching(boolean flag) {
        async_dispatching=flag;
        if(corr != null)
            corr.asyncDispatching(flag);
        return this;
    }

    public boolean                  wrapExceptions()               {return wrap_exceptions;}
    public MessageDispatcher        wrapExceptions(boolean flag)   {
        wrap_exceptions=flag;
        if(corr != null)
            corr.wrapExceptions(flag);
        return this;}

    public UpHandler getProtocolAdapter() {
        return prot_adapter;
    }

    public RequestCorrelator               correlator() {return corr;}

    public <T extends MessageDispatcher> T correlator(RequestCorrelator c) {
        if(c == null)
            return (T)this;
        stop();
        this.corr=c;
        corr.asyncDispatching(this.async_dispatching).wrapExceptions(this.wrap_exceptions);
        start();
        return (T)this;
    }



    /**
     * If this dispatcher is using a user-provided PullPushAdapter, then need to set the members from the adapter
     * initially since viewChange has most likely already been called in PullPushAdapter.
     */
    protected void setMembers(List<Address> new_mbrs) {
        if(new_mbrs != null)
            members=new HashSet<>(new_mbrs); // volatile write - seen by a subsequent read
    }


    /**
     * Adds a new channel listener to be notified on the channel's state change.
     */
    public void addChannelListener(ChannelListener l) {
        if(l != null)
            channel_listeners.add(l);
    }


    public void removeChannelListener(ChannelListener l) {
        if(l != null)
            channel_listeners.remove(l);
    }



    public void start() {
        if(corr == null)
            corr=createRequestCorrelator(prot_adapter, this, local_addr)
              .asyncDispatching(async_dispatching).wrapExceptions(this.wrap_exceptions);
        correlatorStarted();
        corr.start();

        if(channel != null) {
            List<Address> tmp_mbrs=channel.getView() != null ? channel.getView().getMembers() : null;
            setMembers(tmp_mbrs);
            if(channel instanceof JChannel) {
                TP transport=channel.getProtocolStack().getTransport();
                corr.registerProbeHandler(transport);
            }
            TP transport=channel.getProtocolStack().getTransport();
            hardware_multicast_supported=transport.supportsMulticasting();
            // transport.registerProbeHandler(probe_handler);
        }
    }

    protected RequestCorrelator createRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr) {
        return new RequestCorrelator(transport, handler, local_addr);
    }

    protected void correlatorStarted() {
        ;
    }

    @Override public void close() throws IOException {stop();}

    public void stop() {
        if(corr != null)
            corr.stop();

        if(channel instanceof JChannel) {
            TP transport=channel.getProtocolStack().getTransport();
            // transport.unregisterProbeHandler(probe_handler);
            if(corr != null)
                corr.unregisterProbeHandler(transport);
        }
    }

    public final void setMessageListener(MessageListener l) {
        msg_listener=l;
    }

    public MessageListener getMessageListener() {
        return msg_listener;
    }

    public final void setMembershipListener(MembershipListener l) {
        membership_listener=l;
    }

    public final void setRequestHandler(RequestHandler rh) {
        req_handler=rh;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel ch) {
        if(ch == null)
            return;
        this.channel=ch;
        local_addr=channel.getAddress();
        if(prot_adapter == null)
            prot_adapter=new ProtocolAdapter();
        // Don't force installing the UpHandler so subclasses can use this
        // method and still integrate with a MuxUpHandler
        installUpHandler(prot_adapter, false);
    }

    /**
     * Sets the given UpHandler as the UpHandler for the channel, or, if the
     * channel already has a Muxer installed as it's UpHandler, sets the given
     * handler as the Muxer's {@link Muxer#setDefaultHandler(Object) default handler}.
     * If the relevant handler is already installed, the {@code canReplace}
     * controls whether this method replaces it (after logging a WARN) or simply
     * leaves {@code handler} uninstalled.
     * <p>
     * Passing {@code false} as the {@code canReplace} value allows
     * callers to use this method to install defaults without concern about
     * inadvertently overriding
     *
     * @param handler the UpHandler to install
     * @param canReplace {@code true} if an existing Channel upHandler or
     *              Muxer default upHandler can be replaced; {@code false}
     *              if this method shouldn't install
     */
    protected void installUpHandler(UpHandler handler, boolean canReplace)
    {
       UpHandler existing = channel.getUpHandler();
       if (existing == null) {
           channel.setUpHandler(handler);
       }
       else if (existing instanceof Muxer<?>) {
           @SuppressWarnings("unchecked")
           Muxer<UpHandler> mux = (Muxer<UpHandler>) existing;
           if (mux.getDefaultHandler() == null) {
               mux.setDefaultHandler(handler);
           }
           else if (canReplace) {
               log.warn("Channel Muxer already has a default up handler installed (%s) but now it is being overridden",  mux.getDefaultHandler());
               mux.setDefaultHandler(handler);
           }
       }
       else if (canReplace) {
           log.warn("Channel already has an up handler installed (%s) but now it is being overridden", existing);
           channel.setUpHandler(handler);
       }
    }



    /**
     * Sends a message to all members and expects responses from members in dests (if non-null).
     * @param dests A list of group members from which to expect responses (if the call is blocking).
     * @param msg The message to be sent
     * @param options A set of options that govern the call. See {@link org.jgroups.blocks.RequestOptions} for details
     * @return RspList A list of Rsp elements
     * @throws Exception If the request cannot be sent
     * @since 2.9
     */
    public <T> RspList<T> castMessage(final Collection<Address> dests,
                                      Message msg, RequestOptions options) throws Exception {
        GroupRequest<T> req=cast(dests, msg, options, true);
        return req != null? req.getResults() : new RspList();
    }


    /**
     * Sends a message to all members and expects responses from members in dests (if non-null).
     * @param dests A list of group members from which to expect responses (if the call is blocking).
     * @param msg The message to be sent
     * @param options A set of options that govern the call. See {@link org.jgroups.blocks.RequestOptions} for details
     * @param listener A FutureListener which will be registered (if non null) with the future <em>before</em> the call is invoked
     * @return NotifyingFuture<T> A future from which the results (RspList) can be retrieved
     * @throws Exception If the request cannot be sent
     */
    public <T> NotifyingFuture<RspList<T>> castMessageWithFuture(final Collection<Address> dests,
                                                                 Message msg,
                                                                 RequestOptions options,
                                                                 FutureListener<RspList<T>> listener) throws Exception {
        GroupRequest<T> req=cast(dests,msg,options,false, listener);
        return req != null? req : new NullFuture<>(new RspList<T>());
    }

    /**
     * Sends a message to all members and expects responses from members in dests (if non-null).
     * @param dests A list of group members from which to expect responses (if the call is blocking).
     * @param msg The message to be sent
     * @param options A set of options that govern the call. See {@link org.jgroups.blocks.RequestOptions} for details
     * @return NotifyingFuture<T> A future from which the results (RspList) can be retrieved
     * @throws Exception If the request cannot be sent
     */
    public <T> NotifyingFuture<RspList<T>> castMessageWithFuture(final Collection<Address> dests,
                                                                 Message msg,
                                                                 RequestOptions options) throws Exception {
        return castMessageWithFuture(dests, msg, options, null);
    }



    protected <T> GroupRequest<T> cast(final Collection<Address> dests, Message msg, RequestOptions options,
                                       boolean block_for_results, FutureListener<RspList<T>> listener) throws Exception {
        if(msg.getDest() != null && !(msg.getDest() instanceof AnycastAddress))
            throw new IllegalArgumentException("message destination is non-null, cannot send message");

        if(options == null) {
            log.warn("request options were null, using default of sync");
            options=RequestOptions.SYNC();
        }

        msg.setFlag(options.getFlags()).setTransientFlag(options.getTransientFlags());
        if(options.getScope() > 0)
            msg.setScope(options.getScope());

        List<Address> real_dests;
        // we need to clone because we don't want to modify the original
        if(dests != null) {
            real_dests=new ArrayList<>(dests.size());
            for(Address dest: dests) {
                if(dest instanceof SiteAddress || this.members.contains(dest)) {
                    if(!real_dests.contains(dest))
                        real_dests.add(dest);
                }
            }
        }
        else
            real_dests=new ArrayList<>(members);

        // if local delivery is off, then we should not wait for the message from the local member.
        // therefore remove it from the membership
        Channel tmp=channel;
        if((tmp != null && tmp.getDiscardOwnMessages()) || msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK)) {
            if(local_addr == null)
                local_addr=tmp != null? tmp.getAddress() : null;
            if(local_addr != null)
                real_dests.remove(local_addr);
        }

        if(options.hasExclusionList()) {
            Address[] exclusion_list=options.exclusionList();
            for(Address excluding: exclusion_list)
                real_dests.remove(excluding);
        }

        if(real_dests.isEmpty()) {
            log.trace("destination list is empty, won't send message");
            return null;
        }

        boolean sync=options.getMode() != ResponseMode.GET_NONE;
        boolean non_blocking=!sync || !block_for_results, anycast=options.getAnycasting();
        if(non_blocking) {
            if(anycast)
                rpc_stats.addAnycast(sync, 0, real_dests);
            else
                rpc_stats.add(RpcStats.Type.MULTICAST, null, sync, 0);
        }

        GroupRequest<T> req=new GroupRequest<>(corr, real_dests, options);
        if(listener != null)
            req.setListener(listener);
        req.setResponseFilter(options.getRspFilter());
        long start=non_blocking || !rpc_stats.extendedStats()? 0 : System.nanoTime();
        req.execute(msg, block_for_results);
        long time=non_blocking || !rpc_stats.extendedStats()? 0 : System.nanoTime() - start;
        if(!non_blocking) {
            if(anycast)
                rpc_stats.addAnycast(sync, time, real_dests);
            else
                rpc_stats.add(RpcStats.Type.MULTICAST, null, sync, time);
        }
        return req;
    }

    protected <T> GroupRequest<T> cast(final Collection<Address> dests, Message msg, RequestOptions options,
                                       boolean block_for_results) throws Exception {
        return cast(dests, msg, options, block_for_results, null);
    }


    public void done(long req_id) {
        corr.done(req_id);
    }


    /**
     * Sends a unicast message and - depending on the options - returns a result
     * @param msg the message to be sent. The destination needs to be non-null
     * @param opts the options to be used
     * @return T the result
     * @throws Exception If there was problem sending the request, processing it at the receiver, or processing
     *                   it at the sender.
     * @throws TimeoutException If the call didn't succeed within the timeout defined in options (if set)
     */
    public <T> T sendMessage(Message msg, RequestOptions opts) throws Exception {
        Address dest=msg.getDest();
        if(dest == null)
            throw new IllegalArgumentException("message destination is null, cannot send message");

        if(opts == null) {
            log.warn("request options were null, using default of sync");
            opts=RequestOptions.SYNC();
        }

        msg.setFlag(opts.getFlags()).setTransientFlag(opts.getTransientFlags());
        if(opts.getScope() > 0)
            msg.setScope(opts.getScope());
        boolean async_rpc=opts.getMode() == ResponseMode.GET_NONE;
        if(async_rpc)
            rpc_stats.add(RpcStats.Type.UNICAST, dest, false, 0);
        UnicastRequest<T> req=new UnicastRequest<>(corr, dest, opts);
        long start=async_rpc || !rpc_stats.extendedStats()? 0 : System.nanoTime();
        req.execute(msg, true);
        if(async_rpc)
            return null;
        long time=!rpc_stats.extendedStats()? 0 : System.nanoTime() - start;
        rpc_stats.add(RpcStats.Type.UNICAST, dest, true, time);
        Rsp<T> rsp=req.getResult();
        if(rsp.wasSuspected())
            throw new SuspectedException(dest);

        Throwable exception=rsp.getException();
        if(exception != null) {
            if(exception instanceof Error) throw (Error)exception;
            else if(exception instanceof RuntimeException) throw (RuntimeException)exception;
            else if(exception instanceof Exception) throw (Exception)exception;
            else throw new RuntimeException(exception);
        }

        if(rsp.wasUnreachable())
            throw new UnreachableException(dest);
        if(!rsp.wasReceived() && !req.responseReceived())
            throw new TimeoutException("timeout waiting for response from " + dest + ", request: " + req.toString());
        return rsp.getValue();
    }


    /**
     * Sends a unicast message to the target defined by msg.getDest() and returns a future
     * @param msg The unicast message to be sent. msg.getDest() must not be null
     * @param options
     * @param listener A FutureListener which will be registered (if non null) with the future <em>before</em> the call is invoked
     * @return NotifyingFuture<T> A future from which the result can be fetched
     * @throws Exception If there was problem sending the request, processing it at the receiver, or processing
     *                   it at the sender. {@link java.util.concurrent.Future#get()} will throw this exception
     * @throws TimeoutException If the call didn't succeed within the timeout defined in options (if set)
     */
    public <T> NotifyingFuture<T> sendMessageWithFuture(Message msg, RequestOptions options,
                                                        FutureListener<T> listener) throws Exception {
        Address dest=msg.getDest();
        if(dest == null)
            throw new IllegalArgumentException("message destination is null, cannot send message");

        if(options == null) {
            log.warn("request options were null, using default of sync");
            options=RequestOptions.SYNC();
        }

        msg.setFlag(options.getFlags()).setTransientFlag(options.getTransientFlags());
        if(options.getScope() > 0)
            msg.setScope(options.getScope());
        rpc_stats.add(RpcStats.Type.UNICAST, dest, options.getMode() != ResponseMode.GET_NONE, 0);
        UnicastRequest<T> req=new UnicastRequest<>(corr, dest, options);
        if(listener != null)
            req.setListener(listener);
        req.execute(msg, false);
        if(options.getMode() == ResponseMode.GET_NONE)
            return new NullFuture<>(null);
        return req;
    }


    /**
     * Sends a unicast message to the target defined by msg.getDest() and returns a future
     * @param msg The unicast message to be sent. msg.getDest() must not be null
     * @param options
     * @return NotifyingFuture<T> A future from which the result can be fetched
     * @throws Exception If there was problem sending the request, processing it at the receiver, or processing
     *                   it at the sender. {@link java.util.concurrent.Future#get()} will throw this exception
     * @throws TimeoutException If the call didn't succeed within the timeout defined in options (if set)
     */
    public <T> NotifyingFuture<T> sendMessageWithFuture(Message msg, RequestOptions options) throws Exception {
        return sendMessageWithFuture(msg, options, null);
    }



    /* ------------------------ RequestHandler Interface ---------------------- */
    @Override
    public Object handle(Message msg) throws Exception {
        if(req_handler != null)
            return req_handler.handle(msg);
        return null;
    }
    /* -------------------- End of RequestHandler Interface ------------------- */



    /* -------------------- AsyncRequestHandler Interface --------------------- */
    @Override
    public void handle(Message request, Response response) throws Exception {
        if(req_handler != null) {
            if(req_handler instanceof AsyncRequestHandler)
                ((AsyncRequestHandler)req_handler).handle(request, response);
            else {
                Object retval=req_handler.handle(request);
                if(response != null)
                    response.send(retval, false);
            }
            return;
        }

        Object retval=handle(request);
        if(response != null)
            response.send(retval, false);
    }
    /* ------------------ End of AsyncRequestHandler Interface----------------- */




    /* --------------------- Interface ChannelListener ---------------------- */

    @Override
    public void channelConnected(Channel channel) {
        for(ChannelListener l: channel_listeners) {
            try {
                l.channelConnected(channel);
            }
            catch(Throwable t) {
                log.warn("notifying channel listener " + l + " failed", t);
            }
        }
    }

    @Override
    public void channelDisconnected(Channel channel) {
        stop();
        for(ChannelListener l: channel_listeners) {
            try {
                l.channelDisconnected(channel);
            }
            catch(Throwable t) {
                log.warn("notifying channel listener " + l + " failed", t);
            }
        }
    }

    @Override
    public void channelClosed(Channel channel) {
        stop();
        for(ChannelListener l: channel_listeners) {
            try {
                l.channelClosed(channel);
            }
            catch(Throwable t) {
                log.warn("notifying channel listener " + l + " failed", t);
            }
        }
    }

    /* ----------------------------------------------------------------------- */


    protected Object handleUpEvent(Event evt) throws Exception {
        switch(evt.getType()) {
            case Event.MSG:
                if(msg_listener != null)
                    msg_listener.receive((Message) evt.getArg());
                break;

            case Event.GET_APPLSTATE: // reply with GET_APPLSTATE_OK
                byte[] tmp_state=null;
                if(msg_listener != null) {
                    ByteArrayOutputStream output=new ByteArrayOutputStream(1024);
                    msg_listener.getState(output);
                    tmp_state=output.toByteArray();
                }
                return new StateTransferInfo(null, 0L, tmp_state);

            case Event.GET_STATE_OK:
                if(msg_listener != null) {
                    StateTransferResult result=(StateTransferResult)evt.getArg();
                    if(result.hasBuffer()) {
                        ByteArrayInputStream input=new ByteArrayInputStream(result.getBuffer());
                        msg_listener.setState(input);
                    }
                }
                break;

            case Event.STATE_TRANSFER_OUTPUTSTREAM:
                OutputStream os=(OutputStream)evt.getArg();
                if(msg_listener != null && os != null) {
                    msg_listener.getState(os);
                }
                break;

            case Event.STATE_TRANSFER_INPUTSTREAM:
                InputStream is=(InputStream)evt.getArg();
                if(msg_listener != null && is!=null)
                    msg_listener.setState(is);
                break;

            case Event.VIEW_CHANGE:
                View v=(View) evt.getArg();
                List<Address> new_mbrs=v.getMembers();
                setMembers(new_mbrs);
                if(membership_listener != null)
                    membership_listener.viewAccepted(v);
                break;

            case Event.SET_LOCAL_ADDRESS:
                log.trace("setting local_addr (%s) to %s", local_addr, evt.getArg());
                local_addr=(Address)evt.getArg();
                break;

            case Event.SUSPECT:
                if(membership_listener != null)
                    membership_listener.suspect((Address) evt.getArg());
                break;

            case Event.BLOCK:
                if(membership_listener != null)
                    membership_listener.block();
                break;
            case Event.UNBLOCK:
                if(membership_listener != null)
                    membership_listener.unblock();
                break;
        }

        return null;
    }




    class ProtocolAdapter extends Protocol implements UpHandler {


        /* ------------------------- Protocol Interface --------------------------- */

        @Override
        public String getName() {
            return "MessageDispatcher";
        }


        /**
         * Called by channel (we registered before) when event is received. This is the UpHandler interface.
         */
        @Override
        public Object up(Event evt) {
            if(corr != null) {
                if(!corr.receive(evt)) {
                    try {
                        return handleUpEvent(evt);
                    }
                    catch(Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            }
            return null;
        }



        @Override
        public Object down(Event evt) {
            if(channel != null) {
                if(evt.getType() == Event.MSG && !(channel.isConnected() || channel.isConnecting()))
                    throw new IllegalStateException("channel is not connected");
                return channel.down(evt);
            }
            return null;
        }


        /* ----------------------- End of Protocol Interface ------------------------ */

    }



}
