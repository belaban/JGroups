
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.*;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;


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
public class MessageDispatcher implements RequestHandler, Closeable, ChannelListener {
    protected JChannel                              channel;
    protected RequestCorrelator                     corr;
    protected Receiver                              receiver;
    protected RequestHandler                        req_handler;
    protected boolean                               async_dispatching;
    /** When enabled, responses are handled by the common ForkJoinPool (https://issues.redhat.com/browse/JGRP-2644) */
    protected boolean                               async_rsp_handling=!Util.virtualThreadsAvailable();
    protected boolean                               wrap_exceptions;
    protected ProtocolAdapter                       prot_adapter;
    protected volatile Collection<Address>          members=new HashSet<>();
    protected Address                               local_addr;
    protected final Log                             log=LogFactory.getLog(MessageDispatcher.class);
    @SuppressWarnings("rawtypes")
    protected static final RspList                  empty_rsplist;
    @SuppressWarnings("rawtypes")
    protected static final GroupRequest             empty_group_request;

    static {
        empty_rsplist=new RspList<>();
        empty_group_request=new GroupRequest<>(null, Collections.emptyList(), RequestOptions.SYNC());
        empty_group_request.complete(empty_rsplist);
    }


    public MessageDispatcher() {
    }

    public MessageDispatcher(JChannel channel) {
        this.channel=channel;
        prot_adapter=new ProtocolAdapter();
        if(channel != null) {
            channel.addChannelListener(this);
            local_addr=channel.getAddress();
            installUpHandler(prot_adapter, true);
            Protocol top_prot=channel.stack() != null? channel.stack().getTopProtocol() : null;
            if(top_prot != null)
                prot_adapter.setDownProt(top_prot);
        }
        start();
    }


    public MessageDispatcher(JChannel channel, RequestHandler req_handler) {
        this(channel);
        setRequestHandler(req_handler);
    }


    public JChannel          getChannel()                 {return channel;}
    public RequestCorrelator getCorrelator()              {return corr;}
    public RequestCorrelator correlator()                 {return corr;}
    public boolean           getAsyncDispatching()        {return async_dispatching;}
    public boolean           asyncDispatching()           {return async_dispatching;}
    public boolean           asyncRspHandling()           {return async_rsp_handling;}
    public MessageDispatcher asyncRspHandling(boolean f)  {async_rsp_handling=f;
                                                           if(corr != null) corr.asyncRspHandling(async_rsp_handling);
                                                           return this;}
    public boolean           getWrapExceptions()          {return wrap_exceptions;}
    public boolean           wrapExceptions()             {return wrap_exceptions;}
    public UpHandler         getProtocolAdapter()         {return prot_adapter;}
    public UpHandler         protocolAdapter()            {return prot_adapter;}
    public RpcStats          rpcStats()                   {return corr.rpc_stats;}

    public <X extends MessageDispatcher> X setChannel(JChannel ch) {
        if(ch == null)
            return (X)this;
        this.channel=ch;
        if(ch != null) {
            local_addr=channel.getAddress();
            ch.addChannelListener(this);
        }
        if(prot_adapter == null)
            prot_adapter=new ProtocolAdapter();
        // Don't force installing the UpHandler so subclasses can use this method
        return installUpHandler(prot_adapter, false);
    }

    public <X extends MessageDispatcher> X setCorrelator(RequestCorrelator c) {return correlator(c);}
    public <X extends MessageDispatcher> X correlator(RequestCorrelator c) {
        if(c == null)
            return (X)this;
        stop();
        this.corr=c;
        corr.asyncDispatching(this.async_dispatching).asyncRspHandling(async_rsp_handling)
          .wrapExceptions(this.wrap_exceptions);
        start();
        return (X)this;
    }

    public <X extends MessageDispatcher> X setReceiver(Receiver r) {
        this.receiver=r;
        return (X)this;
    }

    public <X extends MessageDispatcher> X setRequestHandler(RequestHandler rh) {
        req_handler=rh;
        if(corr != null)
            corr.setRequestHandler(rh);
        return (X)this;
    }

    public <X extends MessageDispatcher> X setAsynDispatching(boolean flag) {return asyncDispatching(flag);}
    public <X extends MessageDispatcher> X asyncDispatching(boolean flag) {
        async_dispatching=flag;
        if(corr != null)
            corr.asyncDispatching(flag);
        return (X)this;
    }

    public <X extends MessageDispatcher> X setWrapExceptions(boolean flag) {return wrapExceptions(flag);}
    public <X extends MessageDispatcher> X wrapExceptions(boolean flag) {
        wrap_exceptions=flag;
        if(corr != null)
            corr.wrapExceptions(flag);
        return (X)this;
    }

    protected <X extends MessageDispatcher> X setMembers(List<Address> new_mbrs) {
        if(new_mbrs != null)
            members=new HashSet<>(new_mbrs); // volatile write - seen by a subsequent read
        return (X)this;
    }


    public <X extends MessageDispatcher> X start() {
        if(corr == null)
            corr=createRequestCorrelator(prot_adapter, this, local_addr)
              .asyncDispatching(async_dispatching).asyncRspHandling(async_rsp_handling)
              .wrapExceptions(this.wrap_exceptions);
        corr.start();

        if(channel != null) {
            List<Address> tmp_mbrs=channel.getView() != null ? channel.getView().getMembers() : null;
            setMembers(tmp_mbrs);
            if(channel instanceof JChannel) {
                TP transport=channel.getProtocolStack().getTransport();
                corr.registerProbeHandler(transport);
            }
        }
        return (X)this;
    }

    protected static RequestCorrelator createRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr) {
        return new RequestCorrelator(transport, handler, local_addr);
    }


    @Override public void close() throws IOException {stop();}

    public <X extends MessageDispatcher> X stop() {
        if(corr != null) {
            corr.stop();
            if(channel instanceof JChannel) {
                TP transport=channel.getProtocolStack().getTransport();
                corr.unregisterProbeHandler(transport);
            }
        }
        return (X)this;
    }


    /**
     * Sets the given UpHandler as the UpHandler for the channel. If the relevant handler is already installed,
     * the {@code canReplace} controls whether this method replaces it (after logging a WARN) or simply
     * leaves {@code handler} uninstalled.<p>
     * Passing {@code false} as the {@code canReplace} value allows callers to use this method to install defaults
     * without concern about inadvertently overriding
     *
     * @param handler the UpHandler to install
     * @param canReplace {@code true} if an existing Channel upHandler can be replaced; {@code false}
     *              if this method shouldn't install
     */
    protected <X extends MessageDispatcher> X installUpHandler(UpHandler handler, boolean canReplace) {
        UpHandler existing = channel.getUpHandler();
        if (existing == null)
            channel.setUpHandler(handler);
        else if(canReplace) {
            log.warn("Channel already has an up handler installed (%s) but now it is being overridden", existing);
            channel.setUpHandler(handler);
        }
        return (X)this;
    }


    /**
     * Sends a message to all members and expects responses from members in dests (if non-null).
     * @param dests A list of group members from which to expect responses (if the call is blocking).
     * @param msg The message to be sent
     * @param opts A set of options that govern the call. See {@link org.jgroups.blocks.RequestOptions} for details
     * @return RspList A list of Rsp elements, or null if the RPC is asynchronous
     * @throws Exception If the request cannot be sent
     * @since 2.9
     */
    public <T> RspList<T> castMessage(final Collection<Address> dests, Message msg, RequestOptions opts) throws Exception {
        GroupRequest<T> req=cast(dests, msg, opts, true);
        return req != null? req.getNow(null) : null;
    }

    /**
     * Sends a message to all members and expects responses from members in dests (if non-null).
     * @param dests A list of group members from which to expect responses (if the call is blocking).
     * @param msg The message to be sent
     * @param opts A set of options that govern the call. See {@link org.jgroups.blocks.RequestOptions} for details
     * @return CompletableFuture A future from which the results (RspList) can be retrieved, or null if the request
     *                              was sent asynchronously
     * @throws Exception If the request cannot be sent
     */
    public <T> CompletableFuture<RspList<T>> castMessageWithFuture(final Collection<Address> dests, Message msg,
                                                                   RequestOptions opts) throws Exception {
        return cast(dests, msg, opts,false);
    }


    protected <T> GroupRequest<T> cast(final Collection<Address> dests, Message msg, RequestOptions options,
                                       boolean block_for_results) throws Exception {
        if(options == null) {
            log.warn("request options were null, using default of sync");
            options=RequestOptions.SYNC();
        }

        List<Address> real_dests;
        // we need to clone because we don't want to modify the original
        if(dests != null) {
            real_dests=new FastArray<>(dests);
            real_dests.removeIf(addr -> !this.members.contains(addr) && !(addr instanceof SiteAddress));
        }
        else
            real_dests=new FastArray<>(members);

        // Remove the local member from the target destination set if we should not deliver our own message
        JChannel tmp=channel;
        if((tmp != null && tmp.getDiscardOwnMessages()) || options.transientFlagSet(Message.TransientFlag.DONT_LOOPBACK)) {
            if(local_addr == null)
                local_addr=tmp != null? tmp.getAddress() : null;
            real_dests.remove(local_addr);
        }

        if(options.hasExclusionList())
            Stream.of(options.exclusionList()).forEach(real_dests::remove);

        if(real_dests.isEmpty()) {
            log.trace("destination list is empty, won't send message");
            return empty_group_request;
        }

        if(options.mode() == ResponseMode.GET_NONE) {
            corr.sendMulticastRequest(real_dests, msg, null, options);
            return null;
        }

        GroupRequest<T> req=new GroupRequest<>(corr, real_dests, options);
        req.execute(msg, block_for_results);
        return req;
    }


    /**
     * Sends a unicast message and - depending on the options - returns a result
     * @param msg the payload to send
     * @param opts the options to be used
     * @return T the result. Null if the call is asynchronous (non-blocking) or if the response is null
     * @throws Exception If there was problem sending the request, processing it at the receiver, or processing
     *                   it at the sender.
     * @throws TimeoutException If the call didn't succeed within the timeout defined in options (if set)
     */
    public <T> T sendMessage(Message msg, RequestOptions opts) throws Exception {
        UnicastRequest<T> req=_sendMessage(msg, opts);
        return req != null? req.execute(msg, true) : null;
    }


    /**
     * Sends a unicast message to the target defined by msg.getDest() and returns a future
     * @param msg the payload to send
     * @param opts the options
     * @return CompletableFuture A future from which the result can be fetched, or null if the call was asynchronous
     * @throws Exception If there was problem sending the request, processing it at the receiver, or processing
     *                   it at the sender. {@link java.util.concurrent.Future#get()} will throw this exception
     */
    public <T> CompletableFuture<T> sendMessageWithFuture(Message msg, RequestOptions opts) throws Exception {
        UnicastRequest<T> req=_sendMessage(msg, opts);
        if(req != null)
            req.execute(msg, false);
        return req;
    }


    /* ------------------------ RequestHandler Interface ---------------------- */
    @Override
    public Object handle(Message msg) throws Exception {
        if(req_handler != null)
            return req_handler.handle(msg);
        return null;
    }

    @Override
    public void handle(Message request, Response response) throws Exception {
        if(req_handler != null) {
            if(async_dispatching)
                req_handler.handle(request, response);
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
    /* ------------------ End of RequestHandler Interface----------------- */



    protected <T> UnicastRequest<T> _sendMessage(Message msg, RequestOptions opts) throws Exception {
        Address dest=msg.getDest();
        if(dest == null)
            throw new IllegalArgumentException("message destination is null, cannot send message");

        if(opts == null) {
            log.warn("request options were null, using default of sync");
            opts=RequestOptions.SYNC();
        }

        // invoke an async RPC directly and return null, without creating a UnicastRequest instance
        if(opts.mode() == ResponseMode.GET_NONE) {
            corr.sendUnicastRequest(msg, null, opts);
            return null;
        }
        return new UnicastRequest<>(corr, dest, opts);
    }


    protected Object handleUpEvent(Event evt) throws Exception {
        switch(evt.getType()) {
            case Event.GET_APPLSTATE: // reply with GET_APPLSTATE_OK
                byte[] tmp_state=null;
                if(receiver != null) {
                    ByteArrayOutputStream output=new ByteArrayOutputStream(1024);
                    if(getState(output))
                        tmp_state=output.toByteArray();
                }
                return new StateTransferInfo(null, 0L, tmp_state);

            case Event.GET_STATE_OK:
                if(receiver != null) {
                    StateTransferResult result=evt.getArg();
                    if(result.hasBuffer()) {
                        ByteArrayInputStream input=new ByteArrayInputStream(result.getBuffer());
                        setState(input);
                    }
                }
                break;

            case Event.STATE_TRANSFER_OUTPUTSTREAM:
                OutputStream os=evt.getArg();
                getState(os);
                break;

            case Event.STATE_TRANSFER_INPUTSTREAM:
                InputStream is=evt.getArg();
                setState(is);
                break;

            case Event.VIEW_CHANGE:
                View v=evt.getArg();
                List<Address> new_mbrs=v.getMembers();
                setMembers(new_mbrs);
                if(receiver != null)
                    receiver.viewAccepted(v);
                break;
        }
        return null;
    }

    protected boolean getState(OutputStream out) throws Exception {
        if(receiver == null || out == null)
            return false;
        try {
            receiver.getState(out);
            return true;
        }
        catch(UnsupportedOperationException un) {
            return false;
        }
    }

    protected boolean setState(InputStream in) throws Exception {
        if(receiver == null || in == null)
            return false;
        try {
            receiver.setState(in);
            return true;
        }
        catch(UnsupportedOperationException un) {
            return false;
        }
    }

    public void channelDisconnected(JChannel channel) {
        stop();
    }

    public void channelClosed(JChannel channel) {
        stop();
    }


    protected class ProtocolAdapter extends Protocol implements UpHandler {


        /* ------------------------- Protocol Interface --------------------------- */

        @Override
        public String getName() {
            return "MessageDispatcher";
        }

        public <T extends Protocol> T setDownProt(Protocol d) {
            down_prot=d;
            return (T)this;
        }

        public <T extends Protocol> T setAddress(Address addr) {
            local_addr=addr;
            MessageDispatcher.this.local_addr=addr;
            if(corr != null)
                corr.setLocalAddress(addr);
            return (T)this;
        }

        public UpHandler setLocalAddress(Address a) {
            setAddress(a);
            return this;
        }

        /**
         * Called by channel (we registered before) when event is received. This is the UpHandler interface.
         */
        @Override
        public Object up(Event evt) {
            if(corr != null && !corr.receive(evt)) {
                try {
                    return handleUpEvent(evt);
                }
                catch(Throwable t) {
                    throw new RuntimeException(t);
                }
            }
            return null;
        }

        public Object up(Message msg) {
            if(corr != null)
                corr.receiveMessage(msg);
            return null;
        }

        public void up(MessageBatch batch) {
            if(corr == null)
                return;
            corr.receiveMessageBatch(batch);
        }

        @Override
        public Object down(Event evt) {
            return channel != null? channel.down(evt) : null;
        }

        public Object down(Message msg) {
            if(channel != null) {
                if(!(channel.isConnected() || channel.isConnecting())) {
                    // return null;
                    throw new IllegalStateException("channel is not connected");
                }
                return channel.down(msg);
            }
            return null;
        }

        /* ----------------------- End of Protocol Interface ------------------------ */

    }



}
