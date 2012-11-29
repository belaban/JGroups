
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.blocks.mux.Muxer;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;


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
 * Used on top of channel to implement group requests. Client's <code>handle()</code>
 * method is called when request is received. Is the equivalent of RpcProtocol on
 * the application instead of protocol level.
 *
 * @author Bela Ban
 */
public class MessageDispatcher implements RequestHandler, ChannelListener {
    protected Channel channel=null;
    protected RequestCorrelator corr=null;
    protected MessageListener msg_listener=null;
    protected MembershipListener membership_listener=null;
    protected RequestHandler req_handler=null;
    protected ProtocolAdapter prot_adapter=null;
    protected volatile Collection<Address> members=new HashSet<Address>();
    protected Address local_addr=null;
    protected final Log log=LogFactory.getLog(getClass());
    protected boolean hardware_multicast_supported=false;
    protected final AtomicInteger sync_unicasts=new AtomicInteger(0);
    protected final AtomicInteger async_unicasts=new AtomicInteger(0);
    protected final AtomicInteger sync_multicasts=new AtomicInteger(0);
    protected final AtomicInteger async_multicasts=new AtomicInteger(0);
    protected final AtomicInteger sync_anycasts=new AtomicInteger(0);
    protected final AtomicInteger async_anycasts=new AtomicInteger(0);
    protected final Set<ChannelListener> channel_listeners=new CopyOnWriteArraySet<ChannelListener>();
    protected final DiagnosticsHandler.ProbeHandler probe_handler=new MyProbeHandler();


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
        if(channel != null) {
            installUpHandler(prot_adapter, true);
        }
        start();
    }




    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, RequestHandler req_handler) {
        this(channel, l, l2);
        setRequestHandler(req_handler);
    }




    public UpHandler getProtocolAdapter() {
        return prot_adapter;
    }



    /**
     * If this dispatcher is using a user-provided PullPushAdapter, then need to set the members from the adapter
     * initially since viewChange has most likely already been called in PullPushAdapter.
     */
    protected void setMembers(List<Address> new_mbrs) {
        if(new_mbrs != null)
            members=new HashSet<Address>(new_mbrs); // volatile write - seen by a subsequent read
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
            corr=createRequestCorrelator(prot_adapter, this, local_addr);
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
            transport.registerProbeHandler(probe_handler);
        }
    }

    protected RequestCorrelator createRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr) {
        return new RequestCorrelator(transport, handler, local_addr);
    }

    protected void correlatorStarted() {
        ;
    }


    public void stop() {
        if(corr != null)
            corr.stop();

        if(channel instanceof JChannel) {
            TP transport=channel.getProtocolStack().getTransport();
            transport.unregisterProbeHandler(probe_handler);
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
     * If the relevant handler is already installed, the <code>canReplace</code>
     * controls whether this method replaces it (after logging a WARN) or simply
     * leaves <code>handler</code> uninstalled.
     * <p>
     * Passing <code>false</code> as the <code>canReplace</code> value allows
     * callers to use this method to install defaults without concern about
     * inadvertently overriding
     * 
     * @param handler the UpHandler to install
     * @param canReplace <code>true</code> if an existing Channel upHandler or 
     *              Muxer default upHandler can be replaced; <code>false</code>
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
               log.warn("Channel Muxer already has a default up handler installed (" +
                     mux.getDefaultHandler() + ") but now it is being overridden"); 
               mux.setDefaultHandler(handler);
           }
       }
       else if (canReplace) {
           log.warn("Channel already has an up handler installed (" + existing + ") but now it is being overridden");
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
     * @return NotifyingFuture<T> A future from which the results (RspList) can be retrieved
     * @throws Exception If the request cannot be sent
     */
    public <T> NotifyingFuture<RspList<T>> castMessageWithFuture(final Collection<Address> dests,
                                                                 Message msg,
                                                                 RequestOptions options) throws Exception {
        GroupRequest<T> req=cast(dests, msg, options, false);
        return req != null? req : new NullFuture<RspList>(new RspList());
    }

    protected <T> GroupRequest<T> cast(final Collection<Address> dests, Message msg,
                                       RequestOptions options,
                                       boolean block_for_results) throws Exception {
        List<Address> real_dests;

        // we need to clone because we don't want to modify the original
        if(dests != null) {
            real_dests=new ArrayList<Address>();
            for(Address dest: dests) {
                if(dest instanceof SiteAddress || this.members.contains(dest)) {
                    if(!real_dests.contains(dest))
                        real_dests.add(dest);
                }
            }
        }
        else
            real_dests=new ArrayList<Address>(members);

        // if local delivery is off, then we should not wait for the message from the local member.
        // therefore remove it from the membership
        Channel tmp=channel;
        if(tmp != null && tmp.getDiscardOwnMessages()) {
            if(local_addr == null)
                local_addr=tmp.getAddress();
            if(local_addr != null)
                real_dests.remove(local_addr);
        }

        if(options != null && options.hasExclusionList()) {
            Collection<Address> exclusion_list=options.getExclusionList();
            real_dests.removeAll(exclusion_list);
        }

        // don't even send the message if the destination list is empty
        if(log.isTraceEnabled())
            log.trace("real_dests=" + real_dests);

        if(real_dests.isEmpty()) {
            if(log.isTraceEnabled())
                log.trace("destination list is empty, won't send message");
            return null;
        }

        if(options != null) {
            boolean async=options.getMode() == ResponseMode.GET_NONE;
            if(options.getAnycasting()) {
                if(async) async_anycasts.incrementAndGet();
                else sync_anycasts.incrementAndGet();
            }
            else {
                if(async) async_multicasts.incrementAndGet();
                else sync_multicasts.incrementAndGet();
            }
        }

        GroupRequest<T> req=new GroupRequest<T>(msg, corr, real_dests, options);
        if(options != null) {
            req.setResponseFilter(options.getRspFilter());
            req.setAnycasting(options.getAnycasting());
            msg.setFlag(options.getFlags());
            if(options.getScope() > 0)
                msg.setScope(options.getScope());
        }
        req.setBlockForResults(block_for_results);
        req.execute();
        return req;
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

        if(opts != null) {
            msg.setFlag(opts.getFlags());
            if(opts.getScope() > 0)
                msg.setScope(opts.getScope());
            if(opts.getMode() == ResponseMode.GET_NONE)
                async_unicasts.incrementAndGet();
            else
                sync_unicasts.incrementAndGet();
        }

        UnicastRequest<T> req=new UnicastRequest<T>(msg, corr, dest, opts);
        req.execute();

        if(opts != null && opts.getMode() == ResponseMode.GET_NONE)
            return null;

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
            throw new TimeoutException("timeout sending message to " + dest);
        return rsp.getValue();
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
        Address dest=msg.getDest();
        if(dest == null)
            throw new IllegalArgumentException("message destination is null, cannot send message");

        if(options != null) {
            msg.setFlag(options.getFlags());
            if(options.getScope() > 0)
                msg.setScope(options.getScope());
            if(options.getMode() == ResponseMode.GET_NONE)
                async_unicasts.incrementAndGet();
            else
                sync_unicasts.incrementAndGet();
        }

        UnicastRequest<T> req=new UnicastRequest<T>(msg, corr, dest, options);
        req.setBlockForResults(false);
        req.execute();
        if(options != null && options.getMode() == ResponseMode.GET_NONE)
            return new NullFuture<T>(null);
        return req;
    }



    /* ------------------------ RequestHandler Interface ---------------------- */
    public Object handle(Message msg) throws Exception {
        if(req_handler != null)
            return req_handler.handle(msg);
        return null;
    }
    /* -------------------- End of RequestHandler Interface ------------------- */





    /* --------------------- Interface ChannelListener ---------------------- */

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
                    ByteArrayInputStream input=new ByteArrayInputStream(result.getBuffer());
                    msg_listener.setState(input);
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
                if(log.isTraceEnabled())
                    log.trace("setting local_addr (" + local_addr + ") to " + evt.getArg());
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


    class MyProbeHandler implements DiagnosticsHandler.ProbeHandler {

        public Map<String,String> handleProbe(String... keys) {
            Map<String,String> retval=new HashMap<String,String>();
            for(String key: keys) {
                if("rpcs".equals(key)) {
                    String channel_name = channel != null ? channel.getClusterName() : "";
                    retval.put(channel_name + ": sync  unicast   RPCs", sync_unicasts.toString());
                    retval.put(channel_name + ": sync  multicast RPCs", sync_multicasts.toString());
                    retval.put(channel_name + ": async unicast   RPCs", async_unicasts.toString());
                    retval.put(channel_name + ": async multicast RPCs", async_multicasts.toString());
                    retval.put(channel_name + ": sync  anycast   RPCs", sync_anycasts.toString());
                    retval.put(channel_name + ": async anycast   RPCs", async_anycasts.toString());
                }
                if("rpcs-reset".equals(key)) {
                    sync_unicasts.set(0);
                    sync_multicasts.set(0);
                    async_unicasts.set(0);
                    async_multicasts.set(0);
                    sync_anycasts.set(0);
                    async_anycasts.set(0);
                }
            }
            return retval;
        }

        public String[] supportedKeys() {
            return new String[]{"rpcs", "rpcs-reset"};
        }
    }


    class ProtocolAdapter extends Protocol implements UpHandler {


        /* ------------------------- Protocol Interface --------------------------- */

        public String getName() {
            return "MessageDispatcher";
        }


        /**
         * Called by channel (we registered before) when event is received. This is the UpHandler interface.
         */
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
