
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.blocks.mux.Muxer;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.*;


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
public class MessageDispatcher implements RequestHandler {
    protected Channel channel=null;
    protected RequestCorrelator corr=null;
    protected MessageListener msg_listener=null;
    protected MembershipListener membership_listener=null;
    protected RequestHandler req_handler=null;
    protected ProtocolAdapter prot_adapter=null;
    protected TransportAdapter transport_adapter=null;
    protected final Collection<Address> members=new TreeSet<Address>();
    protected Address local_addr=null;
    protected PullPushAdapter adapter=null;
    protected PullPushHandler handler=null;
    protected Serializable id=null;
    protected final Log log=LogFactory.getLog(getClass());
    protected boolean hardware_multicast_supported=false;


    public MessageDispatcher() {
    }

    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2) {
        this.channel=channel;
        prot_adapter=new ProtocolAdapter();
        if(channel != null) {
            local_addr=channel.getAddress();
        }
        setMessageListener(l);
        setMembershipListener(l2);
        if(channel != null) {
            installUpHandler(prot_adapter, true);
        }
        start();
    }

    @Deprecated
    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, boolean deadlock_detection) {
        this.channel=channel;
        prot_adapter=new ProtocolAdapter();
        if(channel != null) {
            local_addr=channel.getAddress();
        }
        setMessageListener(l);
        setMembershipListener(l2);
        if(channel != null) {
            installUpHandler(prot_adapter, true);
        }
        start();
    }

    @Deprecated
    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2,
                             boolean deadlock_detection, boolean concurrent_processing) {
        this.channel=channel;
        prot_adapter=new ProtocolAdapter();
        if(channel != null) {
            local_addr=channel.getAddress();
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


    @Deprecated
    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, RequestHandler req_handler,
                             boolean deadlock_detection) {
        this(channel, l, l2, deadlock_detection, false);
        setRequestHandler(req_handler);
    }

    @Deprecated
    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, RequestHandler req_handler,
                             boolean deadlock_detection, boolean concurrent_processing) {
        this(channel, l, l2, deadlock_detection, concurrent_processing);
        setRequestHandler(req_handler);
    }


    /*
     * Uses a user-provided PullPushAdapter rather than a Channel as transport. If id is non-null, it will be
     * used to register under that id. This is typically used when another building block is already using
     * PullPushAdapter, and we want to add this building block in addition. The id is the used to discriminate
     * between messages for the various blocks on top of PullPushAdapter. If null, we will assume we are the
     * first block created on PullPushAdapter.
     * @param adapter The PullPushAdapter which to use as underlying transport
     * @param id A serializable object (e.g. an Integer) used to discriminate (multiplex/demultiplex) between
     *           requests/responses for different building blocks on top of PullPushAdapter.
     */
    @Deprecated
    public MessageDispatcher(PullPushAdapter adapter, Serializable id, MessageListener l, MembershipListener l2) {
        this.adapter=adapter;
        this.id=id;
        setMembers(((Channel) adapter.getTransport()).getView().getMembers());
        setMessageListener(l);
        setMembershipListener(l2);
        handler=new PullPushHandler();
        transport_adapter=new TransportAdapter();
        adapter.addMembershipListener(handler); // remove in stop()
        if(id == null) { // no other building block around, let's become the main consumer of this PullPushAdapter
            adapter.setListener(handler);
        }
        else {
            adapter.registerListener(id, handler);
        }

        Transport tp;
        if((tp=adapter.getTransport()) instanceof Channel) {
            local_addr=((Channel) tp).getAddress();
        }
        start();
    }


    /*
     * Uses a user-provided PullPushAdapter rather than a Channel as transport. If id is non-null, it will be
     * used to register under that id. This is typically used when another building block is already using
     * PullPushAdapter, and we want to add this building block in addition. The id is the used to discriminate
     * between messages for the various blocks on top of PullPushAdapter. If null, we will assume we are the
     * first block created on PullPushAdapter.
     * @param adapter The PullPushAdapter which to use as underlying transport
     * @param id A serializable object (e.g. an Integer) used to discriminate (multiplex/demultiplex) between
     *           requests/responses for different building blocks on top of PullPushAdapter.
     * @param req_handler The object implementing RequestHandler. It will be called when a request is received
     */
    @Deprecated
    public MessageDispatcher(PullPushAdapter adapter, Serializable id,
                             MessageListener l, MembershipListener l2,
                             RequestHandler req_handler) {
        this.adapter=adapter;
        this.id=id;
        setMembers(((Channel) adapter.getTransport()).getView().getMembers());
        setRequestHandler(req_handler);
        setMessageListener(l);
        setMembershipListener(l2);
        handler=new PullPushHandler();
        transport_adapter=new TransportAdapter();
        adapter.addMembershipListener(handler);
        if(id == null) { // no other building block around, let's become the main consumer of this PullPushAdapter
            adapter.setListener(handler);
        }
        else {
            adapter.registerListener(id, handler);
        }

        Transport tp;
        if((tp=adapter.getTransport()) instanceof Channel) {
            local_addr=((Channel) tp).getAddress(); // fixed bug #800774
        }

        start();
    }

    @Deprecated
    public MessageDispatcher(PullPushAdapter adapter, Serializable id,
                             MessageListener l, MembershipListener l2,
                             RequestHandler req_handler, boolean concurrent_processing) {
        this.adapter=adapter;
        this.id=id;
        setMembers(((Channel) adapter.getTransport()).getView().getMembers());
        setRequestHandler(req_handler);
        setMessageListener(l);
        setMembershipListener(l2);
        handler=new PullPushHandler();
        transport_adapter=new TransportAdapter();
        adapter.addMembershipListener(handler);
        if(id == null) { // no other building block around, let's become the main consumer of this PullPushAdapter
            adapter.setListener(handler);
        }
        else {
            adapter.registerListener(id, handler);
        }

        Transport tp;
        if((tp=adapter.getTransport()) instanceof Channel) {
            local_addr=((Channel) tp).getAddress(); // fixed bug #800774
        }

        start();
    }


    public UpHandler getProtocolAdapter() {
        return prot_adapter;
    }


    /** Returns a copy of members */
    protected Collection getMembers() {
        synchronized(members) {
            return new ArrayList(members);
        }
    }


    /**
     * If this dispatcher is using a user-provided PullPushAdapter, then need to set the members from the adapter
     * initially since viewChange has most likely already been called in PullPushAdapter.
     */
    private void setMembers(Vector new_mbrs) {
        if(new_mbrs != null) {
            synchronized(members) {
                members.clear();
                members.addAll(new_mbrs);
            }
        }
    }

    @Deprecated
    public boolean getDeadlockDetection() {return false;}

    @Deprecated
    public void setDeadlockDetection(boolean flag) {
    }


    @Deprecated
    public boolean getConcurrentProcessing() {return false;}

    @Deprecated
    public void setConcurrentProcessing(boolean flag) {
    }


    public void start() {
        if(corr == null) {
            if(transport_adapter != null) {
                corr=createRequestCorrelator(transport_adapter, this, local_addr);
            }
            else {
                corr=createRequestCorrelator(prot_adapter, this, local_addr);
            }
        }
        correlatorStarted();
        corr.start();

        if(channel != null) {
            Vector tmp_mbrs=channel.getView() != null ? channel.getView().getMembers() : null;
            setMembers(tmp_mbrs);
            if(channel instanceof JChannel) {
                TP transport=channel.getProtocolStack().getTransport();
                corr.registerProbeHandler(transport);
            }
            TP transport=channel.getProtocolStack().getTransport();
            hardware_multicast_supported=transport.supportsMulticasting();
        }
    }

    protected RequestCorrelator createRequestCorrelator(Object transport, RequestHandler handler, Address local_addr) {
        return new RequestCorrelator(transport, handler, local_addr);
    }

    protected void correlatorStarted() {
        ;
    }


    public void stop() {
        if(corr != null) {
            corr.stop();
        }

        if(channel instanceof JChannel) {
            TP transport=channel.getProtocolStack().getTransport();
            corr.unregisterProbeHandler(transport);
        }

        // fixes leaks of MembershipListeners (http://jira.jboss.com/jira/browse/JGRP-160)
        if(adapter != null && handler != null) {
            adapter.removeMembershipListener(handler);
        }
    }


    public final void setMessageListener(MessageListener l) {
        msg_listener=l;
    }

    /**
     * Gives access to the currently configured MessageListener. Returns null if there is no
     * configured MessageListener.
     */
    public MessageListener getMessageListener() {
        return msg_listener;
    }

    public final void setMembershipListener(MembershipListener l) {
        membership_listener=l;
    }

    public final void setRequestHandler(RequestHandler rh) {
        req_handler=rh;
    }

    /**
     * Offers access to the underlying Channel.
     * @return a reference to the underlying Channel.
     */
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

    @Deprecated
    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException {
        if(channel != null) {
            channel.send(msg);
            return;
        }
        if(adapter != null) {
            try {
                if(id != null)
                    adapter.send(id, msg);
                else
                    adapter.send(msg);
            }
            catch(Throwable ex) {
                log.error("exception=" + Util.print(ex));
            }
        }
        else {
            log.error("channel == null");
        }
    }

    @Deprecated
    public RspList castMessage(final Vector dests, Message msg, int mode, long timeout) {
        return castMessage(dests, msg, new RequestOptions(mode, timeout, false, null));
    }


    @Deprecated
    public RspList castMessage(final Vector dests, Message msg, int mode, long timeout, boolean use_anycasting) {
        return castMessage(dests, msg, new RequestOptions(mode, timeout, use_anycasting, null));
    }

    // used by Infinispan
    @Deprecated
    /**
     * @deprecated Use {@link #castMessage(java.util.Collection, org.jgroups.Message, RequestOptions)} instead
     */
    public RspList castMessage(final Vector dests, Message msg, int mode, long timeout, boolean use_anycasting,
                               RspFilter filter) {
        RequestOptions opts=new RequestOptions(mode, timeout, use_anycasting, filter);
        return castMessage(dests, msg, opts);
    }

    /**
     * Sends a message to the members listed in dests. If dests is null, the message is sent to all current group
     * members.
     * @param dests A list of group members. The message is sent to all members of the current group if null
     * @param msg The message to be sent
     * @param options A set of options that govern the call. See {@link org.jgroups.blocks.RequestOptions} for details
     * @return
     * @since 2.9
     */
    public RspList castMessage(final Collection<Address> dests, Message msg, RequestOptions options) {
        GroupRequest req=cast(dests, msg, options, true);
        return req != null? req.getResults() : RspList.EMPTY_RSP_LIST;
    }

    @Deprecated
    public NotifyingFuture<RspList> castMessageWithFuture(final Vector dests, Message msg, int mode, long timeout, boolean use_anycasting,
                                                          RspFilter filter) {
        return castMessageWithFuture(dests, msg, new RequestOptions(mode, timeout, use_anycasting, filter));
    }

    public NotifyingFuture<RspList> castMessageWithFuture(final Collection<Address> dests, Message msg, RequestOptions options) {
        GroupRequest req=cast(dests, msg, options, false);
        return req != null? req : new NullFuture(RspList.EMPTY_RSP_LIST);
    }

    protected GroupRequest cast(final Collection<Address> dests, Message msg, RequestOptions options, boolean block_for_results) {
        List<Address> real_dests;

        // we need to clone because we don't want to modify the original
        // (we remove ourselves if LOCAL is false, see below) !
        // real_dests=dests != null ? (Vector) dests.clone() : (members != null ? new Vector(members) : null);
        if(dests != null) {
            real_dests=new ArrayList<Address>(dests);
            real_dests.retainAll(this.members);
        }
        else {
            synchronized(members) {
                real_dests=new ArrayList(members);
            }
        }

        // if local delivery is off, then we should not wait for the message from the local member.
        // therefore remove it from the membership
        Channel tmp=channel;
        if(tmp == null) {
            if(adapter != null && adapter.getTransport() instanceof Channel) {
                tmp=(Channel) adapter.getTransport();
            }
        }

        if(tmp != null && tmp.getOpt(Channel.LOCAL).equals(Boolean.FALSE)) {
            if(local_addr == null) {
                local_addr=tmp.getAddress();
            }
            if(local_addr != null) {
                real_dests.remove(local_addr);
            }
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

        GroupRequest req=new GroupRequest(msg, corr, real_dests, options);
        if(options != null) {
            req.setResponseFilter(options.getRspFilter());
            req.setAnycasting(options.getAnycasting());
        }
        req.setBlockForResults(block_for_results);

        try {
            req.execute();
            return req;
        }
        catch(Exception ex) {
            throw new RuntimeException("failed executing request " + req, ex);
        }
    }



    public void done(long req_id) {
        corr.done(req_id);
    }


    /**
     * Sends a message to a single member (destination = msg.dest) and returns the response. The message's destination
     * must be non-zero !
     * @deprecated Use {@link #sendMessage(org.jgroups.Message, RequestOptions)} instead
     */
    @Deprecated
    public Object sendMessage(Message msg, int mode, long timeout) throws TimeoutException, SuspectedException {
        return sendMessage(msg, new RequestOptions(mode, timeout, false, null));
    }


    public Object sendMessage(Message msg, RequestOptions opts) throws TimeoutException, SuspectedException {
        Address dest=msg.getDest();
        if(dest == null) {
            if(log.isErrorEnabled())
                log.error("the message's destination is null, cannot send message");
            return null;
        }

        UnicastRequest req=new UnicastRequest(msg, corr, dest, opts);
        try {
            req.execute();
        }
        catch(Exception t) {
            throw new RuntimeException("failed executing request " + req, t);
        }

        if(opts.getMode() == Request.GET_NONE)
            return null;

        Rsp rsp=req.getResult();
        if(rsp.wasSuspected())
            throw new SuspectedException(dest);
        if(!rsp.wasReceived())
            throw new TimeoutException("timeout sending message to " + dest);
        return rsp.getValue();
    }

    @Deprecated
    public <T> NotifyingFuture<T> sendMessageWithFuture(Message msg, int mode, long timeout) throws TimeoutException, SuspectedException {
        return sendMessageWithFuture(msg, new RequestOptions(mode, timeout, false, null));
    }

    public <T> NotifyingFuture<T> sendMessageWithFuture(Message msg, RequestOptions options) throws TimeoutException, SuspectedException {
        Address dest=msg.getDest();
        if(dest == null) {
            if(log.isErrorEnabled())
                log.error("the message's destination is null, cannot send message");
            return null;
        }

        UnicastRequest req=new UnicastRequest(msg, corr, dest, options);
        req.setBlockForResults(false);
        try {
            req.execute();
            if(options.getMode() == Request.GET_NONE)
                return new NullFuture(null);
            return req;
        }
        catch(Exception t) {
            throw new RuntimeException("failed executing request " + req, t);
        }
    }



    /* ------------------------ RequestHandler Interface ---------------------- */
    public Object handle(Message msg) {
        if(req_handler != null) {
            return req_handler.handle(msg);
        }
        else {
            return null;
        }
    }
    /* -------------------- End of RequestHandler Interface ------------------- */






    class ProtocolAdapter extends Protocol implements UpHandler {


        /* ------------------------- Protocol Interface --------------------------- */

        public String getName() {
            return "MessageDispatcher";
        }



        private Object handleUpEvent(Event evt) {
            switch(evt.getType()) {
                case Event.MSG:
                    if(msg_listener != null) {
                        msg_listener.receive((Message) evt.getArg());
                    }
                    break;

                case Event.GET_APPLSTATE: // reply with GET_APPLSTATE_OK
                    StateTransferInfo info=(StateTransferInfo)evt.getArg();
                    String state_id=info.state_id;
                    byte[] tmp_state=null;
                    if(msg_listener != null) {
                        try {
                            if(msg_listener instanceof ExtendedMessageListener && state_id!=null) {
                                tmp_state=((ExtendedMessageListener)msg_listener).getState(state_id);
                            }
                            else {
                                tmp_state=msg_listener.getState();
                            }
                        }
                        catch(Throwable t) {
                            this.log.error("failed getting state from message listener (" + msg_listener + ')', t);
                        }
                    }
                    return new StateTransferInfo(null, state_id, 0L, tmp_state);

                case Event.GET_STATE_OK:
                    if(msg_listener != null) {
                        try {
                            info=(StateTransferInfo)evt.getArg();
                            String id=info.state_id;
                            if(msg_listener instanceof ExtendedMessageListener && id!=null) {
                                ((ExtendedMessageListener)msg_listener).setState(id, info.state);
                            }
                            else {
                                msg_listener.setState(info.state);
                            }
                        }
                        catch(ClassCastException cast_ex) {
                            if(this.log.isErrorEnabled())
                                this.log.error("received SetStateEvent, but argument " +
                                        evt.getArg() + " is not serializable. Discarding message.");
                        }
                    }
                    break;

                case Event.STATE_TRANSFER_OUTPUTSTREAM:
                    StateTransferInfo sti=(StateTransferInfo)evt.getArg();
                    OutputStream os=sti.outputStream;
                    if(msg_listener instanceof ExtendedMessageListener) {                        
                        if(os != null && msg_listener instanceof ExtendedMessageListener) {
                            if(sti.state_id == null)
                                ((ExtendedMessageListener)msg_listener).getState(os);
                            else
                                ((ExtendedMessageListener)msg_listener).getState(sti.state_id, os);
                        }
                        return new StateTransferInfo(null, os, sti.state_id);
                    }
                    else if(msg_listener instanceof MessageListener){
                        if(log.isWarnEnabled()){
                            log.warn("Channel has STREAMING_STATE_TRANSFER, however,"
                                    + " application does not implement ExtendedMessageListener. State is not transfered");
                            Util.close(os);
                        }
                    }
                    break;

                case Event.STATE_TRANSFER_INPUTSTREAM:
                    sti=(StateTransferInfo)evt.getArg();
                    InputStream is=sti.inputStream;
                    if(msg_listener instanceof ExtendedMessageListener) {                    	
                        if(is!=null && msg_listener instanceof ExtendedMessageListener) {
                            if(sti.state_id == null)
                                ((ExtendedMessageListener)msg_listener).setState(is);
                            else
                                ((ExtendedMessageListener)msg_listener).setState(sti.state_id, is);
                        }
                    }
                    else if(msg_listener instanceof MessageListener){
                        if(log.isWarnEnabled()){
                            log.warn("Channel has STREAMING_STATE_TRANSFER, however,"
                                    + " application does not implement ExtendedMessageListener. State is not transfered");
                            Util.close(is);
                        }
                    }
                    break;

                case Event.VIEW_CHANGE:
                    View v=(View) evt.getArg();
                    Vector new_mbrs=v.getMembers();
                    setMembers(new_mbrs);
                    if(membership_listener != null) {
                        membership_listener.viewAccepted(v);
                    }
                    break;

                case Event.SET_LOCAL_ADDRESS:
                    if(log.isTraceEnabled())
                        log.trace("setting local_addr (" + local_addr + ") to " + evt.getArg());
                    local_addr=(Address)evt.getArg();
                    break;

                case Event.SUSPECT:
                    if(membership_listener != null) {
                        membership_listener.suspect((Address) evt.getArg());
                    }
                    break;

                case Event.BLOCK:
                    if(membership_listener != null) {
                        membership_listener.block();
                    }
                    channel.blockOk();
                    break;
                case Event.UNBLOCK:
                    if(membership_listener instanceof ExtendedMembershipListener) {
                        ((ExtendedMembershipListener)membership_listener).unblock();
                    }
                    break;
            }

            return null;
        }






        /**
         * Called by channel (we registered before) when event is received. This is the UpHandler interface.
         */
        public Object up(Event evt) {
            if(corr != null) {
                if(!corr.receive(evt)) {
                    return handleUpEvent(evt);
                }
            }
            else {
                if(log.isErrorEnabled()) { //Something is seriously wrong, correlator should not be null since latch is not locked!
                    log.error("correlator is null, event will be ignored (evt=" + evt + ")");
                }
            }
            return null;
        }



        public Object down(Event evt) {
            if(channel != null) {
                return channel.downcall(evt);
            }
            else
                if(this.log.isWarnEnabled()) {
                    this.log.warn("channel is null, discarding event " + evt);
                }
            return null;
        }


        /* ----------------------- End of Protocol Interface ------------------------ */

    }

    @Deprecated
    class TransportAdapter implements Transport {

        public void send(Message msg) throws Exception {
            if(channel != null) {
                channel.send(msg);
            }
            else
                if(adapter != null) {
                    try {
                        if(id != null) {
                            adapter.send(id, msg);
                        }
                        else {
                            adapter.send(msg);
                        }
                    }
                    catch(Throwable ex) {
                        if(log.isErrorEnabled()) {
                            log.error("exception=" + Util.print(ex));
                        }
                    }
                }
                else {
                    if(log.isErrorEnabled()) {
                        log.error("channel == null");
                    }
                }
        }

        public Object receive(long timeout) throws Exception {
            return null; // not supported and not needed
        }
    }

    @Deprecated
    class PullPushHandler implements ExtendedMessageListener, MembershipListener {


        /* ------------------------- MessageListener interface ---------------------- */
        public void receive(Message msg) {
            boolean consumed=false;
            if(corr != null) {
                consumed=corr.receiveMessage(msg);
            }

            if(!consumed) {   // pass on to MessageListener
                if(msg_listener != null) {
                    msg_listener.receive(msg);
                }
            }
        }

        public byte[] getState() {
            return msg_listener != null ? msg_listener.getState() : null;
        }

        public byte[] getState(String state_id) {
            if(msg_listener == null) return null;
            if(msg_listener instanceof ExtendedMessageListener && state_id!=null) {
                return ((ExtendedMessageListener)msg_listener).getState(state_id);
            }
            else {
                return msg_listener.getState();
            }
        }

        public void setState(byte[] state) {
            if(msg_listener != null) {
                msg_listener.setState(state);
            }
        }

        public void setState(String state_id, byte[] state) {
            if(msg_listener != null) {
                if(msg_listener instanceof ExtendedMessageListener && state_id!=null) {
                    ((ExtendedMessageListener)msg_listener).setState(state_id, state);
                }
                else {
                    msg_listener.setState(state);
                }
            }
        }

        public void getState(OutputStream ostream) {
            if (msg_listener instanceof ExtendedMessageListener) {
                ((ExtendedMessageListener) msg_listener).getState(ostream);
            }
        }

        public void getState(String state_id, OutputStream ostream) {
            if (msg_listener instanceof ExtendedMessageListener && state_id!=null) {
                ((ExtendedMessageListener) msg_listener).getState(state_id,ostream);
            }

        }

        public void setState(InputStream istream) {
            if (msg_listener instanceof ExtendedMessageListener) {
                ((ExtendedMessageListener) msg_listener).setState(istream);
            }
        }

        public void setState(String state_id, InputStream istream) {
            if (msg_listener instanceof ExtendedMessageListener && state_id != null) {
                ((ExtendedMessageListener) msg_listener).setState(state_id,istream);
            }
        }
        /*
		 * --------------------- End of MessageListener interface
		 * -------------------
		 */


        /* ------------------------ MembershipListener interface -------------------- */
        public void viewAccepted(View v) {
            if(corr != null) {
                corr.receiveView(v);
            }

            Vector new_mbrs=v.getMembers();
            setMembers(new_mbrs);
            if(membership_listener != null) {
                membership_listener.viewAccepted(v);
            }
        }

        public void suspect(Address suspected_mbr) {
            if(corr != null) {
                corr.receiveSuspect(suspected_mbr);
            }
            if(membership_listener != null) {
                membership_listener.suspect(suspected_mbr);
            }
        }

        public void block() {
            if(membership_listener != null) {
                membership_listener.block();
            }
        }

        /* --------------------- End of MembershipListener interface ---------------- */



        // @todo: receive SET_LOCAL_ADDR event and call corr.setLocalAddress(addr)

    }


}
