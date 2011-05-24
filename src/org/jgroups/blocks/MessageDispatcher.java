
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
    protected final Collection<Address> members=new TreeSet<Address>();
    protected Address local_addr=null;
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




    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, RequestHandler req_handler) {
        this(channel, l, l2);
        setRequestHandler(req_handler);
    }




    public UpHandler getProtocolAdapter() {
        return prot_adapter;
    }


    /** Returns a copy of members */
    protected Collection<Address> getMembers() {
        synchronized(members) {
            return new ArrayList<Address>(members);
        }
    }


    /**
     * If this dispatcher is using a user-provided PullPushAdapter, then need to set the members from the adapter
     * initially since viewChange has most likely already been called in PullPushAdapter.
     */
    private void setMembers(List<Address> new_mbrs) {
        if(new_mbrs != null) {
            synchronized(members) {
                members.clear();
                members.addAll(new_mbrs);
            }
        }
    }



    public void start() {
        if(corr == null) {
            corr=createRequestCorrelator(prot_adapter, this, local_addr);
        }
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
        }
    }

    protected RequestCorrelator createRequestCorrelator(Object transport, RequestHandler handler, Address local_addr) {
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
            corr.unregisterProbeHandler(transport);
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



    /**
     * Sends a message to the members listed in dests. If dests is null, the message is sent to all current group
     * members.
     * @param dests A list of group members. The message is sent to all members of the current group if null
     * @param msg The message to be sent
     * @param options A set of options that govern the call. See {@link org.jgroups.blocks.RequestOptions} for details
     * @return
     * @since 2.9
     */
    public <T> RspList<T> castMessage(final Collection<Address> dests, Message msg, RequestOptions options) {
        GroupRequest<T> req=cast(dests, msg, options, true);
        return req != null? req.getResults() : RspList.EMPTY_RSP_LIST;
    }


    public <T> NotifyingFuture<RspList<T>> castMessageWithFuture(final Collection<Address> dests, Message msg, RequestOptions options) {
        GroupRequest<T> req=cast(dests, msg, options, false);
        return req != null? req : new NullFuture<RspList>(RspList.EMPTY_RSP_LIST);
    }

    protected <T> GroupRequest<T> cast(final Collection<Address> dests, Message msg, RequestOptions options, boolean block_for_results) {
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
                real_dests=new ArrayList<Address>(members);
            }
        }

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

        GroupRequest<T> req=new GroupRequest<T>(msg, corr, real_dests, options);
        if(options != null) {
            req.setResponseFilter(options.getRspFilter());
            req.setAnycasting(options.getAnycasting());
            msg.setFlag(options.getFlags());
            if(options.getScope() > 0)
                msg.setScope(options.getScope());
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



    public <T> T sendMessage(Message msg, RequestOptions opts) throws Throwable {
        Address dest=msg.getDest();
        if(dest == null) {
            if(log.isErrorEnabled())
                log.error("the message's destination is null, cannot send message");
            return null;
        }

        if(opts != null) {
            msg.setFlag(opts.getFlags());
            if(opts.getScope() > 0)
                msg.setScope(opts.getScope());
        }

        UnicastRequest<T> req=new UnicastRequest<T>(msg, corr, dest, opts);
        try {
            req.execute();
        }
        catch(Exception t) {
            throw new RuntimeException("failed executing request " + req, t);
        }

        if(opts != null && opts.getMode() == ResponseMode.GET_NONE)
            return null;

        Rsp<T> rsp=req.getResult();
        if(rsp.wasSuspected())
            throw new SuspectedException(dest);

        if(rsp.getException() != null)
            throw rsp.getException();

        if(!rsp.wasReceived())
            throw new TimeoutException("timeout sending message to " + dest);
        return rsp.getValue();
    }


    public <T> NotifyingFuture<T> sendMessageWithFuture(Message msg, RequestOptions options) throws TimeoutException, SuspectedException {
        Address dest=msg.getDest();
        if(dest == null) {
            if(log.isErrorEnabled())
                log.error("the message's destination is null, cannot send message");
            return null;
        }

        if(options != null) {
            msg.setFlag(options.getFlags());
            if(options.getScope() > 0)
                msg.setScope(options.getScope());
        }

        UnicastRequest<T> req=new UnicastRequest<T>(msg, corr, dest, options);
        req.setBlockForResults(false);
        try {
            req.execute();
            if(options != null && options.getMode() == ResponseMode.GET_NONE)
                return new NullFuture<T>(null);
            return req;
        }
        catch(Exception t) {
            throw new RuntimeException("failed executing request " + req, t);
        }
    }



    /* ------------------------ RequestHandler Interface ---------------------- */
    public Object handle(Message msg) throws Throwable {
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
                    byte[] tmp_state=null;
                    if(msg_listener != null) {
                        try {
                            tmp_state=msg_listener.getState();
                        }
                        catch(Throwable t) {
                            this.log.error("failed getting state from message listener (" + msg_listener + ')', t);
                        }
                    }
                    return new StateTransferInfo(null, 0L, tmp_state);

                case Event.GET_STATE_OK:
                    if(msg_listener != null) {
                        try {
                            info=(StateTransferInfo)evt.getArg();
                            msg_listener.setState(info.state);
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
                    if(msg_listener != null) {
                        if(os != null)
                            msg_listener.getState(os);
                        return new StateTransferInfo(null, os);
                    }
                    break;

                case Event.STATE_TRANSFER_INPUTSTREAM:
                    sti=(StateTransferInfo)evt.getArg();
                    InputStream is=sti.inputStream;
                    if(msg_listener != null && is!=null)
                        msg_listener.setState(is);
                    break;

                case Event.VIEW_CHANGE:
                    View v=(View) evt.getArg();
                    List<Address> new_mbrs=v.getMembers();
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
                return channel.down(evt);
            }
            else
                if(this.log.isWarnEnabled()) {
                    this.log.warn("channel is null, discarding event " + evt);
                }
            return null;
        }


        /* ----------------------- End of Protocol Interface ------------------------ */

    }



}
