// $Id: MessageDispatcher.java,v 1.6 2003/12/11 06:58:55 belaban Exp $

package org.jgroups.blocks;

import java.io.Serializable;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.log.Trace;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;





/**
 * Used on top of channel to implement group requests. Client's <code>handle()</code>
 * method is called when request is received. Is the equivalent of RpcProtocol on the
 * application instead of protocol level.
 * @author Bela Ban
 */
public class MessageDispatcher implements RequestHandler {
    protected Channel             channel=null;
    protected RequestCorrelator   corr=null;
    protected MessageListener     msg_listener=null;
    protected MembershipListener  membership_listener=null;
    protected RequestHandler      req_handler=null;
    protected ProtocolAdapter     prot_adapter=null;
    protected TransportAdapter    transport_adapter=null;
    protected Vector              members=new Vector();
    protected Address             local_addr=null;
    protected boolean             deadlock_detection=true;
    protected PullPushAdapter     adapter=null;
    protected Serializable        id=null;

    /** Process items on the queue concurrently (RequestCorrelator). The default is to wait until the
     * processing of an item has completed before fetching the next item from the queue. Note that setting
     * this to true may destroy the properties of a protocol stack, e.g total or causal order may not be
     * guaranteed. Set this to true only if you know what you're doing ! */
    protected boolean             concurrent_processing=false;




    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2) {
        this.channel=channel;
        prot_adapter=new ProtocolAdapter();
        if(channel != null) {
            local_addr=channel.getLocalAddress();
            channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        }
        setMessageListener(l);
        setMembershipListener(l2);
        if(channel != null)
            channel.setUpHandler(prot_adapter);
        start();
    }


    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, boolean deadlock_detection) {
        this.channel=channel;
        this.deadlock_detection=deadlock_detection;
        prot_adapter=new ProtocolAdapter();
        if(channel != null) {
            local_addr=channel.getLocalAddress();
            channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        }
        setMessageListener(l);
        setMembershipListener(l2);
        if(channel != null)
            channel.setUpHandler(prot_adapter);
        start();
    }

    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2,
                             boolean deadlock_detection, boolean concurrent_processing) {
        this.channel=channel;
        this.deadlock_detection=deadlock_detection;
        this.concurrent_processing=concurrent_processing;
        prot_adapter=new ProtocolAdapter();
        if(channel != null) {
            local_addr=channel.getLocalAddress();
            channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        }
        setMessageListener(l);
        setMembershipListener(l2);
        if(channel != null)
            channel.setUpHandler(prot_adapter);
        start();
    }



    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, RequestHandler req_handler) {
        this(channel, l, l2);
        setRequestHandler(req_handler);
    }



    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, RequestHandler req_handler,
                             boolean deadlock_detection) {
        this(channel, l, l2);
        this.deadlock_detection=deadlock_detection;
        setRequestHandler(req_handler);
    }

    public MessageDispatcher(Channel channel, MessageListener l, MembershipListener l2, RequestHandler req_handler,
                             boolean deadlock_detection, boolean concurrent_processing) {
        this(channel, l, l2);
        this.deadlock_detection=deadlock_detection;
        this.concurrent_processing=concurrent_processing;
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
    public MessageDispatcher(PullPushAdapter adapter, Serializable id,
                             MessageListener l, MembershipListener l2) {
        this.adapter=adapter; this.id=id;
        setMembers (((Channel) adapter.getTransport()).getView().getMembers());
        setMessageListener(l);
        setMembershipListener(l2);
        PullPushHandler handler=new PullPushHandler();
        Transport       tp;

        transport_adapter=new TransportAdapter();
        adapter.addMembershipListener(handler);
        if(id == null) // no other building block around, let's become the main consumer of this PullPushAdapter
            adapter.setListener(handler);
        else
            adapter.registerListener(id, handler);

        if((tp=adapter.getTransport()) instanceof Channel) {
            ((Channel)tp).setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
            local_addr=((Channel)tp).getLocalAddress();
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
    public MessageDispatcher(PullPushAdapter adapter, Serializable id,
                             MessageListener l, MembershipListener l2,
                             RequestHandler req_handler) {
        this.adapter=adapter; this.id=id;
        setMembers (((Channel) adapter.getTransport()).getView().getMembers());
        setRequestHandler(req_handler);
        setMessageListener(l);
        setMembershipListener(l2);
        PullPushHandler handler=new PullPushHandler();
        Transport       tp;

        transport_adapter=new TransportAdapter();
        adapter.addMembershipListener(handler);
        if(id == null) // no other building block around, let's become the main consumer of this PullPushAdapter
            adapter.setListener(handler);
        else
            adapter.registerListener(id, handler);

        if((tp=adapter.getTransport()) instanceof Channel) {
            ((Channel)tp).setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
            local_addr=((Channel)tp).getLocalAddress(); // fixed bug #800774
        }

        start();
    }


    public MessageDispatcher(PullPushAdapter adapter, Serializable id,
                             MessageListener l, MembershipListener l2,
                             RequestHandler req_handler, boolean concurrent_processing) {
        this.concurrent_processing=concurrent_processing;
        this.adapter=adapter; this.id=id;
        setMembers (((Channel) adapter.getTransport()).getView().getMembers());
        setRequestHandler(req_handler);
        setMessageListener(l);
        setMembershipListener(l2);
        PullPushHandler handler=new PullPushHandler();
        Transport       tp;

        transport_adapter=new TransportAdapter();
        adapter.addMembershipListener(handler);
        if(id == null) // no other building block around, let's become the main consumer of this PullPushAdapter
            adapter.setListener(handler);
        else
            adapter.registerListener(id, handler);

        if((tp=adapter.getTransport()) instanceof Channel) {
            ((Channel)tp).setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
            local_addr=((Channel)tp).getLocalAddress(); // fixed bug #800774
        }

        start();
    }


    /**
     * If this dispatcher is using a user-provided PullPushAdapter, then need to set the members
     * from the adapter initially since viewChange has most likely already been called in PullPushAdapter. 
     */
    private void setMembers (Vector new_mbrs)
    {
        if(new_mbrs != null) {
            members.removeAllElements();
            for(int i=0; i < new_mbrs.size(); i++)
                members.addElement(new_mbrs.elementAt(i));
        }
    }
    
    public void setDeadlockDetection(boolean flag) {
        deadlock_detection=flag;
        corr.setDeadlockDetection(flag);
    }

    public void setConcurrentProcessing(boolean flag) {
        this.concurrent_processing=flag;
    }

    public void finalize() {
        stop();
    }


    public void start() {
        if(corr == null) {
            if(transport_adapter != null)
                corr=new RequestCorrelator("MessageDispatcher", transport_adapter, 
                                           this, deadlock_detection, local_addr, concurrent_processing);
            else {
                corr=new RequestCorrelator("MessageDispatcher", prot_adapter, 
                                           this, deadlock_detection, local_addr, concurrent_processing);
            }
            corr.start();
        }
        if(channel != null) {
            Vector tmp_mbrs=channel.getView() != null? channel.getView().getMembers() : null;
            setMembers(tmp_mbrs);
        }
    }


    public void stop() {
        if(corr != null) {
            corr.stop();
            corr=null;
        }
    }


    public void setMessageListener(MessageListener l) {
        msg_listener=l;
    }


    public void setMembershipListener(MembershipListener l) {
        membership_listener=l;
    }

    public void setRequestHandler(RequestHandler rh) {
        req_handler=rh;
    }



    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException {
        if(channel != null)
            channel.send(msg);
        else if(adapter != null) {
            try {
                if(id != null)
                    adapter.send(id, msg);
                else
                    adapter.send(msg);
            }
            catch(Throwable ex) {
                Trace.error("MessageDispatcher.send()", "exception=" + Util.print(ex));
            }
        }
        else {
            Trace.error("MessageDispatcher.send()", "channel == null");
        }
    }
    




   /**
       Cast a message to all members, and wait for <code>mode</code> responses. The responses are
       returned in a response list, where each response is associated with its sender.<p>
       Uses <code>GroupRequest</code>.
       @param dests The members to which the message is to be sent. If it is null, then the message 
                    is sent to all members
       @param msg The message to be sent to n members
       @param mode Defined in <code>GroupRequest</code>. The number of responses to wait for:
                   <ol>
                   <li>GET_FIRST: return the first response received.
                   <li>GET_ALL: wait for all responses (minus the ones from suspected members)
                   <li>GET_MAJORITY: wait for a majority of all responses (relative to the grp size)
                   <li>GET_ABS_MAJORITY: wait for majority (absolute, computed once)
                   <li>GET_N: wait for n responses (may block if n > group size)
                   <li>GET_NONE: wait for no responses, return immediately (non-blocking)
                   </ol>
       @param timeout If 0: wait forever. Otherwise, wait for <code>mode</code> responses 
                      <em>or</em> timeout time.
       @return RspList A list of responses. Each response is an <code>Object</code> and associated
                       to its sender.
   */    
    public RspList castMessage(final Vector dests, Message msg, int mode, long timeout) {
        GroupRequest  _req=null;
        Vector        real_dests;
        Channel       tmp;

        // we need to clone because we don't want to modify the original
        // (we remove ourselves if LOCAL is false, see below) !
        real_dests=dests != null ? (Vector)dests.clone() : (members != null? (Vector)members.clone() : null);

        // if local delivery is off, then we should not wait for the message from the local member.
        // therefore remove it from the membership
        tmp=channel;
        if(tmp == null) {
            if(adapter != null && adapter.getTransport() instanceof Channel)
                tmp=(Channel)adapter.getTransport();
        }

        if(tmp != null && tmp.getOpt(Channel.LOCAL).equals(Boolean.FALSE)) {
            if(local_addr == null)
                local_addr=tmp.getLocalAddress();
            if(local_addr != null && real_dests != null)
                real_dests.removeElement(local_addr);
        }
        
        // don't even send the message if the destination list is empty
        if(real_dests == null || real_dests.size() == 0) {
            if(Trace.trace) {
                Trace.info("MessageDispatcher.castMessage()",
                           "destination list is empty, won't send message");
                return new RspList(); // return empty response list
            }
        }

        _req=new GroupRequest(msg, corr, real_dests, mode, timeout, 0);
        _req.execute();

        return _req.getResults();
    }


    /**
     * Multicast a message request to all members in <code>dests</code> and receive responses
     * via the RspCollector interface. When done receiving the required number of responses, the caller
     * has to call done(req_id) on the underlyinh RequestCorrelator, so that the resources allocated to that
     * request can be freed.
     * @param dests The list of members from which to receive responses. Null means all members
     * @param req_id The ID of the request. Used by the underlying RequestCorrelator to
     *               correlate responses with requests
     * @param msg The request to be sent
     * @param coll The sender needs to provide this interface to collect responses. Call will return
     *             immediately if this is null
     */
    public void castMessage(final Vector dests, long req_id, Message msg, RspCollector coll) {
        Vector        real_dests;
        Channel       tmp;

        if(msg == null) {
            Trace.error("MessageDispatcher.castMessage()", "request is null");
            return;
        }

        if(coll == null) {
            Trace.error("MessageDispatcher.castMessage()", "response collector is null (must be non-null)");
            return;
        }
            
        // we need to clone because we don't want to modify the original
        // (we remove ourselves if LOCAL is false, see below) !
        real_dests=dests != null ? (Vector)dests.clone() : (Vector)members.clone();

        // if local delivery is off, then we should not wait for the message from the local member.
        // therefore remove it from the membership
        tmp=channel;
        if(tmp == null) {
            if(adapter != null && adapter.getTransport() instanceof Channel)
                tmp=(Channel)adapter.getTransport();
        }

        if(tmp != null && tmp.getOpt(Channel.LOCAL).equals(Boolean.FALSE)) {
            if(local_addr == null)
                local_addr=tmp.getLocalAddress();
            if(local_addr != null)
                real_dests.removeElement(local_addr);
        }
        
        // don't even send the message if the destination list is empty
        if(real_dests.size() == 0) {
            if(Trace.trace) {
                Trace.info("MessageDispatcher.castMessage()",
                           "destination list is empty, won't send message");
                return;
            }
        }

        corr.sendRequest(req_id, real_dests, msg, coll);
    }

    
    public void done(long req_id) {
        corr.done(req_id);
    }



    
    /**
     * Sends a message to a single member (destination = msg.dest) and returns the response. 
     * The message's destination must be non-zero !
     */
    public Object sendMessage(Message msg, int mode, long timeout) throws TimeoutException, SuspectedException {
        Vector        mbrs=new Vector();
        RspList       rsp_list=null;
        Object        dest=msg.getDest();
        Rsp           rsp;
        GroupRequest  _req=null;

        if(dest == null) {
            Trace.error("MessageProtocol.sendMessage()", "the message's destination is null, " +
                        "cannot send message");
            return null;
        }
        
        mbrs.addElement(dest);   // dummy membership (of destination address)        

        _req=new GroupRequest(msg, corr, mbrs, mode, timeout, 0);
        _req.execute();

        if(mode == GroupRequest.GET_NONE)
            return null;


        rsp_list=_req.getResults();
        
        if(rsp_list.size() == 0) {
            Trace.warn("MessageProtocol.sendMessage()", " response list is empty");
            return null;            
        }
        if(rsp_list.size() > 1)
            Trace.warn("MessageProtocol.sendMessage()", "response list contains " +
                       "more that 1 response; returning first response !");
        rsp=(Rsp)rsp_list.elementAt(0);
        if(rsp.wasSuspected())
            throw new SuspectedException(dest);
        if(!rsp.wasReceived())
            throw new TimeoutException();
        return rsp.getValue();
    }






    
    /* ------------------------ RequestHandler Interface ---------------------- */
    public Object handle(Message msg) {
        if(req_handler != null)
            return req_handler.handle(msg);
        else
            return null;
    }
    /* -------------------- End of RequestHandler Interface ------------------- */






    class ProtocolAdapter extends Protocol implements UpHandler {

        /* ------------------------- Protocol Interface --------------------------- */

        public String getName() {return "MessageDispatcher";}

        public void startUpHandler() {
            // do nothing, DON'T REMOVE !!!!
        }

        public void startDownHandler() {
            // do nothing, DON'T REMOVE !!!!
        }


        public void stopInternal() {
            // do nothing, DON'T REMOVE !!!!
        }

        protected void receiveUpEvent(Event evt) {
        }

        protected void receiveDownEvent(Event evt) {
        }


        /**
         * Called by request correlator when message was not generated by it. We handle it and call the
         * message listener's corresponding methods
         */
        public void passUp(Event evt) {
            switch(evt.getType()) {
            case Event.MSG:
                if(msg_listener != null)
                    msg_listener.receive((Message)evt.getArg());
                break;            

            case Event.GET_APPLSTATE: // reply with GET_APPLSTATE_OK
                if(msg_listener != null)
                    channel.returnState(msg_listener.getState());
                else
                    channel.returnState(null);
                break;

            case Event.GET_STATE_OK:
                if(msg_listener != null) {
                    try {
                        msg_listener.setState((byte[])evt.getArg());
                    }
                    catch(ClassCastException cast_ex) {
                        Trace.error("MessageDispatcher.passUp()", "received SetStateEvent, but argument " + 
                                    evt.getArg() + " is not serializable. Discarding message.");
                    }
                }
                break;

            case Event.VIEW_CHANGE:
                View    v=(View)evt.getArg();
                Vector  new_mbrs=v.getMembers();

                if(new_mbrs != null) {
                    members.removeAllElements();
                    for(int i=0; i < new_mbrs.size(); i++)
                        members.addElement(new_mbrs.elementAt(i));
                }

                if(membership_listener != null)
                    membership_listener.viewAccepted(v);
                break;
            
            case Event.SUSPECT:
                if(membership_listener != null)
                    membership_listener.suspect((Address)evt.getArg());
                break;
            
            case Event.BLOCK:
                if(membership_listener != null)
                    membership_listener.block();
                break;
            }
        }


        public void passDown(Event evt) {
            down(evt);
        }


        /** Called by channel (we registered before) when event is received. This is the UpHandler interface.
         */
        public void up(Event evt) {
            if(corr != null)
                corr.receive(evt);
            else
                Trace.error("MessageDispatcher.up()", "corr == null");
        }


        public void down(Event evt) {
            if(channel != null)
                channel.down(evt);
            else
                Trace.error("MessageDispatcher.down()", "channel == null");
        }
        /* ----------------------- End of Protocol Interface ------------------------ */

    }





    class TransportAdapter implements Transport {

        public void send(Message msg) throws Exception {
            if(channel != null)
                channel.send(msg);
            else if(adapter != null) {
                try {
                    if(id != null)
                        adapter.send(id, msg);
                    else
                        adapter.send(msg);
                }
                catch(Throwable ex) {
                    Trace.error("MessageDispatcher.send()", "exception=" + Util.print(ex));
                }
            }
            else {
                Trace.error("MessageDispatcher.send()", "channel == null");
            }
        }

        public Object receive(long timeout) throws Exception {
            // @todo: implement
            return null;
        }
    }



    class PullPushHandler implements MessageListener, MembershipListener {
        

        /* ------------------------- MessageListener interface ---------------------- */
        public void receive(Message msg) {
            boolean pass_up=true;
            if(corr != null)
                pass_up=corr.receiveMessage(msg);

            if(pass_up) {   // pass on to MessageListener
                if(msg_listener != null)
                    msg_listener.receive(msg);
            }
        }

        public byte[] getState() {
            return msg_listener != null? msg_listener.getState() : null;
        }

        public void setState(byte[] state) {
            if(msg_listener != null)
                msg_listener.setState(state);
        }
        /* --------------------- End of MessageListener interface ------------------- */


        /* ------------------------ MembershipListener interface -------------------- */
        public void viewAccepted(View v) {
            if(corr != null)
                corr.receiveView(v);

            Vector  new_mbrs=v.getMembers();
            if(new_mbrs != null) {
                members.removeAllElements();
                for(int i=0; i < new_mbrs.size(); i++)
                    members.addElement(new_mbrs.elementAt(i));
            }

            if(membership_listener != null)
                membership_listener.viewAccepted(v);
        }

        public void suspect(Address suspected_mbr) {
            if(corr != null)
                corr.receiveSuspect(suspected_mbr);
            if(membership_listener != null)
                membership_listener.suspect(suspected_mbr);
        }

        public void block() {
            if(membership_listener != null)
                membership_listener.block();
        }
        /* --------------------- End of MembershipListener interface ---------------- */



        // @todo: receive SET_LOCAL_ADDR event and call corr.setLocalAddress(addr)

    }



}
