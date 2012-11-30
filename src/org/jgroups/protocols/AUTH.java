package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.auth.AuthToken;
import org.jgroups.auth.X509Token;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * The AUTH protocol adds a layer of authentication to JGroups
 * @author Chris Mills
 * @autho Bela Ban
 */
@MBean(description="Provides authentication of joiners, to prevent un-authorized joining of a cluster")
public class AUTH extends Protocol {

    /** Interface to provide callbacks for handling up events */
    public interface UpHandler {
        /**
         * Called when an up event has been received
         * @param evt the event
         * @return true if the event should be pass up, else false
         */
        boolean handleUpEvent(Event evt);
    }


    /** Used on the coordinator to authentication joining member requests against */
    protected AuthToken             auth_token=null;

    protected static final short    gms_id=ClassConfigurator.getProtocolId(GMS.class);

    /** List of UpHandler which are called when an up event has been received. Usually used by AuthToken impls */
    protected final List<UpHandler> up_handlers=new ArrayList<UpHandler>();

    protected Address               local_addr;


    public AUTH() {name="AUTH";}

   

    @Property(name="auth_class")
    public void setAuthClass(String class_name) throws Exception {
        Object obj=Class.forName(class_name).newInstance();
        auth_token=(AuthToken)obj;
        auth_token.setAuth(this);
    }

    public String    getAuthClass()                {return auth_token != null? auth_token.getClass().getName() : null;}
    public AuthToken getAuthToken()                {return auth_token;}
    public void      setAuthToken(AuthToken token) {this.auth_token=token;}
    public void      register(UpHandler handler)   {up_handlers.add(handler);}
    public void      unregister(UpHandler handler) {up_handlers.remove(handler);}
    public Address   getAddress()                  {return local_addr;}


    protected List<Object> getConfigurableObjects() {
        List<Object> retval=new LinkedList<Object>();
        if(auth_token != null)
            retval.add(auth_token);
        return retval;
    }

    public void init() throws Exception {
        super.init();
        if(auth_token instanceof X509Token) {
            X509Token tmp=(X509Token)auth_token;
            tmp.setCertificate();
        }
        auth_token.init();
    }



    /**
     * An event was received from the layer below. Usually the current layer will want to examine
     * the event type and - depending on its type - perform some computation
     * (e.g. removing headers from a MSG event type, or updating the internal membership list
     * when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down
     * the stack using <code>down_prot.down()</code> or c) the event (or another event) is sent up
     * the stack using <code>up_prot.up()</code>.
     */
    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                AuthHeader auth_hdr=(AuthHeader)msg.getHeader(id);
                if(auth_hdr == null)
                    break;

                GMS.GmsHeader gms_hdr=getGMSHeader(evt);
                if(gms_hdr == null)
                    throw new IllegalStateException("found AuthHeader but no GmsHeader");
                if(handleAuthHeader(gms_hdr, auth_hdr, msg) == false) // authentication failed
                    return null;    // don't pass up
                break;
        }
        if(callUpHandlers(evt) == false)
            return null;

        return up_prot.up(evt);
    }


    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using <code>down_prot.down()</code>. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using <code>up_prot.up()</code>.
     */
    public Object down(Event evt) {
        GMS.GmsHeader hdr = getGMSHeader(evt);
        if((hdr != null) && (hdr.getType() == GMS.GmsHeader.JOIN_REQ
          || hdr.getType() == GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER
          || hdr.getType() == GMS.GmsHeader.MERGE_REQ)) {
            //we found a join request message - now add an AUTH Header
            Message msg = (Message)evt.getArg();
            AuthHeader authHeader = new AuthHeader(this.auth_token);
            msg.putHeader(this.id, authHeader);
        }

        if(evt.getType() == Event.SET_LOCAL_ADDRESS)
            local_addr=(Address)evt.getArg();

        return down_prot.down(evt);
    }


    /**
     * Handles a GMS header
     * @param gms_hdr
     * @param msg
     * @return true if the message should be passed up, or else false
     */
    protected boolean handleAuthHeader(GMS.GmsHeader gms_hdr, AuthHeader auth_hdr, Message msg) {
        switch(gms_hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
            case GMS.GmsHeader.MERGE_REQ:
                if(this.auth_token.authenticate(auth_hdr.getToken(), msg))
                    return true; //  authentication passed, send message up the stack
                if(log.isWarnEnabled())
                    log.warn("failed to validate AuthHeader token from " + msg.getSrc() + ", token: " + auth_token);
                sendRejectionMessage(gms_hdr.getType(), msg.getSrc(), "Authentication failed");
                return false;
            default:
                return true; // pass up
        }
    }


    protected void sendRejectionMessage(byte type, Address dest, String error_msg) {
        switch(type) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                sendJoinRejectionMessage(dest, error_msg);
                break;
            case GMS.GmsHeader.MERGE_REQ:
                sendMergeRejectionMessage(dest);
                break;
            default:
                log.error("type " + type + " unknown");
                break;
        }
    }

    protected void sendJoinRejectionMessage(Address dest, String error_msg) {
        if(dest == null)
            return;

        Message msg = new Message(dest, null, null);
        JoinRsp joinRes=new JoinRsp(error_msg); // specify the error message on the JoinRsp

        GMS.GmsHeader gmsHeader=new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP, joinRes);
        msg.putHeader(gms_id, gmsHeader);
        down_prot.down(new Event(Event.MSG, msg));
    }

    protected void sendMergeRejectionMessage(Address dest) {
        Message msg=new Message(dest, null, null);
        msg.setFlag(Message.OOB);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.setMergeRejected(true);
        msg.putHeader(gms_id, hdr);
        if(log.isDebugEnabled()) log.debug("merge response=" + hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }

    protected boolean callUpHandlers(Event evt) {
        boolean pass_up=true;
        for(UpHandler handler: up_handlers) {
            if(handler.handleUpEvent(evt) == false)
                pass_up=false;
        }
        return pass_up;
    }

    /**
     * Get the header from a GMS message
     * @param evt The event object passed in to AUTH
     * @return A GmsHeader object or null if the event contains a message of a different type
     */
    protected static GMS.GmsHeader getGMSHeader(Event evt){
        switch(evt.getType()){
          case Event.MSG:
                Message msg = (Message)evt.getArg();
                Header hdr = msg.getHeader(gms_id);
                if(hdr instanceof GMS.GmsHeader)
                    return (GMS.GmsHeader)hdr;
        }
        return null;
    }
}
