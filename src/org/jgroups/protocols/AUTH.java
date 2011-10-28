package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.annotations.Property;
import org.jgroups.auth.AuthToken;
import org.jgroups.auth.X509Token;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;

import java.util.LinkedList;
import java.util.List;


/**
 * The AUTH protocol adds a layer of authentication to JGroups
 * @author Chris Mills
 * @autho Bela Ban
 */
public class AUTH extends Protocol {


    /**
     * used on the coordinator to authentication joining member requests against
     */
    protected AuthToken auth_token=null;

    protected static final short gms_id=ClassConfigurator.getProtocolId(GMS.class);

    
    public AUTH() {
        name="AUTH";
    }

   

    @Property(name="auth_class")
    public void setAuthClass(String class_name) throws Exception {
        Object obj=Class.forName(class_name).newInstance();
        auth_token=(AuthToken)obj;
        auth_token.setAuth(this);
    }

    public String getAuthClass() {return auth_token != null? auth_token.getClass().getName() : null;}

    public AuthToken getAuthToken() {return auth_token;}
    public void setAuthToken(AuthToken token) {this.auth_token=token;}


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
        GMS.GmsHeader hdr=getGMSHeader(evt);
        if(hdr == null)
            return up_prot.up(evt);

        if(hdr.getType() == GMS.GmsHeader.JOIN_REQ || hdr.getType() == GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER
          || hdr.getType() == GMS.GmsHeader.MERGE_REQ) { // we found a join or merge message - now try and get the AUTH Header
            Message msg=(Message)evt.getArg();
            if((msg.getHeader(this.id) != null) && (msg.getHeader(this.id) instanceof AuthHeader)) {
                AuthHeader authHeader=(AuthHeader)msg.getHeader(this.id);

                if(authHeader != null) {
                    //Now we have the AUTH Header we need to validate it
                    if(this.auth_token.authenticate(authHeader.getToken(), msg)) {
                        return up_prot.up(evt);
                    }
                    else {
                        if(log.isWarnEnabled())
                            log.warn("failed to validate AuthHeader token");
                        sendRejectionMessage(hdr.getType(), msg.getSrc(), "Authentication failed");
                        return null;
                    }
                }
                else {
                    //Invalid AUTH Header - need to send failure message
                    if(log.isWarnEnabled())
                        log.warn("AUTH failed to get valid AuthHeader from Message");
                    sendRejectionMessage(hdr.getType(), msg.getSrc(), "Failed to find valid AuthHeader in Message");
                    return null;
                }
            }
            else {
                sendRejectionMessage(hdr.getType(), msg.getSrc(), "Failed to find an AuthHeader in Message");
                return null;
            }
        }
        return up_prot.up(evt);
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
            AuthHeader authHeader = new AuthHeader();
            authHeader.setToken(this.auth_token);
            msg.putHeader(this.id, authHeader);
        }
        return down_prot.down(evt);
    }

    /**
     * Get the header from a GMS message
     * @param evt The event object passed in to AUTH
     * @return A GmsHeader object or null if the event contains a message of a different type
     */
    protected static GMS.GmsHeader getGMSHeader(Event evt){
        Message msg;
        switch(evt.getType()){
          case Event.MSG:
                msg = (Message)evt.getArg();
                Object obj = msg.getHeader(gms_id);
                if(obj == null || !(obj instanceof GMS.GmsHeader)){
                    return null;
                }
                return (GMS.GmsHeader)obj;
        }
        return null;
    }
}
