package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.Component;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.XmlAttribute;
import org.jgroups.auth.AuthToken;
import org.jgroups.auth.X509Token;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.Iterator;


/**
 * The AUTH protocol adds a layer of authentication to JGroups. It intercepts join and merge requests and rejects them
 * if the joiner or merger is not permitted to join a or merge into a cluster. AUTH should be placed right below
 * {@link GMS} in the configuration.<p>
 * Note that some of the AuthTokens (such as MD5Token, SimpleToken etc) cannot prevent rogue members from joining a
 * cluster, and are thus deprecated. Read the manual for a detailed description of why.
 * @author Chris Mills
 * @author Bela Ban
 */
@XmlAttribute(attrs={
  "auth_value",                                                         // SimpleToken, MD5Token, X509Token
  "fixed_members_value", "fixed_members_seperator",                     // FixedMembershipToken
  "client_principal_name", "client_password", "service_principal_name", // Krb5Token
  "match_string", "match_ip_address", "match_logical_name",             // RegexMembership
  "keystore_type", "cert_alias", "keystore_path", "cipher_type",
  "cert_password", "keystore_password"                                  // X509Token
})
@MBean(description="Provides authentication of joiners, to prevent un-authorized joining of a cluster")
public class AUTH extends Protocol {
    protected static final short    GMS_ID=ClassConfigurator.getProtocolId(GMS.class);

    /** Used on the coordinator to authentication joining member requests against */
    @Component
    protected AuthToken             auth_token;
    protected volatile boolean      authenticate_coord=true;

    public AUTH() {}

    
    @Property(description="Do join or merge responses from the coordinator also need to be authenticated")
    public AUTH setAuthCoord( boolean authenticateCoord) {
        this.authenticate_coord= authenticateCoord; return this;
    }

    @Property(name="auth_class",description="The fully qualified name of the class implementing the AuthToken interface")
    public void setAuthClass(String class_name) throws Exception {
        Object obj=Class.forName(class_name).getDeclaredConstructor().newInstance();
        auth_token=(AuthToken)obj;
        auth_token.setAuth(this);
    }

    public String          getAuthClass()                {return auth_token != null? auth_token.getClass().getName() : null;}
    public AuthToken       getAuthToken()                {return auth_token;}
    public AUTH            setAuthToken(AuthToken token) {this.auth_token=token; return this;}
    public PhysicalAddress getPhysicalAddress()          {return getTransport().getPhysicalAddress();}


    public void init() throws Exception {
        super.init();
        if(auth_token == null)
            throw new IllegalStateException("no authentication mechanism configured");

        auth_token.init();
    }

    public void start() throws Exception {
        if(auth_token instanceof X509Token) {
            log.debug("X509Token detected. Initializing certificates");
            X509Token tmp=(X509Token)auth_token;
            tmp.setCertificate();
        }

        super.start();
        if(auth_token != null)
            auth_token.start();
    }

    public void stop() {
        if(auth_token != null)
            auth_token.stop();
        super.stop();
    }

    public void destroy() {
        if(auth_token != null)
            auth_token.destroy();
        super.destroy();
    }

    /**
     * An event was received from the layer below. Usually the current layer will want to examine the event type and
     * - depending on its type - perform some computation (e.g. removing headers from a MSG event type, or updating
     * the internal membership list when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down the stack using {@code down_prot.down()}
     * or c) the event (or another event) is sent up the stack using {@code up_prot.up()}.
     */
    public Object up(Message msg) {
        // If we have a join or merge request --> authenticate, else pass up
        GMS.GmsHeader gms_hdr=getGMSHeader(msg);
        if(gms_hdr != null && needsAuthentication(msg, gms_hdr)) {
            AuthHeader auth_hdr=msg.getHeader(id);
            if(auth_hdr == null) {
                sendRejectionMessage(gms_hdr.getType(), msg.getSrc(), "no AUTH header found in message");
                throw new IllegalStateException(String.format("found %s from %s but no AUTH header", gms_hdr, msg.getSrc()));
            }
            if(!handleAuthHeader(gms_hdr, auth_hdr, msg)) // authentication failed
                return null;    // don't pass up
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        Iterator<Message> it=batch.iterator();
        while(it.hasNext()) {
            Message msg=it.next();
            // If we have a join or merge request --> authenticate, else pass up
            GMS.GmsHeader gms_hdr=getGMSHeader(msg);
            if(gms_hdr != null && needsAuthentication(msg, gms_hdr)) {
                AuthHeader auth_hdr=msg.getHeader(id);
                if(auth_hdr == null) {
                    log.warn("%s: found GMS join or merge request from %s but no AUTH header", local_addr, batch.sender());
                    sendRejectionMessage(gms_hdr.getType(), batch.sender(), "join or merge without an AUTH header");
                    it.remove();
                }
                else if(!handleAuthHeader(gms_hdr, auth_hdr, msg)) // authentication failed
                    it.remove();    // don't pass up
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    public Object down(Message msg) {
        GMS.GmsHeader hdr=getGMSHeader(msg);
        if(hdr != null && needsAuthentication(msg, hdr))
            msg.putHeader(this.id, new AuthHeader(this.auth_token));
        return down_prot.down(msg);
    }

    protected boolean needsAuthentication(Message msg, GMS.GmsHeader hdr) {
        switch(hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
            case GMS.GmsHeader.MERGE_REQ:
                return true;
            case GMS.GmsHeader.JOIN_RSP:
                JoinRsp jr=getJoinResponse(msg);
                if(jr.getFailReason() != null)
                    return false;
            case GMS.GmsHeader.MERGE_RSP:
            case GMS.GmsHeader.INSTALL_MERGE_VIEW:
                return this.authenticate_coord;
            default:
                return false;
        }
    }


    /**
     * Handles a GMS header
     * @return true if the message should be processed (= passed up), or else false
     */
    protected boolean handleAuthHeader(GMS.GmsHeader gms_hdr, AuthHeader auth_hdr, Message msg) {
        if(needsAuthentication(msg, gms_hdr)) {
            if(this.auth_token.authenticate(auth_hdr.getToken(), msg))
                return true; //  authentication passed, send message up the stack
            log.warn("%s: failed to validate AuthHeader (token: %s) from %s; dropping message and sending " +
                       "rejection message",
                     local_addr, auth_token.getClass().getSimpleName(), msg.src());
            sendRejectionMessage(gms_hdr.getType(), msg.getSrc(), "authentication failed");
            return false;
        }
        return true;
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
        }
    }

    protected void sendJoinRejectionMessage(Address dest, String error_msg) {
        if(dest == null)
            return;

        JoinRsp joinRes=new JoinRsp(error_msg); // specify the error message on the JoinRsp
        Message msg=new BytesMessage(dest).putHeader(GMS_ID, new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP))
          .setArray(GMS.marshal(joinRes));
        down_prot.down(msg);
    }

    protected void sendMergeRejectionMessage(Address dest) {
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP).setMergeRejected(true);
        Message msg=new EmptyMessage(dest).setFlag(Message.Flag.OOB).putHeader(GMS_ID, hdr);
        if(this.authenticate_coord)
            msg.putHeader(this.id, new AuthHeader(this.auth_token));
        log.debug("merge response=%s", hdr);
        down_prot.down(msg);
    }


    protected static GMS.GmsHeader getGMSHeader(Message msg) {
        Header hdr = msg.getHeader(GMS_ID);
        if(hdr instanceof GMS.GmsHeader)
            return (GMS.GmsHeader)hdr;
        return null;
    }

    protected static JoinRsp getJoinResponse(Message msg) {
        byte[] buf=msg.getArray();
        try {
            return buf != null? Util.streamableFromBuffer(JoinRsp::new, buf, msg.getOffset(), msg.getLength()) : null;
        }
        catch(Exception e) {
            return null;
        }
    }
}
