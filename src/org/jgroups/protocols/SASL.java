package org.jgroups.protocols;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.auth.sasl.SaslClientContext;
import org.jgroups.auth.sasl.SaslContext;
import org.jgroups.auth.sasl.SaslServerContext;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.GMS.GmsHeader;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/**
 * The SASL protocol implements authentication and, if requested by the mech, encryption
 *
 * @author Tristan Tarrant
 */
@MBean(description = "Provides SASL authentication")
public class SASL extends Protocol {
    public static final short GMS_ID = ClassConfigurator.getProtocolId(GMS.class);
    public static final short SASL_ID = ClassConfigurator.getProtocolId(SASL.class);

    @Property(name = "mech", description = "The name of the mech to require for authentication. Can be any mech supported by your local SASL provider. The JDK comes standard with CRAM-MD5, DIGEST-MD5, GSSAPI, NTLM")
    protected String mech;

    @Property(name = "sasl_props", description = "Properties specific to the chosen mech", converter = PropertyConverters.StringProperties.class)
    protected Map<String, String> sasl_props = new HashMap<String, String>();

    @Property(name = "timeout", description = "How long to wait (in ms) for a response to a challenge")
    protected long timeout = 5000;

    protected CallbackHandler callback_handler;

    protected Address local_addr;
    protected final Map<Address, SaslContext> sasl_context = new HashMap<Address, SaslContext>();



    public SASL() {
        name = this.getClass().getSimpleName();
    }

    @Property(name = "callback_handler_class")
    public void setCallbackHandlerClass(String handlerClass) throws Exception {
        callback_handler = Class.forName(handlerClass).asSubclass(CallbackHandler.class).newInstance();
    }

    public String getCallbackHandlerClass() {
        return callback_handler != null ? callback_handler.getClass().getName() : null;
    }

    public CallbackHandler getCallbackHandler() {
        return callback_handler;
    }

    public void setCallbackHandler(CallbackHandler callback_handler) {
        this.callback_handler = callback_handler;
    }

    public void setMech(String mech) {
        this.mech = mech;
    }

    public String getMech() {
        return mech;
    }

    public void setSaslProps(Map<String, String> sasl_props) {
        this.sasl_props = sasl_props;
    }

    public Map<String, String> getSaslProps() {
        return sasl_props;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getTimeout() {
        return timeout;
    }

    public Address getAddress() {
        return local_addr;
    }

    @Override
    public void init() throws Exception {
        super.init();
    }

    @Override
    public void stop() {
        super.stop();
        cleanup();
    }

    @Override
    public void destroy() {
        super.destroy();
        cleanup();
    }

    private void cleanup() {
        for(SaslContext context : sasl_context.values()) {
            context.dispose();
        }
        sasl_context.clear();
    }

    @Override
    public Object up(Event evt) {
        if (evt.getType() == Event.MSG) {
            Message msg = (Message) evt.getArg();
            SaslHeader saslHeader = (SaslHeader) msg.getHeader(SASL_ID);
            GmsHeader gmsHeader = (GmsHeader) msg.getHeader(GMS_ID);
            if (needsAuthentication(gmsHeader)) {
                if (saslHeader == null)
                    throw new IllegalStateException("Found GMS join or merge request but no SASL header");
                if (!serverChallenge(gmsHeader, saslHeader, msg))
                    return null; // failed auth, don't pass up
            } else if (saslHeader != null) {
                Address remoteAddress = msg.getSrc();
                SaslContext saslContext = sasl_context.get(remoteAddress);
                if (saslContext == null) {
                    throw new IllegalStateException(String.format(
                            "Cannot find server context to challenge SASL request from %s", remoteAddress.toString()));
                }
                switch (saslHeader.getType()) {
                case CHALLENGE:
                    try {
                        if (log.isTraceEnabled())
                            log.trace("%s: received CHALLENGE from %s", getAddress(), remoteAddress);
                        Message response = saslContext.nextMessage(remoteAddress, saslHeader);
                        if (log.isTraceEnabled())
                            log.trace("%s: sending RESPONSE to %s", getAddress(), remoteAddress);
                        down_prot.down(new Event(Event.MSG, response));
                    } catch (SaslException e) {
                        disposeContext(remoteAddress);
                        if (log.isWarnEnabled()) {
                            log.warn("failed to validate CHALLENGE from " + remoteAddress + ", token", e);
                        }
                        sendRejectionMessage(gmsHeader.getType(), remoteAddress, "authentication failed");
                    }
                    break;
                case RESPONSE:
                    try {
                        if (log.isTraceEnabled())
                            log.trace("%s: received RESPONSE from %s", getAddress(), remoteAddress);
                        Message challenge = saslContext.nextMessage(remoteAddress, saslHeader);
                        if (challenge != null) {
                            if (log.isTraceEnabled())
                                log.trace("%s: sending CHALLENGE to %s", getAddress(), remoteAddress);

                            down_prot.down(new Event(Event.MSG, challenge));
                        } else {
                            if (log.isTraceEnabled())
                                log.trace("%s: authentication complete from %s", getAddress(), remoteAddress);
                        }
                    } catch (SaslException e) {
                        disposeContext(remoteAddress);
                        if (log.isWarnEnabled()) {
                            log.warn("failed to validate RESPONSE from " + remoteAddress + ", token", e);
                        }
                    }
                    break;
                }
                return null;
            }
        }

        return up_prot.up(evt);
    }

    private void disposeContext(Address address) {
        SaslContext context = sasl_context.remove(address);
        if (context != null) {
            context.dispose();
        }
    }

    @Override
    public void up(MessageBatch batch) {
        for (Message msg : batch) {
            // If we have a join or merge request --> authenticate, else pass up
            GmsHeader gmsHeader = (GmsHeader) msg.getHeader(GMS_ID);
            if (needsAuthentication(gmsHeader)) {
                SaslHeader saslHeader = (SaslHeader) msg.getHeader(id);
                if (saslHeader == null) {
                    log.warn("Found GMS join or merge request but no SASL header");
                    sendRejectionMessage(gmsHeader.getType(), batch.sender(), "join or merge without an SASL header");
                    batch.remove(msg);
                } else if (!serverChallenge(gmsHeader, saslHeader, msg)) // authentication failed
                    batch.remove(msg); // don't pass up
            }
        }

        if (!batch.isEmpty())
            up_prot.up(batch);
    }

    @Override
    public Object down(Event evt) {
        switch (evt.getType()) {
        case Event.SET_LOCAL_ADDRESS:
            local_addr = (Address) evt.getArg();
            break;
        case Event.MSG:
            Message msg = (Message) evt.getArg();
            GmsHeader hdr = (GmsHeader) msg.getHeader(GMS_ID);
            if (needsAuthentication(hdr)) {
                // We are a client who needs to authenticate
                SaslClientContext ctx = null;
                Address remoteAddress = msg.getDest();
                try {
                    ctx = new SaslClientContext(mech, remoteAddress, callback_handler, sasl_props);
                    sasl_context.put(remoteAddress, ctx);
                    ctx.addHeader(msg, null);
                } catch (SaslException e) {
                    if (ctx != null) {
                        disposeContext(remoteAddress);
                    }
                    throw new SecurityException(e);
                }
            }
            break;
        }

        return down_prot.down(evt);
    }

    protected static boolean needsAuthentication(GmsHeader hdr) {
        return (hdr != null)
                && (hdr.getType() == GmsHeader.JOIN_REQ || hdr.getType() == GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER || hdr
                        .getType() == GmsHeader.MERGE_REQ);
    }

    protected boolean serverChallenge(GmsHeader gmsHeader, SaslHeader saslHeader, Message msg) {
        switch (gmsHeader.getType()) {
        case GmsHeader.JOIN_REQ:
        case GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
        case GmsHeader.MERGE_REQ:
            Address remoteAddress = msg.getSrc();
            SaslServerContext ctx = null;
            try {
                ctx = new SaslServerContext(mech, local_addr, callback_handler, sasl_props);
                sasl_context.put(remoteAddress, ctx);
                this.getDownProtocol().down(new Event(Event.MSG, ctx.nextMessage(remoteAddress, saslHeader)));
                ctx.awaitCompletion(timeout);
                if (ctx.isSuccessful()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Authorization successful for %s", ctx.getAuthorizationID());
                    }
                    return true;
                } else {
                    log.warn("failed to validate SaslHeader from %s, header: %s", msg.getSrc(), saslHeader);
                    sendRejectionMessage(gmsHeader.getType(), msg.getSrc(), "authentication failed");
                    return false;
                }
            } catch (SaslException e) {
                log.warn("failed to validate SaslHeader from %s, header: %s", msg.getSrc(), saslHeader);
                sendRejectionMessage(gmsHeader.getType(), msg.getSrc(), "authentication failed");
            } catch (InterruptedException e) {
                return false;
            } finally {
                if (ctx != null && !ctx.needsWrapping()) {
                    disposeContext(remoteAddress);
                }
            }
        default:
            return true; // pass up
        }
    }

    protected void sendRejectionMessage(byte type, Address dest, String error_msg) {
        switch (type) {
        case GmsHeader.JOIN_REQ:
        case GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
            sendJoinRejectionMessage(dest, error_msg);
            break;
        case GmsHeader.MERGE_REQ:
            sendMergeRejectionMessage(dest);
            break;
        default:
            log.error("type " + type + " unknown");
            break;
        }
    }

    protected void sendJoinRejectionMessage(Address dest, String error_msg) {
        if (dest == null)
            return;

        JoinRsp joinRes = new JoinRsp(error_msg); // specify the error message on the JoinRsp
        Message msg = new Message(dest).putHeader(GMS_ID, new GmsHeader(GmsHeader.JOIN_RSP)).setBuffer(
                GMS.marshal(joinRes));
        down_prot.down(new Event(Event.MSG, msg));
    }

    protected void sendMergeRejectionMessage(Address dest) {
        Message msg = new Message(dest).setFlag(Message.Flag.OOB);
        GmsHeader hdr = new GmsHeader(GmsHeader.MERGE_RSP);
        hdr.setMergeRejected(true);
        msg.putHeader(GMS_ID, hdr);
        if (log.isDebugEnabled())
            log.debug("merge response=" + hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }
}
