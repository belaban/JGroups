package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.auth.sasl.SaslClientCallbackHandler;
import org.jgroups.auth.sasl.SaslClientContext;
import org.jgroups.auth.sasl.SaslContext;
import org.jgroups.auth.sasl.SaslServerContext;
import org.jgroups.auth.sasl.SaslUtils;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.GMS.GmsHeader;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServerFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * The SASL protocol implements authentication and, if requested by the mech, encryption
 *
 * @author Tristan Tarrant
 */
@MBean(description = "Provides SASL authentication")
public class SASL extends Protocol {
    public static final short GMS_ID = ClassConfigurator.getProtocolId(GMS.class);
    public static final short SASL_ID = ClassConfigurator.getProtocolId(SASL.class);
    public static final String SASL_PROTOCOL_NAME = "jgroups";

    @Property(name = "login_module_name", description = "The name of the JAAS login module to use to obtain a subject for creating the SASL client and server (optional). Only required by some SASL mechs (e.g. GSSAPI)")
    protected String login_module_name;

    @Property(name = "client_name", description = "The name to use when a node is acting as a client (i.e. it is not the coordinator. Will also be used to obtain the subject if using a JAAS login module")
    protected String client_name;

    @Property(name = "client_password", description = "The password to use when a node is acting as a client (i.e. it is not the coordinator. Will also be used to obtain the subject if using a JAAS login module", exposeAsManagedAttribute = false)
    protected String client_password;

    @Property(name = "mech", description = "The name of the mech to require for authentication. Can be any mech supported by your local SASL provider. The JDK comes standard with CRAM-MD5, DIGEST-MD5, GSSAPI, NTLM")
    protected String mech;

    @Property(name = "sasl_props", description = "Properties specific to the chosen mech", converter = PropertyConverters.StringProperties.class)
    protected Map<String, String> sasl_props = new HashMap<>();

    @Property(name = "server_name", description = "The fully qualified server name")
    protected String server_name;

    @Property(name = "timeout", description = "How long to wait (in ms) for a response to a challenge")
    protected long timeout = 5000;

    @Property(name = "client_callback_handler", description = "The CallbackHandler to use when a node acts as a client (i.e. it is not the coordinator")
    protected CallbackHandler client_callback_handler;

    @Property(name = "server_callback_handler", description = "The CallbackHandler to use when a node acts as a server (i.e. it is the coordinator")
    protected CallbackHandler server_callback_handler;

    protected Subject client_subject;
    protected Subject server_subject;

    protected Address local_addr;
    protected final Map<Address, SaslContext> sasl_context = new HashMap<>();
    private SaslServerFactory saslServerFactory;
    private SaslClientFactory saslClientFactory;

    public SASL() {
    }

    @Property(name = "client_callback_handler_class")
    public void setClientCallbackHandlerClass(String handlerClass) throws Exception {
        client_callback_handler = Class.forName(handlerClass).asSubclass(CallbackHandler.class).newInstance();
    }

    public String getClientCallbackHandlerClass() {
        return client_callback_handler != null ? client_callback_handler.getClass().getName() : null;
    }

    public CallbackHandler getClientCallbackHandler() {
        return client_callback_handler;
    }

    public void setClientCallbackHandler(CallbackHandler client_callback_handler) {
        this.client_callback_handler = client_callback_handler;
    }

    @Property(name = "server_callback_handler_class")
    public void setServerCallbackHandlerClass(String handlerClass) throws Exception {
        server_callback_handler = Class.forName(handlerClass).asSubclass(CallbackHandler.class).newInstance();
    }

    public String getServerCallbackHandlerClass() {
        return server_callback_handler != null ? server_callback_handler.getClass().getName() : null;
    }

    public CallbackHandler getServerCallbackHandler() {
        return server_callback_handler;
    }

    public void setServerCallbackHandler(CallbackHandler server_callback_handler) {
        this.server_callback_handler = server_callback_handler;
    }

    public void setLoginModuleName(String login_module_name) {
        this.login_module_name = login_module_name;
    }

    public String getLoginModulename() {
        return login_module_name;
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

    public void setClientSubject(Subject client_subject) {
        this.client_subject = client_subject;
    }

    public Subject getClientSubject() {
        return client_subject;
    }

    public void setServerSubject(Subject server_subject) {
        this.server_subject = server_subject;
    }

    public Subject getServerSubject() {
        return server_subject;
    }

    public void setServerName(String server_name) {
        this.server_name = server_name;
    }

    public String getServerName(String server_name) {
        return this.server_name;
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
        saslServerFactory = SaslUtils.getSaslServerFactory(mech, sasl_props);
        saslClientFactory = SaslUtils.getSaslClientFactory(mech, sasl_props);
        char[] client_password_chars = client_password == null ? new char[]{} : client_password.toCharArray();
        if (client_callback_handler == null) {
            client_callback_handler = client_password == null ? Void -> {} : new SaslClientCallbackHandler(client_name, client_password_chars);
        }

        if (server_callback_handler == null) {
            server_callback_handler = Void -> {};
        }

        if (server_subject == null && login_module_name != null) {
            LoginContext lc = new LoginContext(login_module_name);
            lc.login();
            server_subject = lc.getSubject();
        }
        if (client_subject == null && login_module_name != null) {
            LoginContext lc = new LoginContext(login_module_name, new SaslClientCallbackHandler(client_name, client_password_chars));
            lc.login();
            client_subject = lc.getSubject();
        }
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
        sasl_context.values().forEach(SaslContext::dispose);
        sasl_context.clear();
    }

    @Override
    public Object up(Message msg) {
        SaslHeader saslHeader =msg.getHeader(SASL_ID);
        GmsHeader gmsHeader =msg.getHeader(GMS_ID);
        Address remoteAddress = msg.getSrc();
        if (needsAuthentication(gmsHeader, remoteAddress)) {
            if (saslHeader == null)
                throw new IllegalStateException("Found GMS join or merge request but no SASL header");
            if (!serverChallenge(gmsHeader, saslHeader, msg))
                return null; // failed auth, don't pass up
        } else if (saslHeader != null) {
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
                        // the response computed can be null if the challenge-response cycle has ended
                        Message response = saslContext.nextMessage(remoteAddress, saslHeader);
                        if (response != null) {
                            if (log.isTraceEnabled())
                                log.trace("%s: sending RESPONSE to %s", getAddress(), remoteAddress);
                            down_prot.down(response);
                        } else {
                            if (!saslContext.isSuccessful()) {
                                throw new SaslException("computed response is null but challenge-response cycle not complete!");
                            }
                            if (log.isTraceEnabled())
                                log.trace("%s: authentication complete from %s", getAddress(), remoteAddress);
                        }
                    } catch (SaslException e) {
                        disposeContext(remoteAddress);
                        if (log.isWarnEnabled()) {
                            log.warn(getAddress() + ": failed to validate CHALLENGE from " + remoteAddress + ", token", e);
                        }
                    }
                    break;
                case RESPONSE:
                    try {
                        if (log.isTraceEnabled())
                            log.trace("%s: received RESPONSE from %s", getAddress(), remoteAddress);
                        Message challenge = saslContext.nextMessage(remoteAddress, saslHeader);
                        // the challenge computed can be null if the challenge-response cycle has ended
                        if (challenge != null) {
                            if (log.isTraceEnabled())
                                log.trace("%s: sending CHALLENGE to %s", getAddress(), remoteAddress);

                            down_prot.down(challenge);
                        } else {
                            if (!saslContext.isSuccessful()) {
                                throw new SaslException("computed challenge is null but challenge-response cycle not complete!");
                            }
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
        return up_prot.up(msg);
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
            GmsHeader gmsHeader =msg.getHeader(GMS_ID);
            Address remoteAddress = msg.getSrc();
            if (needsAuthentication(gmsHeader, remoteAddress)) {
                SaslHeader saslHeader =msg.getHeader(id);
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
            local_addr = evt.getArg();
            break;
        }
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        GmsHeader hdr =msg.getHeader(GMS_ID);
        Address remoteAddress = msg.getDest();
        if (needsAuthentication(hdr, remoteAddress)) {
            // We are a client who needs to authenticate
            SaslClientContext ctx = null;

            try {
                ctx = new SaslClientContext(saslClientFactory, mech, server_name != null ? server_name : remoteAddress.toString(), client_callback_handler, sasl_props, client_subject);
                sasl_context.put(remoteAddress, ctx);
                ctx.addHeader(msg, null);
            } catch (Exception e) {
                if (ctx != null) {
                    disposeContext(remoteAddress);
                }
                throw new SecurityException(e);
            }
        }
        return down_prot.down(msg);
    }

    private boolean isSelf(Address remoteAddress) {
        return remoteAddress.equals(local_addr);
    }

    private boolean needsAuthentication(GmsHeader hdr, Address remoteAddress) {
        if (hdr != null) {
            switch (hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                return true;
            case GMS.GmsHeader.MERGE_REQ:
                return !isSelf(remoteAddress);
            case GMS.GmsHeader.JOIN_RSP:
            case GMS.GmsHeader.MERGE_RSP:
                return false;
            default:
                return false;
            }
        } else {
            return false;
        }
    }

    protected boolean serverChallenge(GmsHeader gmsHeader, SaslHeader saslHeader, Message msg) {
        switch (gmsHeader.getType()) {
        case GmsHeader.JOIN_REQ:
        case GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
        case GmsHeader.MERGE_REQ:
            Address remoteAddress = msg.getSrc();
            SaslServerContext ctx = null;
            try {
                ctx = new SaslServerContext(saslServerFactory, mech, server_name != null ? server_name : local_addr.toString(), server_callback_handler, sasl_props, server_subject);
                sasl_context.put(remoteAddress, ctx);
                this.getDownProtocol().down(ctx.nextMessage(remoteAddress, saslHeader));
                ctx.awaitCompletion(timeout);
                if (ctx.isSuccessful()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Authentication successful for %s", ctx.getAuthorizationID());
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
        down_prot.down(msg);
    }

    protected void sendMergeRejectionMessage(Address dest) {
        Message msg = new Message(dest).setFlag(Message.Flag.OOB);
        GmsHeader hdr = new GmsHeader(GmsHeader.MERGE_RSP);
        hdr.setMergeRejected(true);
        msg.putHeader(GMS_ID, hdr);
        if (log.isDebugEnabled())
            log.debug("merge response=" + hdr);
        down_prot.down(msg);
    }
}
