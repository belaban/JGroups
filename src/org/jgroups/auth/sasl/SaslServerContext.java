package org.jgroups.auth.sasl;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.protocols.SASL;
import org.jgroups.protocols.SaslHeader;
import org.jgroups.protocols.SaslHeader.Type;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SaslServerContext implements SaslContext {
    SaslServer server;
    CountDownLatch latch = new CountDownLatch(1);
    Subject subject;

    public SaslServerContext(final SaslServerFactory saslServerFactory, final String mech, final String serverName, final CallbackHandler callback_handler, final Map<String, String> props, final Subject subject) throws SaslException {
        this.subject = subject;
        if (this.subject != null) {
            try {
                server = Subject.doAs(this.subject, (PrivilegedExceptionAction<SaslServer>)()
                  -> saslServerFactory.createSaslServer(mech, SASL.SASL_PROTOCOL_NAME, serverName, props, callback_handler));
            } catch (PrivilegedActionException e) {
                throw (SaslException)e.getCause(); // The createSaslServer will only throw this type of exception
            }
        } else {
            server = saslServerFactory.createSaslServer(mech, SASL.SASL_PROTOCOL_NAME, serverName, props, callback_handler);
        }
    }

    @Override
    public boolean isSuccessful() {
        return server.isComplete();
    }

    @Override
    public boolean needsWrapping() {
        if (server.isComplete()) {
            String qop = (String) server.getNegotiatedProperty(Sasl.QOP);
            return (qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf")));
        } else {
            return false;
        }
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        return server.wrap(outgoing, offset, len);
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        return server.unwrap(incoming, offset, len);
    }

    @Override
    public Message nextMessage(Address address, SaslHeader header) throws SaslException {
        Message message = new EmptyMessage(address).setFlag(Message.Flag.OOB);
        byte[] challenge = server.evaluateResponse(header.getPayload());
        if (server.isComplete()) {
            latch.countDown();
        }
        if (challenge != null) {
            return message.putHeader(SASL.SASL_ID, new SaslHeader(Type.CHALLENGE, challenge));
        } else {
            return null;
        }
    }

    public void awaitCompletion(long timeout) throws InterruptedException {
        latch.await(timeout, TimeUnit.MILLISECONDS);
    }

    public String getAuthorizationID() {
        return server.getAuthorizationID();
    }

    @Override
    public void dispose() {
        try {
            server.dispose();
        } catch (SaslException e) {
        }
    }

}
