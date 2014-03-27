package org.jgroups.auth.sasl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.protocols.SASL;
import org.jgroups.protocols.SaslHeader;
import org.jgroups.protocols.SaslHeader.Type;

public class SaslServerContext implements SaslContext {
    SaslServer server;
    CountDownLatch latch = new CountDownLatch(1);

    public SaslServerContext(String mech, Address local_addr, CallbackHandler callback_handler, Map<String, String> props) throws SaslException {
        server = Sasl.createSaslServer(mech, "jgroups", local_addr.toString(), props, callback_handler);
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
        Message message = new Message(address).setFlag(Message.Flag.OOB);
        byte[] challenge = server.evaluateResponse(header.getPayload());
        if (server.isComplete()) {
            latch.countDown();
        }
        if (challenge != null) {
            return message.putHeader(SASL.SASL_ID, new SaslHeader(Type.RESPONSE, challenge));
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