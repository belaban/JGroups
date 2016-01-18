package org.jgroups.auth.sasl;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.protocols.SaslHeader;

import javax.security.sasl.SaslException;

public interface SaslContext {
    boolean isSuccessful();

    boolean needsWrapping();

    byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException;

    byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException;

    void dispose();

    Message nextMessage(Address address, SaslHeader saslHeader) throws SaslException;
}
