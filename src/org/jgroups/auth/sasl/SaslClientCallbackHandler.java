package org.jgroups.auth.sasl;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * SaslClientCallbackHandler.
 *
 * @author Tristan Tarrant
 */
public class SaslClientCallbackHandler implements CallbackHandler {

    private final String name;
    private final char[] password;

    public SaslClientCallbackHandler(String name, char[] password) {
        this.name = name;
        this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(password);
            } else if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(name);
            }
        }
    }

}
