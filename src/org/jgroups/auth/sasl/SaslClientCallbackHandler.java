package org.jgroups.auth.sasl;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import java.io.IOException;

/**
 * SaslClientCallbackHandler.
 *
 * @author Tristan Tarrant
 */
public class SaslClientCallbackHandler implements CallbackHandler {

    private final String name;
    private final char[] password;
    private final String realm;

    public SaslClientCallbackHandler(String name, char[] password) {
        int realmSep = name != null ? name.indexOf('@') : -1;
        this.realm = realmSep < 0 ? "" : name.substring(realmSep+1);
        this.name = realmSep < 0 ? name : name.substring(0, realmSep);
        this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(password);
            } else if (callback instanceof RealmCallback) {
                ((RealmCallback) callback).setText(realm);
            } else if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(name);
            }
        }
    }

}
