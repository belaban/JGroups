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
 * @author Martes G Wigglesworth
 */
public class SaslClientCallbackHandler implements CallbackHandler {

    private String client_name;
    private char[] client_password;
    
    public SaslClientCallbackHandler()
    {
        client_name = null;
        client_password = null;
    }

    public SaslClientCallbackHandler(String name, char[] password) {
        this.client_name = name;
        this.client_password = password;
    }
    
    public String getClient_Name()
    {
        return this.client_name;
    }
    
    public char[] getClient_Password()
    {
        return this.client_password;
    }
    public void setClient_Name(String newName)
    {
        this.client_name = newName;
    }
    
    public void setClient_Password(char[] newPassword)
    {
        this.client_password = newPassword;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(client_password);
            } else if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(client_name);
            }
        }
    }

}
