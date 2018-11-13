package org.jgroups.auth;

import org.ietf.jgss.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Base64;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Support class that implements all the low level Kerberos security calls
 * @author Martin Swales
 * @author Claudio Corsi
 */
public class Krb5TokenUtils {
	
    private static final Log log = LogFactory.getLog(Krb5TokenUtils.class);
	
    private static Oid krb5Oid;
	
    static {
        try {
            krb5Oid = new Oid("1.2.840.113554.1.2.2");
        } catch(Exception e) {
            log.error(Util.getMessage("ExceptionWasGeneratedWhileCreatingAnOidInstance"), e);
            // Set to null to use the default mechanism.
            krb5Oid = null;
        }
    }
    
    // Authenticate against the KDC using JAAS.
    public static Subject generateSecuritySubject(String jassLoginConfig, String username, String password) throws LoginException {
  	  
        LoginContext loginCtx = null;
      
        try {
      
            // "Client" references the JAAS configuration in the jaas.conf file.
            loginCtx = new LoginContext(jassLoginConfig, new Krb5TokenUtils.LoginCallbackHandler(username, password));
            loginCtx.login();
 
            log.debug(" : Krb5Token Kerberos login succeeded against user: %s", username);
      
            return loginCtx.getSubject();
        }
        catch(LoginException e) {
            log.debug(" : Krb5Token Kerberos login failed against user: %s", username);
            throw e;
        }
    }
    
    // Generate the service ticket that will be passed to the cluster master for authentication
    public static byte[] initiateSecurityContext(Subject subject, String servicePrincipalName) throws GSSException {
        GSSManager manager    = GSSManager.getInstance();
        GSSName    serverName = manager.createName(servicePrincipalName, GSSName.NT_HOSTBASED_SERVICE);

        final GSSContext context = manager.createContext(serverName, krb5Oid, null, GSSContext.DEFAULT_LIFETIME);

        // The GSS context initiation has to be performed as a privileged action.
        return Subject.doAs(subject,
                            (PrivilegedAction<byte[]>)() -> {
                                try {
                                    byte[] token = new byte[0];
                                    // This is a one pass context initialization.
                                    context.requestMutualAuth(false);
                                    context.requestCredDeleg(false);
                                    return context.initSecContext(token, 0,
                                                                  token.length);
                                } catch (GSSException e) {
                                    log.error(Util.getMessage("Krb5TokenKerberosContextProcessingException"),e);
                                    return null;
                                }
                            });
    }
	
    // Validate the service ticket by extracting the client principal name
    public static String validateSecurityContext(Subject subject, final byte[] serviceTicket) throws GSSException {

        // Accept the context and return the client principal name.
        return Subject.doAs(subject, (PrivilegedAction<String>)() -> {
            try {
                // Identify the server that communications are being made
                // to.
                GSSManager manager = GSSManager.getInstance();
                GSSContext context = manager.createContext((GSSCredential) null);
                context.acceptSecContext(serviceTicket, 0, serviceTicket.length);
                return context.getSrcName().toString();
            } catch (Exception e) {
                log.error(Util.getMessage("Krb5TokenKerberosContextProcessingException"),e);
                return null;
            }
        });
    }
    
    public static void encodeDataToStream(byte[] data, DataOutput out) throws Exception {
        String encodedToken =Base64.encodeBytes(data); //  DatatypeConverter.printBase64Binary(data);
		
        log.debug(" : Written Encoded Data: \n%s", encodedToken);
		
        Bits.writeString(encodedToken,out);
    }
	
    public static byte[] decodeDataFromStream(DataInput in) throws Exception {
        String str = Bits.readString(in);
        log.debug(" : Read Encoded Data: \n%s", str);
        return Base64.decode(str); //  DatatypeConverter.parseBase64Binary(str);
    }
    
    /*
     * Callback Handler Class
     */
    public static class LoginCallbackHandler implements CallbackHandler {
		
        private String password;
        private String username;

        public LoginCallbackHandler() {
            super();
        }

        public LoginCallbackHandler(String name, String password) {
            super();
            this.username = name;
            this.password = password;
        }

        public LoginCallbackHandler(String password) {
            super();
            this.password = password;
        }

        /**
         * Handles the callbacks, and sets the user/password detail.
         *
         * @param callbacks
         *            the callbacks to handle
         * @throws IOException
         *             if an input or output error occurs.
         */
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {

            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback && username != null) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(username);
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    pc.setPassword(password.toCharArray());
                } else {
                    /*
		     * throw new UnsupportedCallbackException( callbacks[i],
		     * "Unrecognized Callback");
		     */
                }
            }
        }
    }

}
