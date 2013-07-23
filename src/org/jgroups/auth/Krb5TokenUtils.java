package org.jgroups.auth;

/*******************************************
 * 
 * Support class that implements all the low level Kerberos security calls
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.xml.bind.DatatypeConverter;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.jgroups.util.Util;

public class Krb5TokenUtils {
	
    private static final Log log = LogFactory.getLog(Krb5TokenUtils.class);
	
    private static Oid krb5Oid;
	
    static {
	try {
	    krb5Oid = new Oid("1.2.840.113554.1.2.2");
	} catch(Exception e) {
	    log.error("Exception was generated while creating an Oid instance", e);
	    // Set to null to use the default mechanism.
	    krb5Oid = null;
	}
    }
    
    // Authenticate against the KDC using JAAS.
    public Subject generateSecuritySubject(String jassLoginConfig, String username, String password) throws LoginException {
  	  
	LoginContext loginCtx = null;
      
	try {
      
	    // "Client" references the JAAS configuration in the jaas.conf file.
	    loginCtx = new LoginContext(jassLoginConfig, new Krb5TokenUtils.LoginCallbackHandler(username, password));
	    loginCtx.login();
 
	    log.debug(" : Krb5Token Kerberos login succeeded aganst user: %s", username);
      
	    return loginCtx.getSubject();
	}
	catch(LoginException e) {
	    log.debug(" : Krb5Token Kerberos login failed aganst user: %s", username);
	    throw e;
	}
    }
    
    // Generate the service ticket that will be passed to the cluster master for authentication
    public byte[] initiateSecurityContext(Subject subject, String servicePrincipalName) throws GSSException {
	GSSManager manager    = GSSManager.getInstance();
	GSSName    serverName = manager.createName(servicePrincipalName, GSSName.NT_HOSTBASED_SERVICE);

	final GSSContext context = manager.createContext(serverName, krb5Oid, null, GSSContext.DEFAULT_LIFETIME);

	// The GSS context initiation has to be performed as a privileged action.
	return Subject.doAs(subject,
			    new PrivilegedAction<byte[]>() {
				public byte[] run() {
				    try {
					byte[] token = new byte[0];
					// This is a one pass context initialization.
					context.requestMutualAuth(false);
					context.requestCredDeleg(false);
					return context.initSecContext(token, 0,
								      token.length);
				    } catch (GSSException e) {
					log.debug(" : Krb5Token Kerberos context processing exception", e);
					return null;
				    }
				}
			    });
    }
	
    // Validate the service ticket by extracting the client principal name
    public String validateSecurityContext(Subject subject, final byte[] serviceTicket) throws GSSException {

	// Accept the context and return the client principal name.
	return Subject.doAs(subject, new PrivilegedAction<String>() {
		public String run() {
		    try {
			// Identify the server that communications are being made
			// to.
			GSSManager manager = GSSManager.getInstance();
			GSSContext context = manager.createContext((GSSCredential) null);
			context.acceptSecContext(serviceTicket, 0, serviceTicket.length);
			return context.getSrcName().toString();
		    } catch (Exception e) {
			log.debug(" : Krb5Token Kerberos context processing exception", e);
			return null;
		    }
		}
	    });
    }
    
    public void encodeDataToStream(byte[] data, DataOutput out) throws Exception {
		
	log.debug(" : encodeDataToStream");
		
	String encodedToken = DatatypeConverter.printBase64Binary(data);
		
	log.debug(" : Written Encoded Data: \n%s", encodedToken);
		
	Util.writeString(encodedToken, out);
    }
	
    public byte[] decodeDataFromStream(DataInput in) throws Exception {
	log.debug(" : decodeDataFromStream");
		
	String str = Util.readString(in);
		
	log.debug(" : Read Encoded Data: \n%s", str);

	return DatatypeConverter.parseBase64Binary(str);
    }
    
    /*
     * Callback Handler Class
     */
    public class LoginCallbackHandler implements CallbackHandler {
		
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
