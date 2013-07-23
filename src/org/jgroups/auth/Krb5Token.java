package org.jgroups.auth;

/*
 * JGroups AuthToken Class to for Kerberos v5 authentication.  
 * 
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSException;
import org.jgroups.Message;
import org.jgroups.auth.AuthToken;
import org.jgroups.util.Util;

public class Krb5Token extends AuthToken {

    private static final long serialVersionUID = 2603254877873488217L;
        
    private static final String JASS_SECURITY_CONFIG = "JGoupsKrb5TokenSecurityConf";
    public  static final String CLIENT_PRINCIPAL_NAME = "client_principal_name";
    public  static final String CLIENT_PASSWORD = "client_password";
    public  static final String SERVICE_PRINCIPAL_NAME = "service_principal_name";
        
    private static final Krb5TokenUtils kerb5Utils = new Krb5TokenUtils();
        
    private String clientPrincipalName;
    private String clientPassword;
    private String servicePrincipalName;
        
    private Subject subject;
    private byte[]  krbServiceTicket;
    private byte[]  remoteKrbServiceTicket;
        
        
    public Krb5Token() {
	// Need an empty constructor
	log.debug(" : Krb5Token instance created");
    }
        
    public void setValue(Properties properties) {
	log.debug(" : In Krb5Token setValue");
                
	String value;
                
	if((value = properties.getProperty(CLIENT_PRINCIPAL_NAME)) != null){
            this.clientPrincipalName = value; 
            properties.remove(CLIENT_PRINCIPAL_NAME);
        }
                
	if((value = properties.getProperty(CLIENT_PASSWORD)) != null){
            this.clientPassword = value;
            properties.remove(CLIENT_PASSWORD);
        }
                
	if((value = properties.getProperty(SERVICE_PRINCIPAL_NAME)) != null){
            this.servicePrincipalName = value;
            properties.remove(SERVICE_PRINCIPAL_NAME);
        }
                
	try {
	    authenticateClientPrincipal();
	}
	catch (Exception e) {
	    // If we get any kind of exception then blank the subject
	    log.debug(" : Krb5Token failed to authenticate", e);
	    subject = null;
	}
                
    }

    public String getName() {
        return Krb5Token.class.getName();
    }

    public boolean authenticate(AuthToken token, Message msg) {
        log.debug(" : In Krb5Token authenticate");
        
        if (!isAuthenticated()) {
	    log.debug(" : Krb5Token failed to setup correctly - cannot authenticate any peers");
	    return false;
        }
        
        if((token != null) && 
           (token instanceof Krb5Token)) {
                
	    Krb5Token remoteToken = (Krb5Token)token;

	    try {
		validateRemoteServiceTicket(remoteToken);
		return true;
	    }
	    catch (Exception e) {
		log.debug(" : Krb5Token service ticket validation failed", e);
		return false;
	    }
                
	    /*
	      if((remoteToken.fingerPrint != null) && 
	      (this.fingerPrint.equalsIgnoreCase(remoteToken.fingerPrint))) {
	      log.debug(" : Krb5Token authenticate match");
	      return true;
	      }else {
	      log.debug(" : Krb5Token authenticate fail");
	      return false;
	      }
            */
        }
        
        log.debug(" : Krb5Token authenticate catch all fail");
        return false;
    }
    
    public void writeTo(DataOutput out) throws IOException {
        log.debug(" : In Krb5Token writeTo");
        
        if (isAuthenticated()) {
	    generateServiceTicket();
	    writeServiceTicketToSream(out);
        }
    }

    public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
        log.debug(" : In Krb5Token readFrom");
        
        // This method is called from within a temporary token so it has not authenticated to a client principal
        // This token is passed to the authenticate
        readRemoteServiceTicketFromStream(in);
    }
 
    public int size() {
        return Util.size(krbServiceTicket);
    }
    
    /******************************************************
     * 
     * Private Methods
     * 
     */
    
    private boolean isAuthenticated() {
        return !(subject == null);
    }
    
    private void authenticateClientPrincipal() throws LoginException {
        
        subject  = kerb5Utils.generateSecuritySubject(JASS_SECURITY_CONFIG, clientPrincipalName, clientPassword);

        log.debug(" : Instance Authenticated");
    }
    
    private void generateServiceTicket() throws IOException {
        try {
	    krbServiceTicket = kerb5Utils.initiateSecurityContext(subject, servicePrincipalName);
        }
        catch(GSSException ge) {
	    throw new IOException("Failed to generate serviceticket", ge);
        }
    }
    
    private void validateRemoteServiceTicket(Krb5Token remoteToken) throws Exception {
        byte[] remoteKrbServiceTicket = remoteToken.remoteKrbServiceTicket;
        
        String clientPrincipalName = kerb5Utils.validateSecurityContext(subject, remoteKrbServiceTicket);
        
        if (!clientPrincipalName.equals(this.clientPrincipalName))
	    throw new Exception("Client Principal Names did not match");
    }
    
    private void writeServiceTicketToSream(DataOutput out) throws IOException {
        log.debug(" : In writeServiceTicketToSream");
        try {
	    kerb5Utils.encodeDataToStream(krbServiceTicket, out);
        } catch(IOException ioe) {
	    throw ioe;
        } catch(Exception e) {
	    throw new IOException(e);
        }
    }
    
    private void readRemoteServiceTicketFromStream(DataInput in) throws IOException {
        log.debug(" : In readRemoteServiceTicketFromStream");
        try {
	    remoteKrbServiceTicket = kerb5Utils.decodeDataFromStream(in);
        } catch(IOException ioe) {
	    throw ioe;
        } catch(Exception e) {
	    throw new IOException(e);
        }
    }
}
