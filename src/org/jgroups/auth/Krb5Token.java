package org.jgroups.auth;

import org.ietf.jgss.GSSException;
import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.util.Util;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

/**
 * JGroups AuthToken Class to for Kerberos v5 authentication.
 * @author Martin Swales
 * @author Claudio Corsi
 * @since 3.4
 */
@Experimental
public class Krb5Token extends AuthToken {
    private static final String JASS_SECURITY_CONFIG   = "JGoupsKrb5TokenSecurityConf";
    public  static final String CLIENT_PRINCIPAL_NAME  = "client_principal_name";
    public  static final String CLIENT_PASSWORD        = "client_password";
    public  static final String SERVICE_PRINCIPAL_NAME = "service_principal_name";
        
    @Property protected String client_principal_name;
    @Property(exposeAsManagedAttribute=false) protected String client_password;
    @Property protected String service_principal_name;
        
    private Subject subject;
    private byte[]  krbServiceTicket;
    private byte[]  remoteKrbServiceTicket;
        
        
    public Krb5Token() { // Need an empty constructor
    }
        
    public void setValue(Properties properties) {
        String value;
                
        if((value = properties.getProperty(CLIENT_PRINCIPAL_NAME)) != null){
            this.client_principal_name= value;
            properties.remove(CLIENT_PRINCIPAL_NAME);
        }
                
        if((value = properties.getProperty(CLIENT_PASSWORD)) != null){
            this.client_password= value;
            properties.remove(CLIENT_PASSWORD);
        }
                
        if((value = properties.getProperty(SERVICE_PRINCIPAL_NAME)) != null){
            this.service_principal_name= value;
            properties.remove(SERVICE_PRINCIPAL_NAME);
        }
                
        try {
            authenticateClientPrincipal();
        }
        catch (Exception e) {
            // If we get any kind of exception then blank the subject
            log.warn("Krb5Token failed to authenticate", e);
            subject = null;
        }
    }

    public String getName() {
        return Krb5Token.class.getName();
    }

    public boolean authenticate(AuthToken token, Message msg) {
        if (!isAuthenticated()) {
            log.error(Util.getMessage("Krb5TokenFailedToSetupCorrectlyCannotAuthenticateAnyPeers"));
            return false;
        }
        
        if(token instanceof Krb5Token) {
                
            Krb5Token remoteToken = (Krb5Token)token;

            try {
                validateRemoteServiceTicket(remoteToken);
                return true;
            }
            catch (Exception e) {
                log.error(Util.getMessage("Krb5TokenServiceTicketValidationFailed"), e);
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
        
        return false;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        if (isAuthenticated()) {
            generateServiceTicket();
            writeServiceTicketToSream(out);
        }
    }

    @Override
    public void readFrom(DataInput in) throws IOException {

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
        return (subject != null);
    }
    
    private void authenticateClientPrincipal() throws LoginException {
        subject  = Krb5TokenUtils.generateSecuritySubject(JASS_SECURITY_CONFIG, client_principal_name, client_password);
    }
    
    private void generateServiceTicket() throws IOException {
        try {
            krbServiceTicket = Krb5TokenUtils.initiateSecurityContext(subject,service_principal_name);
        }
        catch(GSSException ge) {
            throw new IOException("Failed to generate serviceticket", ge);
        }
    }
    
    private void validateRemoteServiceTicket(Krb5Token remoteToken) throws Exception {
        byte[] remoteKrbServiceTicketLocal = remoteToken.remoteKrbServiceTicket;
        
        String clientPrincipalName = Krb5TokenUtils.validateSecurityContext(subject, remoteKrbServiceTicketLocal);
        
        if (!clientPrincipalName.equals(this.client_principal_name))
            throw new Exception("Client Principal Names did not match");
    }
    
    private void writeServiceTicketToSream(DataOutput out) throws IOException {
        try {
            Krb5TokenUtils.encodeDataToStream(krbServiceTicket, out);
        } catch(IOException ioe) {
            throw ioe;
        } catch(Exception e) {
            throw new IOException(e);
        }
    }
    
    private void readRemoteServiceTicketFromStream(DataInput in) throws IOException {
        try {
            remoteKrbServiceTicket = Krb5TokenUtils.decodeDataFromStream(in);
        } catch(IOException ioe) {
            throw ioe;
        } catch(Exception e) {
            throw new IOException(e);
        }
    }
}
