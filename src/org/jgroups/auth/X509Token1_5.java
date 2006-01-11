package org.jgroups.auth;

import org.jgroups.util.Util;

import javax.crypto.Cipher;
import java.io.*;
import java.util.HashMap;
import java.util.Properties;
import java.security.cert.X509Certificate;
import java.security.PrivateKey;
import java.security.KeyStore;
/**
 * <p>
 * This is an example of using a preshared token that is encrypted using an X509 certificate for authentication purposes.  All members of the group have to have the same string value in the JGroups config.
 * </p>
 * <p>
 * This example uses certificates contained within a specified keystore.  Configuration parameters for this example are shown below:
 * </p>
 * <ul>
 *  <li>keystore_type = JKS(default)/PKCS12 - see http://java.sun.com/j2se/1.4.2/docs/guide/security/CryptoSpec.html#AppA</li>
 *  <li>keystore_path (required) = the location of the keystore</li>
 *  <li>keystore_password (required) =  the password of the keystore</li>
 *  <li>cert_alias (required) = the alias of the certification within the keystore</li>
 *  <li>cert_password = the password of the certification within the keystore</li>
 *  <li>auth_value (required) = the string to encrypt</li>
 *  <li>cipher_type = RSA(default)/AES/Blowfish/DES/DESede/PBEWithMD5AndDES/PBEWithHmacSHA1AndDESede/RC2/RC4/RC5 - see http://java.sun.com/j2se/1.4.2/docs/guide/security/jce/JCERefGuide.html#AppA</li>
 * </ul>
 * @see org.jgroups.auth.AuthToken
 * @author Chris Mills
 */
public class X509Token1_5 extends AuthToken {

    public static final String KEYSTORE_TYPE = "keystore_type";
    public static final String KEYSTORE_PATH = "keystore_path";
    public static final String KEYSTORE_PASSWORD = "keystore_password";
    public static final String CERT_ALIAS = "cert_alias";
    public static final String CERT_PASSWORD = "cert_password";
    public static final String TOKEN_ATTR = "auth_value";
    public static final String CIPHER_TYPE = "cipher_type";

    private boolean valueSet = false;

    private String keystore_type = null;
    private String cert_alias = null;
    private String keystore_path = null;
    private String token_attr = null;
    private String cipher_type = null;

    private byte[] encryptedToken = null;

    private char[] cert_password = null;
    private char[] keystore_password = null;

    private Cipher cipher = null;
    private PrivateKey certPrivateKey = null;
    private X509Certificate certificate = null;

    public X509Token1_5() {
        //need an empty constructor
    }

    public void setValue(Properties properties) {
        log.debug("setting values on X509Token1_5 object");

        if(properties.containsKey(X509Token1_5.TOKEN_ATTR)){
            this.token_attr = (String) properties.get(X509Token1_5.TOKEN_ATTR);
            properties.remove(X509Token1_5.TOKEN_ATTR);
            log.debug("token_attr = " + this.token_attr);
        }

        if(properties.containsKey(X509Token1_5.KEYSTORE_TYPE)){
            this.keystore_type = (String) properties.get(X509Token1_5.KEYSTORE_TYPE);
            properties.remove(X509Token1_5.KEYSTORE_TYPE);
            log.debug("keystore_type = " + this.keystore_type);
        }else{
            this.keystore_type = "JKS";
            log.debug("keystore_type = " + this.keystore_type);
        }

        if(properties.containsKey(X509Token1_5.KEYSTORE_PATH)){
            this.keystore_path = (String) properties.get(X509Token1_5.KEYSTORE_PATH);
            properties.remove(X509Token1_5.KEYSTORE_PATH);
            log.debug("keystore_path = " + this.keystore_path);
        }

        if(properties.containsKey(X509Token1_5.KEYSTORE_PASSWORD)){
            this.keystore_password = ((String) properties.get(X509Token1_5.KEYSTORE_PASSWORD)).toCharArray();
            properties.remove(X509Token1_5.KEYSTORE_PASSWORD);
            log.debug("keystore_password = " + this.keystore_password);
        }

        if(properties.containsKey(X509Token1_5.CERT_ALIAS)){
            this.cert_alias = (String) properties.get(X509Token1_5.CERT_ALIAS);
            properties.remove(X509Token1_5.CERT_ALIAS);
            log.debug("cert_alias = " + this.cert_alias);
        }

        if(properties.containsKey(X509Token1_5.CERT_PASSWORD)){
            this.cert_password = ((String) properties.get(X509Token1_5.CERT_PASSWORD)).toCharArray();
            properties.remove(X509Token1_5.CERT_PASSWORD);
            log.debug("cert_password = " + this.cert_password);
        }else{
            this.cert_password = this.keystore_password;
            log.debug("cert_password = " + this.cert_password);
        }

        if(properties.containsKey(X509Token1_5.CIPHER_TYPE)){
            this.cipher_type = (String) properties.get(X509Token1_5.CIPHER_TYPE);
            properties.remove(X509Token1_5.CIPHER_TYPE);
            log.debug("cipher_type = " + this.cipher_type);
        }else{
            this.cipher_type = "RSA";
            log.debug("cipher_type = " + this.cipher_type);
        }

        if(getCertificate()){
            this.valueSet = true;
            log.debug("X509Token1_5 created correctly");
        }
    }

    public String getName() {
        return "org.jgroups.auth.X509Token1_5";
    }

    public boolean authenticate(AuthToken token) {
        if (!this.valueSet) {
            log.fatal("X509Token1_5 not setup correctly - check token attrs");
            return false;
        }

        if((token != null) && (token instanceof X509Token1_5)){
            //got a valid X509 token object
            X509Token1_5 serverToken = (X509Token1_5)token;
            if(!serverToken.valueSet){
                log.fatal("X509Token1_5 - recieved token not valid");
                return false;
            }

            try{
                log.debug("setting cipher to decrypt mode");
                this.cipher.init(Cipher.DECRYPT_MODE, this.certPrivateKey);
                String serverBytes = new String(this.cipher.doFinal(serverToken.encryptedToken));
                if((serverBytes != null) && (serverBytes.equalsIgnoreCase(this.token_attr))){
                    log.debug("X509 authentication passed");
                    return true;
                }
            }catch(Exception e){
                log.fatal(e);
            }
        }
        log.warn("X509 authentication failed");
        return false;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        log.debug("X509Token1_5 writeTo()");
        Util.writeByteBuffer(this.encryptedToken, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        log.debug("X509Token1_5 readFrom()");
        this.encryptedToken = Util.readByteBuffer(in);
        this.valueSet = true;
    }
    /**
     * Used during setup to get the certification from the keystore and encrypt the auth_value with the private key
     * @return true if the certificate was found and the string encypted correctly otherwise returns false
     */
    private boolean getCertificate() {
        try{
            KeyStore store = KeyStore.getInstance(this.keystore_type);
            java.io.FileInputStream fis = new java.io.FileInputStream(this.keystore_path);
            store.load(fis, this.keystore_password);

            this.cipher = Cipher.getInstance(this.cipher_type);
            this.certificate = (X509Certificate) store.getCertificate(this.cert_alias);

            log.debug("certificate = " + this.certificate.toString());

            this.cipher.init(Cipher.ENCRYPT_MODE, this.certificate);
            this.encryptedToken = this.cipher.doFinal(this.token_attr.getBytes());

            log.debug("encryptedToken = " + this.encryptedToken);

            KeyStore.PrivateKeyEntry privateKey = (KeyStore.PrivateKeyEntry)store.getEntry(this.cert_alias, new KeyStore.PasswordProtection(this.cert_password));
            this.certPrivateKey = privateKey.getPrivateKey();

            log.debug("certPrivateKey = " + this.certPrivateKey.toString());

            return true;
        }catch(Exception e){
            log.fatal(e);
            return false;
        }
    }
}
