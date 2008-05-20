package org.jgroups.auth;

import org.jgroups.util.Util;
import org.jgroups.Message;

import javax.crypto.Cipher;
import java.io.*;
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
public class X509Token extends AuthToken {

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

    public X509Token() {
        //need an empty constructor
    }

    public void setValue(Properties properties) {
        if(log.isDebugEnabled()){
            log.debug("setting values on X509Token object");
        }

        if(properties.containsKey(TOKEN_ATTR)){
            this.token_attr = (String) properties.get(TOKEN_ATTR);
            properties.remove(TOKEN_ATTR);
            if(log.isDebugEnabled()){
                log.debug("token_attr = " + this.token_attr);
            }
        }

        if(properties.containsKey(KEYSTORE_TYPE)){
            this.keystore_type = (String) properties.get(KEYSTORE_TYPE);
            properties.remove(KEYSTORE_TYPE);
            if(log.isDebugEnabled()){
                log.debug("keystore_type = " + this.keystore_type);
            }
        }else{
            this.keystore_type = "JKS";
            if(log.isDebugEnabled()){
                log.debug("keystore_type = " + this.keystore_type);
            }
        }

        if(properties.containsKey(KEYSTORE_PATH)){
            this.keystore_path = (String) properties.get(KEYSTORE_PATH);
            properties.remove(KEYSTORE_PATH);
            if(log.isDebugEnabled()){
                log.debug("keystore_path = " + this.keystore_path);
            }
        }

        if(properties.containsKey(KEYSTORE_PASSWORD)){
            this.keystore_password = ((String) properties.get(KEYSTORE_PASSWORD)).toCharArray();
            properties.remove(KEYSTORE_PASSWORD);
            if(log.isDebugEnabled()){
                log.debug("keystore_password = " + this.keystore_password);
            }
        }

        if(properties.containsKey(CERT_ALIAS)){
            this.cert_alias = (String) properties.get(CERT_ALIAS);
            properties.remove(CERT_ALIAS);
            if(log.isDebugEnabled()){
                log.debug("cert_alias = " + this.cert_alias);
            }
        }

        if(properties.containsKey(CERT_PASSWORD)){
            this.cert_password = ((String) properties.get(CERT_PASSWORD)).toCharArray();
            properties.remove(CERT_PASSWORD);
            if(log.isDebugEnabled()){
                log.debug("cert_password = " + this.cert_password);
            }
        }else{
            this.cert_password = this.keystore_password;
            if(log.isDebugEnabled()){
                log.debug("cert_password = " + this.cert_password);
            }
        }

        if(properties.containsKey(CIPHER_TYPE)){
            this.cipher_type = (String) properties.get(CIPHER_TYPE);
            properties.remove(CIPHER_TYPE);
            if(log.isDebugEnabled()){
                log.debug("cipher_type = " + this.cipher_type);
            }
        }else{
            this.cipher_type = "RSA";
            if(log.isDebugEnabled()){
                log.debug("cipher_type = " + this.cipher_type);
            }
        }

        if(getCertificate()){
            this.valueSet = true;
            if(log.isDebugEnabled()){
                log.debug("X509Token created correctly");
            }
        }
    }

    public String getName() {
        return "org.jgroups.auth.X509Token";
    }

    public boolean authenticate(AuthToken token, Message msg) {
        if (!this.valueSet) {
            if(log.isFatalEnabled()){
                log.fatal("X509Token not setup correctly - check token attrs");
            }
            return false;
        }

        if((token != null) && (token instanceof X509Token)){
            //got a valid X509 token object
            X509Token serverToken = (X509Token)token;
            if(!serverToken.valueSet){
                if(log.isFatalEnabled()){
                    log.fatal("X509Token - recieved token not valid");
                }
                return false;
            }

            try{
                if(log.isDebugEnabled()){
                    log.debug("setting cipher to decrypt mode");
                }
                this.cipher.init(Cipher.DECRYPT_MODE, this.certPrivateKey);
                String serverBytes = new String(this.cipher.doFinal(serverToken.encryptedToken));
                if((serverBytes != null) && (serverBytes.equalsIgnoreCase(this.token_attr))){
                    if(log.isDebugEnabled()){
                        log.debug("X509 authentication passed");
                    }
                    return true;
                }
            }catch(Exception e){
                if(log.isFatalEnabled()){
                    log.fatal(e);
                }
            }
        }
//        if(log.isWarnEnabled()){
//            log.warn("X509 authentication failed");
//        }
        return false;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        if(log.isDebugEnabled()){
            log.debug("X509Token writeTo()");
        }
        Util.writeByteBuffer(this.encryptedToken, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        if(log.isDebugEnabled()){
            log.debug("X509Token readFrom()");
        }
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

            if(log.isDebugEnabled()){
                log.debug("certificate = " + this.certificate.toString());
            }

            this.cipher.init(Cipher.ENCRYPT_MODE, this.certificate);
            this.encryptedToken = this.cipher.doFinal(this.token_attr.getBytes());

            if(log.isDebugEnabled()){
                log.debug("encryptedToken = " + this.encryptedToken);
            }

            KeyStore.PrivateKeyEntry privateKey = (KeyStore.PrivateKeyEntry)store.getEntry(this.cert_alias, new KeyStore.PasswordProtection(this.cert_password));
            this.certPrivateKey = privateKey.getPrivateKey();

            if(log.isDebugEnabled()){
                log.debug("certPrivateKey = " + this.certPrivateKey.toString());
            }

            return true;
        }catch(Exception e){
            if(log.isFatalEnabled()){
                log.fatal(e);
            }
            return false;
        }
    }
}
