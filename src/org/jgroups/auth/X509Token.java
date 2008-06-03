package org.jgroups.auth;

import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.util.Util;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.BadPaddingException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.security.*;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.util.Properties;

/**
 * <p>
 * This is an example of using a preshared token that is encrypted using an X509 certificate for authentication purposes.  All members of the group have to have the same string value in the JGroups config.
 * </p>
 * <p>
 * This example uses certificates contained within a specified keystore.  Configuration parameters for this example are shown below:
 * </p>
 * <ul>
 * <li>keystore_type = JKS(default)/PKCS12 - see http://java.sun.com/j2se/1.4.2/docs/guide/security/CryptoSpec.html#AppA</li>
 * <li>keystore_path (required) = the location of the keystore</li>
 * <li>keystore_password (required) =  the password of the keystore</li>
 * <li>cert_alias (required) = the alias of the certification within the keystore</li>
 * <li>cert_password = the password of the certification within the keystore</li>
 * <li>auth_value (required) = the string to encrypt</li>
 * <li>cipher_type = RSA(default)/AES/Blowfish/DES/DESede/PBEWithMD5AndDES/PBEWithHmacSHA1AndDESede/RC2/RC4/RC5 - see http://java.sun.com/j2se/1.4.2/docs/guide/security/jce/JCERefGuide.html#AppA</li>
 * </ul>
 * @author Chris Mills
 * @see org.jgroups.auth.AuthToken
 */
public class X509Token extends AuthToken {

    public static final String KEYSTORE_TYPE="keystore_type";
    public static final String KEYSTORE_PATH="keystore_path";
    public static final String KEYSTORE_PASSWORD="keystore_password";
    public static final String CERT_ALIAS="cert_alias";
    public static final String CERT_PASSWORD="cert_password";
    public static final String TOKEN_ATTR="auth_value";
    public static final String CIPHER_TYPE="cipher_type";

    private boolean valueSet=false;

    @Property
    private String keystore_type="JKS";

    @Property
    private String cert_alias=null;

    @Property
    private String keystore_path=null;

    @Property
    private String auth_value=null;

    @Property
    private String cipher_type="RSA";

    private byte[] encryptedToken=null;

    private char[] cert_password=null;
    private char[] keystore_password=null;

    private Cipher cipher=null;
    private PrivateKey certPrivateKey=null;
    private X509Certificate certificate=null;

    public X509Token() {
        //need an empty constructor
    }


    @Property(name="cert_password")
    public void setCertPassword(String pwd) {
        this.cert_password=pwd.toCharArray();
    }

    @Property(name="keystore_password")
    public void setKeyStorePassword(String pwd) {
        this.keystore_password=pwd.toCharArray();
        if(cert_password == null)
            cert_password=keystore_password;
    }



    public String getName() {
        return "org.jgroups.auth.X509Token";
    }

    public boolean authenticate(AuthToken token, Message msg) {
        if(!this.valueSet) {
            if(log.isFatalEnabled()) {
                log.fatal("X509Token not setup correctly - check token attrs");
            }
            return false;
        }

        if((token != null) && (token instanceof X509Token)) {
            //got a valid X509 token object
            X509Token serverToken=(X509Token)token;
            if(!serverToken.valueSet) {
                if(log.isFatalEnabled()) {
                    log.fatal("X509Token - recieved token not valid");
                }
                return false;
            }

            try {
                if(log.isDebugEnabled()) {
                    log.debug("setting cipher to decrypt mode");
                }
                this.cipher.init(Cipher.DECRYPT_MODE, this.certPrivateKey);
                String serverBytes=new String(this.cipher.doFinal(serverToken.encryptedToken));
                if((serverBytes.equalsIgnoreCase(this.auth_value))) {
                    if(log.isDebugEnabled()) {
                        log.debug("X509 authentication passed");
                    }
                    return true;
                }
            }
            catch(Exception e) {
                if(log.isFatalEnabled()) {
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
        if(log.isDebugEnabled()) {
            log.debug("X509Token writeTo()");
        }
        Util.writeByteBuffer(this.encryptedToken, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        if(log.isDebugEnabled()) {
            log.debug("X509Token readFrom()");
        }
        this.encryptedToken=Util.readByteBuffer(in);
        this.valueSet=true;
    }

    /**
     * Used during setup to get the certification from the keystore and encrypt the auth_value with the private key
     * @return true if the certificate was found and the string encypted correctly otherwise returns false
     */
    public void setCertificate() throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, UnrecoverableEntryException {
        KeyStore store=KeyStore.getInstance(this.keystore_type);
        java.io.FileInputStream fis=new java.io.FileInputStream(this.keystore_path);
        store.load(fis, this.keystore_password);

        this.cipher=Cipher.getInstance(this.cipher_type);
        this.certificate=(X509Certificate)store.getCertificate(this.cert_alias);

        if(log.isDebugEnabled()) {
            log.debug("certificate = " + this.certificate.toString());
        }

        this.cipher.init(Cipher.ENCRYPT_MODE, this.certificate);
        this.encryptedToken=this.cipher.doFinal(this.auth_value.getBytes());

        if(log.isDebugEnabled()) {
            log.debug("encryptedToken = " + this.encryptedToken);
        }

        KeyStore.PrivateKeyEntry privateKey=(KeyStore.PrivateKeyEntry)store.getEntry(this.cert_alias, new KeyStore.PasswordProtection(this.cert_password));
        this.certPrivateKey=privateKey.getPrivateKey();

        if(log.isDebugEnabled()) {
            log.debug("certPrivateKey = " + this.certPrivateKey.toString());
        }
    }
}
