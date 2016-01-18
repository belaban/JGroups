package org.jgroups.auth;

import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.util.Util;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * <p>
 * This is an example of using a preshared token that is encrypted using an X509 certificate for
 * authentication purposes. All members of the group have to have the same string value in the
 * JGroups config.
 * </p>
 * <p>
 * This example uses certificates contained within a specified keystore. Configuration parameters
 * for this example are shown below:
 * </p>
 * <ul>
 * <li>keystore_type = JKS(default)/PKCS12 - see
 * http://java.sun.com/j2se/1.4.2/docs/guide/security/CryptoSpec.html#AppA</li>
 * <li>keystore_path (required) = the location of the keystore</li>
 * <li>keystore_password (required) = the password of the keystore</li>
 * <li>cert_alias (required) = the alias of the certification within the keystore</li>
 * <li>cert_password = the password of the certification within the keystore</li>
 * <li>auth_value (required) = the string to encrypt</li>
 * <li>cipher_type =
 * RSA(default)/AES/Blowfish/DES/DESede/PBEWithMD5AndDES/PBEWithHmacSHA1AndDESede/RC2/RC4/RC5 - see
 * http://java.sun.com/j2se/1.4.2/docs/guide/security/jce/JCERefGuide.html#AppA</li>
 * </ul>
 * 
 * @author Chris Mills
 * @see AbstractAuthToken
 */
public class X509Token extends AbstractAuthToken {

    public static final String KEYSTORE_TYPE = "keystore_type";
    public static final String KEYSTORE_PATH = "keystore_path";
    public static final String KEYSTORE_PASSWORD = "keystore_password";
    public static final String CERT_ALIAS = "cert_alias";
    public static final String CERT_PASSWORD = "cert_password";
    public static final String TOKEN_ATTR = "auth_value";
    public static final String CIPHER_TYPE = "cipher_type";

    private boolean valueSet = false;

    @Property
    protected String keystore_type = "JKS";

    @Property
    protected String cert_alias = null;

    @Property
    protected String keystore_path = null;

    @Property(exposeAsManagedAttribute=false)
    protected String auth_value = null;

    @Property
    protected String cipher_type = "RSA";

    private byte[] encryptedToken = null;

    private char[] cert_password = null;
    private char[] keystore_password = null;

    private Cipher cipher = null;
    private PrivateKey certPrivateKey = null;
    private X509Certificate certificate = null;

    public X509Token() {
        // need an empty constructor
    }

    @Property(name = "cert_password",exposeAsManagedAttribute=false)
    public void setCertPassword(String pwd) {
        this.cert_password = pwd.toCharArray();
    }

    @Property(name = "keystore_password",exposeAsManagedAttribute=false)
    public void setKeyStorePassword(String pwd) {
        this.keystore_password = pwd.toCharArray();
        if (cert_password == null)
            cert_password = keystore_password;
    }

    /** To be used for testing only */
    public X509Token encryptedToken(byte[] buf) {
        encryptedToken=buf;
        return this;
    }

    public String getName() {
        return "org.jgroups.auth.X509Token";
    }

    public boolean authenticate(AbstractAuthToken token, Message msg) {
        if (!this.valueSet) {
            if (log.isErrorEnabled()) {
                log.error(Util.getMessage("X509TokenNotSetupCorrectlyCheckTokenAttrs"));
            }
            return false;
        }

        if ((token != null) && (token instanceof X509Token)) {
            // got a valid X509 token object
            X509Token serverToken = (X509Token) token;
            if (!serverToken.valueSet) {
                if (log.isErrorEnabled()) {
                    log.error(Util.getMessage("X509TokenReceivedTokenNotValid"));
                }
                return false;
            }

            try {
                if (log.isDebugEnabled()) {
                    log.debug("setting cipher to decrypt mode");
                }
                this.cipher.init(Cipher.DECRYPT_MODE, this.certPrivateKey);
                String serverBytes = new String(this.cipher.doFinal(serverToken.encryptedToken));
                if ((serverBytes.equalsIgnoreCase(this.auth_value))) {
                    if (log.isDebugEnabled()) {
                        log.debug("X509 authentication passed");
                    }
                    return true;
                }
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    log.error(e.toString());
                }
            }
        }
        // if(log.isWarnEnabled()){
        // log.warn("X509 authentication failed");
        // }
        return false;
    }

    public void writeTo(DataOutput out) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("X509Token writeTo()");
        }
        Util.writeByteBuffer(this.encryptedToken, out);
    }

    public void readFrom(DataInput in) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("X509Token readFrom()");
        }
        this.encryptedToken = Util.readByteBuffer(in);
        this.valueSet = true;
    }

    public int size() {
        return Util.size(encryptedToken);
    }

    /**
     * Used during setup to get the certification from the keystore and encrypt the auth_value with
     * the private key
     */
    public void setCertificate() throws KeyStoreException, IOException, NoSuchAlgorithmException,
                    CertificateException, NoSuchPaddingException, InvalidKeyException,
                    IllegalBlockSizeException, BadPaddingException, UnrecoverableEntryException {
        KeyStore store = KeyStore.getInstance(this.keystore_type);
        InputStream inputStream=null;
        inputStream=Thread.currentThread()
                          .getContextClassLoader()
                          .getResourceAsStream(this.keystore_path);
        if(inputStream == null)
          inputStream=new FileInputStream(this.keystore_path);
        store.load(inputStream, this.keystore_password);

        this.cipher = Cipher.getInstance(this.cipher_type);
        this.certificate = (X509Certificate) store.getCertificate(this.cert_alias);

        if (log.isDebugEnabled()) {
            log.debug("certificate = " + this.certificate.toString());
        }

        this.cipher.init(Cipher.ENCRYPT_MODE, this.certificate);
        this.encryptedToken = this.cipher.doFinal(this.auth_value.getBytes());

        if (log.isDebugEnabled()) {
            log.debug("encryptedToken = " + this.encryptedToken);
        }

        KeyStore.PrivateKeyEntry privateKey = (KeyStore.PrivateKeyEntry) store.getEntry(
                        this.cert_alias, new KeyStore.PasswordProtection(this.cert_password));
        this.certPrivateKey = privateKey.getPrivateKey();

        this.valueSet=true;

        if (log.isDebugEnabled()) {
            log.debug("certPrivateKey = " + this.certPrivateKey.toString());
        }
    }
}
