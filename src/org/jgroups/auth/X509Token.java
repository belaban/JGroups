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
 * @author Bela Ban
 * @see org.jgroups.auth.AuthToken
 */
public class X509Token extends AuthToken {
    private boolean         valueSet;

    @Property
    protected String        keystore_type = "JKS";

    @Property
    protected String        cert_alias;

    @Property
    protected String        keystore_path;

    @Property(exposeAsManagedAttribute=false)
    protected String        auth_value;

    @Property
    protected String        cipher_type = "RSA";

    private byte[]          encryptedToken;
    private char[]          cert_password;
    private char[]          keystore_password;
    private Cipher          cipher;
    private PrivateKey      certPrivateKey;
    private X509Certificate certificate;

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
        return X509Token.class.getName();
    }

    public boolean authenticate(AuthToken token, Message msg) {
        if (!this.valueSet) {
            log.error(Util.getMessage("X509TokenNotSetupCorrectlyCheckTokenAttrs"));
            return false;
        }

        if (token instanceof X509Token) {
            // got a valid X509 token object
            X509Token serverToken = (X509Token) token;
            if (!serverToken.valueSet) {
                log.error(Util.getMessage("X509TokenReceivedTokenNotValid"));
                return false;
            }

            try {
                this.cipher.init(Cipher.DECRYPT_MODE, this.certPrivateKey);
                String serverBytes = new String(this.cipher.doFinal(serverToken.encryptedToken));
                if ((serverBytes.equalsIgnoreCase(this.auth_value))) {
                    log.debug("X509 authentication passed");
                    return true;
                }
            } catch (Exception e) {
                log.error(e.toString());
            }
        }
        return false;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Util.writeByteBuffer(this.encryptedToken, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
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
        InputStream inputStream=Thread.currentThread().getContextClassLoader().getResourceAsStream(this.keystore_path);
        if(inputStream == null)
            inputStream=new FileInputStream(this.keystore_path);
        store.load(inputStream, this.keystore_password);

        this.cipher = Cipher.getInstance(this.cipher_type);
        this.certificate = (X509Certificate) store.getCertificate(this.cert_alias);

        log.debug("certificate = " + this.certificate.toString());

        this.cipher.init(Cipher.ENCRYPT_MODE, this.certificate);
        this.encryptedToken = this.cipher.doFinal(this.auth_value.getBytes());

        KeyStore.PrivateKeyEntry privateKey = (KeyStore.PrivateKeyEntry) store.getEntry(
                        this.cert_alias, new KeyStore.PasswordProtection(this.cert_password));
        this.certPrivateKey = privateKey.getPrivateKey();

        this.valueSet=true;
    }
}
