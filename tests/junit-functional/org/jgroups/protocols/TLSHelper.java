package org.jgroups.protocols;

import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.SocketFactory;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TLS-related utility functions
 * @author Bela Ban
 * @since  5.2.15
 */
public class TLSHelper {
    public  static final String PROTOCOL = "TLSv1.2";
    public  static final String BASE_DN = "CN=%s,OU=JGroups,O=JBoss,L=Red Hat";
    public  static final String KEY_PASSWORD = "secret";
    public  static final String KEY_ALGORITHM = "RSA";
    public  static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";
    public  static final String KEYSTORE_TYPE = "pkcs12";
    private static final SelfSignedX509CertificateAndSigningKey DEFAULT_CA;
    private static final AtomicLong certSerial = new AtomicLong(1);
    private static final X500Principal DEFAULT_CA_DN;

    static {
        DEFAULT_CA_DN = dn("CA");
        DEFAULT_CA=createSelfSignedCertificate(DEFAULT_CA_DN, true, KEY_SIGNATURE_ALGORITHM, KEY_ALGORITHM);
    }

    /** Generates a key pair from the default algorithm ("RSA") */
    public static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        return generateKeyPair(KEY_ALGORITHM);
    }

    /** Generates a key pair from the given algorithm */
    public static KeyPair generateKeyPair(String key_algorithm) throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator=KeyPairGenerator.getInstance(key_algorithm);
        return keyPairGenerator.generateKeyPair();
    }

    public static KeyStore createKeyStore() {
        return createKeyStore(KEYSTORE_TYPE);
    }

    public static KeyStore createKeyStore(String keystore_type) {
        try {
            KeyStore keyStore = KeyStore.getInstance(keystore_type);
            keyStore.load(null);
            return keyStore;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /** Gets the private key from a given key pair */
    public static PrivateKey getPrivateKey(KeyPair kp) {
        return kp.getPrivate();
    }

    /** Gets the public key from a given key pair */
    public static PublicKey getPublicKey(KeyPair kp) {
        return kp.getPublic();
    }

    /** Creates a prinicipal from a common name, using the default base name to form an X500 name */
    public static X500Principal createPrincipal(String name) {return createPrinicipal(String.format(BASE_DN, name));}

    /** Creates a principal from the given distinguished name (X500 name). Throws an exception of the name is not an
     * X500 name */
    public static X500Principal createPrinicipal(String dn) {return new X500Principal(dn);}


    public static SelfSignedX509CertificateAndSigningKey createSelfSignedCertificate(X500Principal dn, boolean is_ca,
                                                                                     String key_signature_alg,
                                                                                     String key_alg) {
        SelfSignedX509CertificateAndSigningKey.Builder builder=SelfSignedX509CertificateAndSigningKey.builder()
          .setDn(dn)
          .setSignatureAlgorithmName(key_signature_alg)
          .setKeyAlgorithmName(key_alg);

        if(is_ca)
            builder.addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647");
        return builder.build();
    }

    public static X509Certificate createSignedCertificate(PublicKey publicKey,
                                                          SelfSignedX509CertificateAndSigningKey ca,
                                                          X500Principal issuerDN, String name) {
        try {
            return new X509CertificateBuilder()
              .setIssuerDn(issuerDN)
              .setSubjectDn(dn(name))
              .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
              .setSigningKey(ca.getSigningKey())
              .setPublicKey(publicKey)
              .setSerialNumber(BigInteger.valueOf(certSerial.getAndIncrement()))
              .addExtension(new BasicConstraintsExtension(false, false, -1))
              .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static KeyStore createCertAndAddToKeystore(PrivateKey signingKey, PublicKey publicKey,
                                                      SelfSignedX509CertificateAndSigningKey ca,
                                                      X500Principal issuerDN,
                                                      String name, KeyStore trustStore,
                                                      String keystore_type, String key_password) {
        try {
            X509Certificate caCertificate=ca.getSelfSignedCertificate();
            X509Certificate certificate=createSignedCertificate(publicKey, ca, issuerDN, name);
            trustStore.setCertificateEntry(name, certificate);
            try {
                KeyStore ks=createKeyStore(keystore_type);
                ks.setCertificateEntry("ca", caCertificate);
                ks.setKeyEntry(name, signingKey, key_password.toCharArray(), new X509Certificate[]{certificate, caCertificate});
                return ks;
            } catch (KeyStoreException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static SSLContext getSSLContext() throws NoSuchAlgorithmException, UnrecoverableKeyException,
                                                    KeyStoreException, KeyManagementException {
        return getSSLContext("X", PROTOCOL, DEFAULT_CA, DEFAULT_CA_DN, KEYSTORE_TYPE, KEY_PASSWORD);
    }

    public static SSLContext getSSLContext(String name, String protocol,
                                           SelfSignedX509CertificateAndSigningKey ca,
                                           X500Principal issuerDN,
                                           String keyStoreType, String keyStorePassword)
      throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, KeyManagementException {
        KeyPair kp=generateKeyPair();
        SSLContext sslContext=SSLContext.getInstance(protocol);
        KeyManagerFactory keyManagerFactory=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore trustStore=createKeyStore(keyStoreType);
        TrustManagerFactory trustManagerFactory=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustStore.setCertificateEntry("ca", ca.getSelfSignedCertificate());
        trustManagerFactory.init(trustStore);
        KeyStore keyStore=createCertAndAddToKeystore(kp.getPrivate(), kp.getPublic(), ca, issuerDN, name,
                                                     trustStore, keyStoreType, keyStorePassword);
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    public static org.jgroups.util.SocketFactory getSSLSocketFactory() throws UnrecoverableKeyException, NoSuchAlgorithmException,
                                                                              KeyStoreException, KeyManagementException {
        return new DefaultSocketFactory(getSSLContext());
    }


    protected static X500Principal dn(String cn) {
        return new X500Principal(String.format(BASE_DN, cn));
    }



    public static void main(String[] args) throws NoSuchAlgorithmException, IOException, UnrecoverableKeyException,
                                                  KeyStoreException, KeyManagementException {
        SocketFactory factory=getSSLSocketFactory();
        ServerSocket srv_sock=factory.createServerSocket("srv-sock", 7000);
        new Thread(() -> {
            try {
                Socket client=srv_sock.accept();
                InputStream in=client.getInputStream();
                while(!client.isClosed()) {
                    int c=in.read();
                    if(c < 0)
                        break;
                    System.out.printf("%c", (char)c);
                }
            }
            catch(IOException e) {
                throw new RuntimeException(e);
            }
        }, "srv-sock-handler").start();

        try(Socket client_sock=factory.createSocket("client-sock", InetAddress.getLocalHost(), 7000);
            OutputStream out=client_sock.getOutputStream()) {
            out.write("Hello world".getBytes());
            out.flush();
        }

    }


}
