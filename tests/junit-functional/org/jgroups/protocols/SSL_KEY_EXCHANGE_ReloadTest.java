package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Test(groups = {Global.FUNCTIONAL, Global.ENCRYPT}, singleThreaded = true)
public class SSL_KEY_EXCHANGE_ReloadTest {
   public static final String PROTOCOL = "TLSv1.2";
   public static final String BASE_DN = "CN=%s,OU=JGroups,O=JBoss,L=Red Hat";
   public static final String KEY_PASSWORD = "secret";
   public static final String KEY_ALGORITHM = "RSA";
   public static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";
   public static final String KEYSTORE_TYPE = "pkcs12";
   public static final String KEYSTORE = "keystore.p12";
   public static final String TRUSTSTORE = "truststore.p12";
   private final AtomicLong certSerial = new AtomicLong(1);
   private final int certValidSeconds = 5;

   protected X500Principal CA_DN;
   protected SelfSignedX509CertificateAndSigningKey ca;
   protected KeyPair keyPair;

   final String cluster_name = getClass().getSimpleName();

   protected File tmpDir;
   protected File keyStoreFile;
   protected File trustStoreFile;

   protected void createTempDirectory() throws IOException {
      tmpDir = new File(System.getProperty("java.io.tmpdir"), File.separator + SSL_KEY_EXCHANGE_ReloadTest.class.getSimpleName());
      if(!tmpDir.exists())
         tmpDir.mkdir();
   }

   protected void removeTempDirectory() {
      for (File f : tmpDir.listFiles()) {
         f.delete();
      }
      tmpDir.delete();
   }

   @BeforeClass
   public void init() throws Exception {
      createTempDirectory();
      keyStoreFile = new File(tmpDir + File.separator + KEYSTORE);
      trustStoreFile = new File(tmpDir + File.separator + TRUSTSTORE);

      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
      keyPair = keyPairGenerator.generateKeyPair();

      // The CA which will sign all trusted certificates
      KeyStore trustStore = KeyStore.getInstance(KEYSTORE_TYPE);
      trustStore.load(null);
      CA_DN = dn("CA");
      ca = createSelfSignedCertificate(CA_DN, true, "ca");
      trustStore.setCertificateEntry("ca", ca.getSelfSignedCertificate());
      saveKeyStoreToFile(trustStore, null, trustStoreFile);

      // Common keystore on disk for all peers
      KeyStore keyStore = createSignedCertificate(keyPair, ca, CA_DN, "peer", certValidSeconds);
      saveKeyStoreToFile(keyStore, KEY_PASSWORD, keyStoreFile);
   }

   private static void saveKeyStoreToFile(KeyStore ks, String password, File file) throws Exception {
      password = password != null ? password : "";
      FileOutputStream out = new FileOutputStream(file);
      ks.store(out, password.toCharArray());
      out.close();
   }

   @AfterClass
   protected void deinit() throws Exception {
      removeTempDirectory();
   }

   @Test
   public void testReload() throws Exception {
      JChannel a = create("A").connect(cluster_name).setReceiver(new MyReceiver<String>().rawMsgs(true));
      JChannel b = create("B").connect(cluster_name).setReceiver(new MyReceiver<String>().rawMsgs(true));

      // ensure certificate has expired
      Thread.sleep(certValidSeconds * 1000);

      // Renew certificate
      KeyStore keyStore = createSignedCertificate(keyPair, ca, CA_DN, "node", certValidSeconds);
      saveKeyStoreToFile(keyStore, KEY_PASSWORD, keyStoreFile);

      // c will only be able to connect if the coordinator has reloaded its certificate
      JChannel c = create("C").connect(cluster_name).setReceiver(new MyReceiver<String>().rawMsgs(true));
      Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);

      Util.close(c, b, a);
   }

   private JChannel create(String name) throws Exception {
      TCP transport = new TCP();
      transport.setBindAddress(InetAddress.getLoopbackAddress());
      transport.setBindPort(9700);
      TCPPING ping = new TCPPING();
      ping.setInitialHosts2(Collections.singletonList(new IpAddress(transport.getBindAddress(), transport.getBindPort())));
      return new JChannel(
            transport,
            ping,
            new SSL_KEY_EXCHANGE().setKeystoreName(keyStoreFile.getAbsolutePath()).setKeystorePassword(KEY_PASSWORD)
               .setTruststoreName(trustStoreFile.getAbsolutePath()).setTruststorePassword("")
               .setPortRange(10).setReloadThreshold(0),
            new ASYM_ENCRYPT().setUseExternalKeyExchange(true)
              .symKeylength(128).symAlgorithm("AES").symIvLength(0).asymKeylength(512).asymAlgorithm("RSA"),
            new NAKACK2(),
            new UNICAST3(),
            new STABLE(),
            new GMS())
            .name(name);
   }

   static X500Principal dn(String cn) {
      return new X500Principal(String.format(BASE_DN, cn));
   }

   protected SelfSignedX509CertificateAndSigningKey createSelfSignedCertificate(X500Principal dn,
                                                                                boolean isCA, String name) {
      SelfSignedX509CertificateAndSigningKey.Builder certificateBuilder = SelfSignedX509CertificateAndSigningKey.builder()
            .setDn(dn)
            .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
            .setKeyAlgorithmName(KEY_ALGORITHM);

      if (isCA) {
         certificateBuilder.addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647");
      }
      return certificateBuilder.build();
   }

   protected KeyStore createSignedCertificate(KeyPair keyPair,
                                              SelfSignedX509CertificateAndSigningKey ca,
                                              X500Principal issuerDN,
                                              String name,
                                              long validSeconds) {
      try {
         X509Certificate caCertificate = ca.getSelfSignedCertificate();
         X509Certificate certificate = new X509CertificateBuilder()
               .setIssuerDn(issuerDN)
               .setSubjectDn(dn(name))
               .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
               .setSigningKey(ca.getSigningKey())
               .setPublicKey(keyPair.getPublic())
               .setSerialNumber(BigInteger.valueOf(certSerial.getAndIncrement()))
               .addExtension(new BasicConstraintsExtension(false, false, -1))
               .setNotValidAfter(LocalDateTime.now().plusSeconds(validSeconds).atZone(ZoneId.systemDefault()))
               .build();
         return createKeyStore(ks -> {
            try {
               ks.setKeyEntry(name, keyPair.getPrivate(), KEY_PASSWORD.toCharArray(), new X509Certificate[]{certificate, caCertificate});
            } catch (KeyStoreException e) {
               throw new RuntimeException(e);
            }

         });
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static KeyStore createKeyStore(Consumer<KeyStore> consumer) {
      try {
         KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
         keyStore.load(null);
         consumer.accept(keyStore);
         return keyStore;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
