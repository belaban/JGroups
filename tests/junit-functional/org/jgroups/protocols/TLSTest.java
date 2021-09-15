package org.jgroups.protocols;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.math.BigInteger;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

@Test(groups = {Global.FUNCTIONAL, Global.ENCRYPT}, singleThreaded = true)
public class TLSTest {
   public static final String PROTOCOL = "TLSv1.2";
   public static final String BASE_DN = "CN=%s,OU=JGroups,O=JBoss,L=Red Hat";
   public static final String KEY_PASSWORD = "secret";
   public static final String KEY_ALGORITHM = "RSA";
   public static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";
   public static final String KEYSTORE_TYPE = "pkcs12";
   private final AtomicLong certSerial = new AtomicLong(1);
   Map<String, KeyStore> keyStores = new HashMap<>();
   Map<String, SSLContext> sslContexts = new HashMap<>();

   JChannel a, b, c;
   MyReceiver<String> ra, rb, rc;
   final String cluster_name = getClass().getSimpleName();

   @BeforeClass
   public void init() throws Exception {
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
      KeyPair keyPair = keyPairGenerator.generateKeyPair();
      PrivateKey signingKey = keyPair.getPrivate();
      PublicKey publicKey = keyPair.getPublic();

      // The truststore which contains all of the certificates
      KeyStore trustStore = KeyStore.getInstance(KEYSTORE_TYPE);
      trustStore.load(null);

      // The CA which will sign all trusted certificates
      X500Principal CA_DN = dn("CA");
      SelfSignedX509CertificateAndSigningKey ca = createSelfSignedCertificate(CA_DN, true, "ca");
      trustStore.setCertificateEntry("ca", ca.getSelfSignedCertificate());

      // One certificate per legitimate node, signed by the CA
      for (String n : Arrays.asList("A", "B", "C")) {
         keyStores.put(n, createSignedCertificate(signingKey, publicKey, ca, CA_DN, n, trustStore));
      }
      // A self-signed certificate which has the same DN as the CA, but which is a rogue
      SelfSignedX509CertificateAndSigningKey other = createSelfSignedCertificate(CA_DN, true, "other");
      keyStores.put("O", createKeyStore(ks -> {
         try {
            ks.setCertificateEntry("O", other.getSelfSignedCertificate());
         } catch (KeyStoreException e) {
            throw new RuntimeException(e);
         }
      }));

      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(trustStore);

      for (Map.Entry<String, KeyStore> keyStore : keyStores.entrySet()) {
         SSLContext sslContext = SSLContext.getInstance(PROTOCOL);

         KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
         keyManagerFactory.init(keyStore.getValue(), KEY_PASSWORD.toCharArray());
         String name = keyStore.getKey();
         sslContext.init(keyManagerFactory.getKeyManagers(), name.charAt(0) < 'D' ? trustManagerFactory.getTrustManagers() : null, null);
         sslContexts.put(name, sslContext);
      }
   }

   @Test
   public void testTLS() throws Exception {
      a = create("A").connect(cluster_name).setReceiver(ra = new MyReceiver<String>().rawMsgs(true));
      b = create("B").connect(cluster_name).setReceiver(rb = new MyReceiver<String>().rawMsgs(true));
      c = create("C").connect(cluster_name).setReceiver(rc = new MyReceiver<String>().rawMsgs(true));
      Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);
      verifyForbiddenJoiner("U");
      verifyForbiddenJoiner("O");
      Util.close(c, b, a);
   }

   private void verifyForbiddenJoiner(String name) throws Exception {
      JChannel channel = create(name);
      GMS gms = channel.getProtocolStack().findProtocol(GMS.class);
      gms.setMaxJoinAttempts(1);
      try {
         channel.connect(cluster_name);
      } catch (Exception ex) {
      }
      for (int i = 0; i < 10; i++) {
         if (a.getView().size() > 3)
            break;
         Util.sleep(500);
      }
      Arrays.asList(a, b, c).forEach(ch -> {
         View view = ch.getView();
         assertEquals(3, view.size());
         assertTrue(view.containsMembers(a.getAddress(), b.getAddress(), c.getAddress()));
      });
   }

   private JChannel create(String name) throws Exception {
      TCP transport = new TCP();
      transport.setBindAddress(InetAddress.getLoopbackAddress());
      transport.setBindPort(9600);
      transport.setSocketFactory(sslContexts.containsKey(name) ? new DefaultSocketFactory(sslContexts.get(name)) : new DefaultSocketFactory());
      TCPPING ping = new TCPPING();
      ping.setInitialHosts2(Collections.singletonList(new IpAddress(transport.getBindAddress(), transport.getBindPort())));
      return new JChannel(
            transport,
            ping,
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

   protected KeyStore createSignedCertificate(PrivateKey signingKey, PublicKey publicKey,
                                              SelfSignedX509CertificateAndSigningKey ca,
                                              X500Principal issuerDN,
                                              String name, KeyStore trustStore) {
      try {
         X509Certificate caCertificate = ca.getSelfSignedCertificate();
         X509Certificate certificate = new X509CertificateBuilder()
               .setIssuerDn(issuerDN)
               .setSubjectDn(dn(name))
               .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
               .setSigningKey(ca.getSigningKey())
               .setPublicKey(publicKey)
               .setSerialNumber(BigInteger.valueOf(certSerial.getAndIncrement()))
               .addExtension(new BasicConstraintsExtension(false, false, -1))
               .build();
         trustStore.setCertificateEntry(name, certificate);
         return createKeyStore(ks -> {
            try {
               ks.setCertificateEntry("ca", caCertificate);
               ks.setKeyEntry(name, signingKey, KEY_PASSWORD.toCharArray(), new X509Certificate[]{certificate, caCertificate});
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
