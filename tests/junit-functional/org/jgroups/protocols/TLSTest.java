package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.TLSHelper;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import java.net.InetAddress;
import java.security.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = {Global.FUNCTIONAL, Global.ENCRYPT}, singleThreaded = true)
public class TLSTest {
   public static final String PROTOCOL = "TLSv1.2";
   public static final String BASE_DN = "CN=%s,OU=JGroups,O=JBoss,L=Red Hat";
   public static final String KEY_PASSWORD = "secret";
   public static final String KEY_ALGORITHM = "RSA";
   public static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";
   public static final String KEYSTORE_TYPE = "pkcs12";
   Map<String, KeyStore>      keyStores = new HashMap<>();
   Map<String, SSLContext>    sslContexts = new HashMap<>();

   JChannel a, b, c;
   MyReceiver<String> ra, rb, rc;
   final String cluster_name = getClass().getSimpleName();

   @BeforeClass
   public void init() throws Exception {
      KeyPair keyPair = TLSHelper.generateKeyPair(KEY_ALGORITHM);
      PrivateKey signingKey = keyPair.getPrivate();
      PublicKey publicKey = keyPair.getPublic();

      // The CA which will sign all trusted certificates
      X500Principal CA_DN = dn("CA");
      SelfSignedX509CertificateAndSigningKey ca = TLSHelper.createSelfSignedCertificate(CA_DN, true,
                                                                                        KEY_SIGNATURE_ALGORITHM,
                                                                                        KEY_ALGORITHM);
      // The truststore which contains all of the certificates
      KeyStore trustStore = TLSHelper.createKeyStore(KEYSTORE_TYPE);
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustStore.setCertificateEntry("ca", ca.getSelfSignedCertificate());
      trustManagerFactory.init(trustStore);

      // One certificate per legitimate node, signed by the CA
      for (String name : Arrays.asList("A", "B", "C")) {
         keyStores.put(name, TLSHelper.createCertAndAddToKeystore(signingKey, publicKey, ca, CA_DN, name, trustStore,
                                                            KEYSTORE_TYPE, KEY_PASSWORD));
      }
      // A self-signed certificate which has the same DN as the CA, but which is a rogue
      SelfSignedX509CertificateAndSigningKey other = TLSHelper.createSelfSignedCertificate(CA_DN, true,
                                                                                     KEY_SIGNATURE_ALGORITHM, KEY_ALGORITHM);
      try {
         KeyStore ks=TLSHelper.createKeyStore(KEYSTORE_TYPE);
         ks.setCertificateEntry("O", other.getSelfSignedCertificate());
         keyStores.put("O", ks);
      } catch (KeyStoreException e) {
         throw new RuntimeException(e);
      }


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
      Util.waitUntilTrue(5000, 500, () -> a.getView().size() > 3);
      Arrays.asList(a, b, c).forEach(ch -> {
         View view = ch.getView();
         assertEquals(3, view.size());
         assertTrue(view.containsMembers(a.getAddress(), b.getAddress(), c.getAddress()));
      });
   }

   private JChannel create(String name) throws Exception {
      TCP tp=new TCP().setBindAddress(InetAddress.getLoopbackAddress()).setBindPort(9600);
      tp.setSocketFactory(sslContexts.containsKey(name) ? new DefaultSocketFactory(sslContexts.get(name)) : new DefaultSocketFactory());
      TCPPING ping = new TCPPING()
        .setInitialHosts2(Collections.singletonList(new IpAddress(tp.getBindAddress(), tp.getBindPort())));
      return new JChannel(
            tp,
            ping,
            new NAKACK2(),
            new UNICAST3(),
            new STABLE(),
            new GMS())
            .name(name);
   }

   protected static X500Principal dn(String cn) {
      return new X500Principal(String.format(BASE_DN, cn));
   }

}
