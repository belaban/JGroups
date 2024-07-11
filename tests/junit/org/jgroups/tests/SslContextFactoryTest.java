package org.jgroups.tests;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.security.auth.x500.X500Principal;

import org.jgroups.Global;
import org.jgroups.util.FileWatcher;
import org.jgroups.util.ReloadingX509KeyManager;
import org.jgroups.util.ReloadingX509TrustManager;
import org.jgroups.util.SslContextFactory;
import org.jgroups.util.TLS;
import org.testng.annotations.Test;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;

@Test(groups= Global.STACK_INDEPENDENT,singleThreaded=true)
public class SslContextFactoryTest {
   public static final String KEY_ALGORITHM = "RSA";
   public static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";
   public static final String SECRET = "secret";

   public void testSslContextFactoryWatch() throws IOException {
      try (FileWatcher watcher = new FileWatcher()) {
         Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"), GossipRouterTest.class.getSimpleName());
         Files.createDirectories(tmpDir);
         Path keystore = createCertificateKeyStore("keystore", SECRET, tmpDir);
         Path truststore = createCertificateKeyStore("truststore", SECRET, tmpDir);

         TLS tls = new TLS();
         tls.enabled(true)
               .setKeystorePassword(SECRET)
               .setKeystorePath(keystore.toString())
               .setTruststorePath(truststore.toString())
               .setTruststorePassword(SECRET)
               .setWatcher(watcher);

         SslContextFactory.Context context = new SslContextFactory()
               .keyStoreFileName(keystore.toString())
               .keyStorePassword(SECRET.toCharArray())
               .trustStoreFileName(truststore.toString())
               .trustStorePassword(SECRET.toCharArray())
               .watcher(watcher)
               .build();

         // Verify that building an SSLEngine works
         context.sslContext().createSSLEngine();

         // Recreate the keystore
         Instant kmLastLoaded = ((ReloadingX509KeyManager) context.keyManager()).lastLoaded();
         createCertificateKeyStore("keystore", SECRET, tmpDir);
         eventually(() -> ((ReloadingX509KeyManager) context.keyManager()).lastLoaded().isAfter(kmLastLoaded));

         // Recreate the truststore
         Instant tmlastLoaded = ((ReloadingX509TrustManager) context.trustManager()).lastLoaded();
         createCertificateKeyStore("truststore", SECRET, tmpDir);
         eventually(() -> ((ReloadingX509TrustManager) context.trustManager()).lastLoaded().isAfter(tmlastLoaded));

         // Verify that building an SSLEngine works
         context.sslContext().createSSLEngine();
      }
   }

   private Path createCertificateKeyStore(String name, String secret, Path dir) {
      SelfSignedX509CertificateAndSigningKey.Builder certificateBuilder = SelfSignedX509CertificateAndSigningKey.builder()
            .setDn(new X500Principal("CN=" + name))
            .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
            .setKeyAlgorithmName(KEY_ALGORITHM);
      SelfSignedX509CertificateAndSigningKey certificate = certificateBuilder.build();
      Path file = dir.resolve(name + ".pfx");
      try (OutputStream os = Files.newOutputStream(file)) {
         KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
         keyStore.load(null, null);
         keyStore.setCertificateEntry(name, certificate.getSelfSignedCertificate());
         keyStore.store(os, secret.toCharArray());
         return file;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private void eventually(Supplier<Boolean> ec) {
      eventually(AssertionError::new, ec, 10000, 100, MILLISECONDS);
   }

   private void eventually(Supplier<AssertionError> assertionErrorSupplier, Supplier<Boolean> ec, long timeout,
                           long pollInterval, TimeUnit unit) {
      if (pollInterval <= 0) {
         throw new IllegalArgumentException("Check interval must be positive");
      }
      try {
         long expectedEndTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
         long sleepMillis = MILLISECONDS.convert(pollInterval, unit);
         do {
            if (ec.get()) return;

            Thread.sleep(sleepMillis);
         } while (expectedEndTime - System.nanoTime() > 0);

         throw assertionErrorSupplier.get();
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }
}
