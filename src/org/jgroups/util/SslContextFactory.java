package org.jgroups.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

/**
 * SslContextFactory.
 *
 * @author Tristan Tarrant
 */
public class SslContextFactory {
   private static final Log log = LogFactory.getLog(SslContextFactory.class);
   private static final String DEFAULT_KEYSTORE_TYPE = "JKS";
   private static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";
   private static final String CLASSPATH_RESOURCE = "classpath:";
   private static final String DEFAULT_SSL_PROVIDER;

   static {
      String sslProvider = null;
      try {
         Class<?> openSslProvider = Util.loadClass("org.wildfly.openssl.OpenSSLProvider", SslContextFactory.class);
         Class<?> openSsl = Util.loadClass("org.wildfly.openssl.SSL", SslContextFactory.class);
         if (openSslProvider != null && openSsl != null) {
            openSslProvider.getMethod("register").invoke(null);
            openSsl.getMethod("getInstance").invoke(null);
            sslProvider = "openssl";
            log.debug("Using OpenSSL");
         }
      } catch (Throwable e) {
      }
      DEFAULT_SSL_PROVIDER = sslProvider;
   }

   private KeyStore keyStore;
   private String keyStoreFileName;
   private char[] keyStorePassword;
   private char[] keyStoreCertificatePassword;
   private String keyStoreType = DEFAULT_KEYSTORE_TYPE;
   private String keyAlias;
   private KeyStore trustStore;
   private String trustStoreFileName;
   private char[] trustStorePassword;
   private String trustStoreType = DEFAULT_KEYSTORE_TYPE;
   private String sslProtocol = DEFAULT_SSL_PROTOCOL;
   private String sslProvider = DEFAULT_SSL_PROVIDER;
   private boolean useNativeIfAvailable = true;
   private ClassLoader classLoader;

   public SslContextFactory() {
   }

   public SslContextFactory keyStore(KeyStore keyStore) {
      this.keyStore = keyStore;
      return this;
   }

   public SslContextFactory keyStoreFileName(String keyStoreFileName) {
      this.keyStoreFileName = keyStoreFileName;
      return this;
   }

   public SslContextFactory keyStorePassword(String keyStorePassword) {
      if (keyStorePassword != null) {
         this.keyStorePassword = keyStorePassword.toCharArray();
      }
      return this;
   }

   public SslContextFactory keyStorePassword(char[] keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
   }

   public SslContextFactory keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      return this;
   }

   public SslContextFactory keyStoreCertificatePassword(String keyStoreCertificatePassword) {
      if (keyStoreCertificatePassword != null) {
         this.keyStoreCertificatePassword = keyStoreCertificatePassword.toCharArray();
      }
      return this;
   }

   public SslContextFactory keyStoreType(String keyStoreType) {
      if (keyStoreType != null) {
         this.keyStoreType = keyStoreType;
      }
      return this;
   }

   public SslContextFactory keyAlias(String keyAlias) {
      this.keyAlias = keyAlias;
      return this;
   }

   public SslContextFactory trustStore(KeyStore trustStore) {
      this.trustStore = trustStore;
      return this;
   }

   public SslContextFactory trustStoreFileName(String trustStoreFileName) {
      this.trustStoreFileName = trustStoreFileName;
      return this;
   }

   public SslContextFactory trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
   }

   public SslContextFactory trustStorePassword(String trustStorePassword) {
      if (trustStorePassword != null) {
         this.trustStorePassword = trustStorePassword.toCharArray();
      }
      return this;
   }

   public SslContextFactory trustStoreType(String trustStoreType) {
      if (trustStoreType != null) {
         this.trustStoreType = trustStoreType;
      }
      return this;
   }

   public SslContextFactory sslProtocol(String sslProtocol) {
      if (sslProtocol != null) {
         this.sslProtocol = sslProtocol;
      }
      return this;
   }

   public SslContextFactory sslProvider(String sslProvider) {
      if (sslProvider != null) {
         this.sslProvider = sslProvider;
      }
      return this;
   }

   public SslContextFactory classLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
   }

   public SSLContext getContext() {
      try {
         KeyManager[] keyManagers = null;
         if (keyStoreFileName != null || keyStore != null) {
            KeyManagerFactory kmf = getKeyManagerFactory();
            keyManagers = kmf.getKeyManagers();
         }
         TrustManager[] trustManagers = null;
         if (trustStoreFileName != null || trustStore != null) {
            TrustManagerFactory tmf = getTrustManagerFactory();
            trustManagers = tmf.getTrustManagers();
         }
         SSLContext sslContext;
         if (sslProvider != null) {
            sslContext = SSLContext.getInstance(sslProtocol, sslProvider);
         } else {
            sslContext = SSLContext.getInstance(sslProtocol);
         }
         sslContext.init(keyManagers, trustManagers, null);
         return sslContext;
      } catch (Exception e) {
         throw new RuntimeException("Could not initialize SSL", e);
      }
   }

   public KeyManagerFactory getKeyManagerFactory() throws IOException, GeneralSecurityException {
      if (keyStore == null) {
         keyStore = KeyStore.getInstance(keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE);
         loadKeyStore(keyStore, keyStoreFileName, keyStorePassword, classLoader);
      }
      char[] keyPassword = keyStoreCertificatePassword == null ? keyStorePassword : keyStoreCertificatePassword;
      if (keyAlias != null) {
         if (keyStore.containsAlias(keyAlias) && keyStore.isKeyEntry(keyAlias)) {
            KeyStore.PasswordProtection passParam = new KeyStore.PasswordProtection(keyPassword);
            KeyStore.Entry entry = keyStore.getEntry(keyAlias, passParam);
            // Recreate the keystore with just one key
            keyStore = KeyStore.getInstance(keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE);
            keyStore.load(null);
            keyStore.setEntry(keyAlias, entry, passParam);
         } else {
            throw new RuntimeException("No alias '" + keyAlias +"' in key store '" + keyStoreFileName+ "'");
         }
      }
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, keyPassword);
      return kmf;
   }

   public TrustManagerFactory getTrustManagerFactory() throws IOException, GeneralSecurityException {
      if (trustStore == null) {
         trustStore = KeyStore.getInstance(trustStoreType != null ? trustStoreType : DEFAULT_KEYSTORE_TYPE);
         loadKeyStore(trustStore, trustStoreFileName, trustStorePassword, classLoader);
      }
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      return tmf;
   }

   public static String getSslProvider() {
      return DEFAULT_SSL_PROVIDER;
   }

   public static String getDefaultSslProtocol() {
      return DEFAULT_SSL_PROTOCOL;
   }

   public static SSLEngine getEngine(SSLContext sslContext, boolean useClientMode, boolean needClientAuth) {
      SSLEngine sslEngine = sslContext.createSSLEngine();
      sslEngine.setUseClientMode(useClientMode);
      sslEngine.setNeedClientAuth(needClientAuth);
      return sslEngine;
   }

   private static void loadKeyStore(KeyStore ks, String keyStoreFileName, char[] keyStorePassword, ClassLoader classLoader) throws IOException, GeneralSecurityException {
      InputStream is = null;
      try {
         if (keyStoreFileName.startsWith(CLASSPATH_RESOURCE)) {
            String fileName = keyStoreFileName.substring(keyStoreFileName.indexOf(":") + 1);
            is = Util.getResourceAsStream(fileName, classLoader);
            if (is == null) {
               throw new IllegalArgumentException("Cannot find `" + keyStoreFileName +"`");
            }
         } else if (Files.exists(Paths.get(keyStoreFileName))) {
            is = new BufferedInputStream(new FileInputStream(keyStoreFileName));
         } else {
            is = Util.getResourceAsStream(keyStoreFileName, classLoader);
         }
         ks.load(is, keyStorePassword);
      } finally {
         Util.close(is);
      }
   }
}
