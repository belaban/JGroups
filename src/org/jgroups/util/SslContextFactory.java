package org.jgroups.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

/**
 * SslContextFactory.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslContextFactory {
   private static final Log log = LogFactory.getLog(SslContextFactory.class);
   private static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";
   public static final String DEFAULT_SSL_PROTOCOL = "TLS";
   private static final String CLASSPATH_RESOURCE = "classpath:";
   private static final ConcurrentHashMap<ClassLoader, Provider[]> PER_CLASSLOADER_PROVIDERS = new ConcurrentHashMap<>(2);
   private KeyStore keyStore;
   private String keyStoreFileName;
   private char[] keyStorePassword;
   private String keyStoreType = DEFAULT_KEYSTORE_TYPE;
   private String keyAlias;
   private KeyStore trustStore;
   private String trustStoreFileName;
   private char[] trustStorePassword;
   private String trustStoreType = DEFAULT_KEYSTORE_TYPE;
   private String sslProtocol = DEFAULT_SSL_PROTOCOL;
   private ClassLoader classLoader;
   private String providerName;

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

   public SslContextFactory trustStorePassword(String trustStorePassword) {
      return trustStorePassword(trustStorePassword != null ? trustStorePassword.toCharArray() : null);
   }

   public SslContextFactory trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
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

   public SslContextFactory provider(String provider) {
      if (provider != null) {
         this.providerName = provider;
      }
      return this;
   }

   public SslContextFactory classLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
   }

   public SSLContext getContext() {
      try {
         SSLContext sslContext;
         if (providerName != null) {
            Provider provider = findProvider(providerName, SSLContext.class.getSimpleName(), sslProtocol);
            if (provider == null) {
               throw new IllegalArgumentException("No such provider " + providerName);
            }
            sslContext = SSLContext.getInstance(sslProtocol, provider);
         } else {
            sslContext = SSLContext.getInstance(sslProtocol);
         }
         initializeContext(sslContext);
         return sslContext;
      } catch (Exception e) {
         throw new RuntimeException("Could not initialize SSL", e);
      }
   }

   public void initializeContext(SSLContext sslContext) {
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
         sslContext.init(keyManagers, trustManagers, null);
      } catch (Exception e) {
         throw new RuntimeException("Could not initialize SSL", e);
      }
   }

   public KeyManagerFactory getKeyManagerFactory() throws IOException, GeneralSecurityException {
      Provider provider;
      KeyStore ks = keyStore != null ? keyStore : null;
      if (ks == null) {
         String type = keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE;
         provider = findProvider(this.providerName, KeyStore.class.getSimpleName(), type);
         ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
         loadKeyStore(ks, keyStoreFileName, keyStorePassword, classLoader);
      } else {
         provider = ks.getProvider();
      }
      if (keyAlias != null) {
         if (ks.containsAlias(keyAlias) && ks.isKeyEntry(keyAlias)) {
            KeyStore.PasswordProtection passParam = new KeyStore.PasswordProtection(keyStorePassword);
            KeyStore.Entry entry = ks.getEntry(keyAlias, passParam);
            // Recreate the keystore with just one key
            ks = provider != null ? KeyStore.getInstance(keyStoreType, provider) : KeyStore.getInstance(keyStoreType);
            ks.load(null, null);
            ks.setEntry(keyAlias, entry, passParam);
         } else {
            throw new RuntimeException("No alias '" + keyAlias + "' in key store '" + keyStoreFileName + "'");
         }
      }
      String algorithm = KeyManagerFactory.getDefaultAlgorithm();
      provider = findProvider(this.providerName, KeyManagerFactory.class.getSimpleName(), algorithm);
      KeyManagerFactory kmf = provider != null ? KeyManagerFactory.getInstance(algorithm, provider) : KeyManagerFactory.getInstance(algorithm);
      kmf.init(ks, keyStorePassword);
      return kmf;
   }

   public TrustManagerFactory getTrustManagerFactory() throws IOException, GeneralSecurityException {
      Provider provider;
      KeyStore ts = trustStore != null ? trustStore : null;
      if (ts == null) {
         String type = trustStoreType != null ? trustStoreType : DEFAULT_KEYSTORE_TYPE;
         provider = findProvider(this.providerName, KeyStore.class.getSimpleName(), type);
         ts = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
         loadKeyStore(ts, trustStoreFileName, trustStorePassword, classLoader);
      }
      String algorithm = KeyManagerFactory.getDefaultAlgorithm();
      provider = findProvider(this.providerName, TrustManagerFactory.class.getSimpleName(), algorithm);
      TrustManagerFactory tmf = provider != null ? TrustManagerFactory.getInstance(algorithm, provider) : TrustManagerFactory.getInstance(algorithm);
      tmf.init(ts);
      return tmf;
   }

   private static void loadKeyStore(KeyStore ks, String keyStoreFileName, char[] keyStorePassword, ClassLoader classLoader) throws IOException, GeneralSecurityException {
      InputStream is = null;
      try {
         if (keyStoreFileName.startsWith(CLASSPATH_RESOURCE)) {
            String fileName = keyStoreFileName.substring(keyStoreFileName.indexOf(":") + 1);
            is = Util.getResourceAsStream(fileName, classLoader);
            if (is == null) {
               throw new IllegalArgumentException("Cannot find `" + keyStoreFileName + "`");
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

   public static Provider findProvider(String providerName, String serviceType, String algorithm) {
      Provider[] providers = discoverSecurityProviders(Thread.currentThread().getContextClassLoader());
      for (Provider provider : providers) {
         if (providerName == null || providerName.equals(provider.getName())) {
            Provider.Service providerService = provider.getService(serviceType, algorithm);
            if (providerService != null) {
               return provider;
            }
         }
      }
      return null;
   }

   public static Provider[] discoverSecurityProviders(ClassLoader classLoader) {
      return PER_CLASSLOADER_PROVIDERS.computeIfAbsent(classLoader, cl -> {
               // We need to keep them sorted by insertion order, since we want system providers first
               Map<Class<? extends Provider>, Provider> providers = new LinkedHashMap<>();
               for (Provider provider : Security.getProviders()) {
                  providers.put(provider.getClass(), provider);
               }
               Iterator<Provider> loader = ServiceLoader.load(Provider.class, classLoader).iterator();
               for (; ; ) {
                  try {
                     if (!loader.hasNext()) {
                        return providers.values().toArray(new Provider[0]);
                     } else {
                        Provider provider = loader.next();
                        providers.putIfAbsent(provider.getClass(), provider);
                     }
                  } catch (ServiceConfigurationError ignored) {
                     // explicitly ignored
                  }
               }
            }
      );
   }
}
