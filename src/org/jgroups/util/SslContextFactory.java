package org.jgroups.util;

import javax.net.ssl.*;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SslContextFactory.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslContextFactory {
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
   private FileWatcher watcher;

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

   public SslContextFactory watcher(FileWatcher watcher) {
      this.watcher = watcher;
      return this;
   }

   public Context build() {
      try {
         KeyManager[] kms = getKeyManagers();
         TrustManager[] tms = getTrustManagers();
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
         sslContext.init(kms, tms, null);
         return new Context(sslContext, kms != null ? kms[0] : null, tms != null ? tms[0] : null);
      } catch (Exception e) {
         throw new RuntimeException("Could not initialize SSL", e);
      }
   }

   public void initializeContext(SSLContext sslContext) {
      try {
         KeyManager[] kms = getKeyManagers();
         TrustManager[] tms = getTrustManagers();
         sslContext.init(kms, tms, null);
      } catch (Exception e) {
         throw new RuntimeException("Could not initialize SSL", e);
      }
   }

   private KeyManager[] getKeyManagers() {
      if (keyStoreFileName == null && keyStore == null)
         return null;

      if (keyStoreFileName == null || watcher == null)
         return new KeyManager[]{getKeyManager()};

      return new KeyManager[]{new ReloadingX509KeyManager(watcher, Path.of(keyStoreFileName), p -> getKeyManager())};
   }

   private TrustManager[] getTrustManagers() {
      if (trustStoreFileName == null && trustStore == null)
         return null;

      if (trustStoreFileName == null || watcher == null)
         return new TrustManager[]{getTrustManager()};

      return new TrustManager[]{new ReloadingX509TrustManager(watcher, Path.of(trustStoreFileName), p -> getTrustManager())};
   }

   private X509ExtendedKeyManager getKeyManager() {
      try {
         Provider provider;
         KeyStore ks = keyStore != null ? keyStore : null;
         if (ks == null) {
            String type = keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE;
            provider = findProvider(this.providerName, KeyStore.class.getSimpleName(), type);
            ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
            loadKeyStore(ks, keyStoreFileName, keyStorePassword, classLoader);
         } else {
            provider = keyStore.getProvider();
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
               throw new RuntimeException(String.format("The alias '%s' does not exist in the key store '%s'", keyAlias, keyStoreFileName));
            }
         }
         String algorithm = KeyManagerFactory.getDefaultAlgorithm();
         provider = findProvider(this.providerName, KeyManagerFactory.class.getSimpleName(), algorithm);
         KeyManagerFactory kmf = provider != null ? KeyManagerFactory.getInstance(algorithm, provider) : KeyManagerFactory.getInstance(algorithm);
         kmf.init(ks, keyStorePassword);
         for (KeyManager km : kmf.getKeyManagers()) {
            if (km instanceof X509ExtendedKeyManager) {
               return (X509ExtendedKeyManager) km;
            }
         }
         throw new GeneralSecurityException("Could not obtain an X509ExtendedKeyManager");
      } catch (GeneralSecurityException | IOException e) {
         throw new RuntimeException("Error while initializing SSL context", e);
      }
   }

   private X509ExtendedTrustManager getTrustManager() {
      try {
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
         for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509ExtendedTrustManager) {
               return (X509ExtendedTrustManager) tm;
            }
         }
         throw new GeneralSecurityException("Could not obtain an X509TrustManager");
      } catch (GeneralSecurityException | IOException e) {
         throw new RuntimeException("Error while initializing SSL context", e);
      }
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

   public record Context(SSLContext sslContext, KeyManager keyManager, TrustManager trustManager) {
   }
}
