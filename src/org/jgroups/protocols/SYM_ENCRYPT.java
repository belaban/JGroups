package org.jgroups.protocols;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;

import javax.crypto.SecretKey;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyStore;

/**
 * Encrypts and decrypts communication in JGroups by using a secret key shared by all cluster members.<p>
 *
 * The secret key is identical for all cluster members and is injected into this protocol at startup, e.g. by reading
 * it from a keystore. Messages are sent by encrypting them with the secret key and received by decrypting them with
 * the secret key. Note that all cluster members must be shipped with the same keystore file<p>
 *
 * This protocol is typically placed under {@link org.jgroups.protocols.pbcast.NAKACK2}, so that most important
 * headers are encrypted as well, to prevent replay attacks.<p>
 *
 * A possible configuration looks like this:<br><br>
 * {@code <SYM_ENCRYPT key_store_name="defaultStore.keystore" store_password="changeit" alias="myKey"/>}
 * <br>
 * <br>
 * In order to use SYM_ENCRYPT layer in this manner, it is necessary to have the secret key already generated in a
 * keystore file. The directory containing the keystore file must be on the application's classpath. You cannot create a
 * secret key keystore file using the keytool application shipped with the JDK. A java file called KeyStoreGenerator is
 * included in the demo package that can be used from the command line (or IDE) to generate a suitable keystore.
 *
 * @author Bela Ban
 * @author Steve Woodcock
 */
@MBean(description="Symmetric encryption protocol. The (shared) shared secret key is configured up front, " +
  "e.g. via a key store, or injection")
public class SYM_ENCRYPT extends Encrypt<KeyStore.SecretKeyEntry> {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    @Property(description="File on classpath that contains keystore repository")
    protected String   keystore_name;

    @Property(description="The type of the keystore. " +
      "Types are listed in http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html")
    protected String   keystore_type="JCEKS";

    @Property(description="Password used to check the integrity/unlock the keystore. Change the default",
      exposeAsManagedAttribute=false)
    protected String   store_password="changeit"; // JDK default

    @Property(description="Password for recovering the key. Change the default", exposeAsManagedAttribute=false)
    protected String   key_password; // allows to assign keypwd=storepwd if not set (https://issues.jboss.org/browse/JGRP-1375)


    @Property(name="alias", description="Alias used for recovering the key. Change the default",exposeAsManagedAttribute=false)
    protected String   alias="mykey"; // JDK default


    public String      keystoreName()                      {return this.keystore_name;}
    public SYM_ENCRYPT keystoreName(String n)              {this.keystore_name=n; return this;}
    public String      alias()                             {return alias;}
    public SYM_ENCRYPT alias(String a)                     {this.alias=a; return this;}
    public String      storePassword()                     {return store_password;}
    public SYM_ENCRYPT storePassword(String pwd)           {this.store_password=pwd; return this;}

    @Override
    public void setKeyStoreEntry(KeyStore.SecretKeyEntry entry) {
        this.setSecretKey(entry.getSecretKey());
    }

    public void setSecretKey(SecretKey key) {
        String key_algorithm = key.getAlgorithm();
        if (sym_algorithm == null)
            sym_algorithm = key_algorithm;
        else if (!getAlgorithm(sym_algorithm).equals(key_algorithm)) {
            // avoid overwriting the sym_algorithm transformation in case it includes a mode and padding
            if (getModeAndPadding(sym_algorithm) != null) {
                log.warn("%s: replacing sym_algorithm %s with key algorithm %s", local_addr, sym_algorithm, key_algorithm);
            }
            this.sym_algorithm = key_algorithm;
        }
        this.secret_key = key;
    }

    @Override
    public void init() throws Exception {
        if (this.secret_key == null) {
            readSecretKeyFromKeystore();
        }
        super.init();
    }

    /**
     * Initialisation if a supplied key is defined in the properties. This supplied key must be in a keystore which
     * can be generated using the keystoreGenerator file in demos. The keystore must be on the classpath to find it.
     */
    protected void readSecretKeyFromKeystore() throws Exception {
        // must not use default keystore type - as it does not support secret keys
        KeyStore store=KeyStore.getInstance(keystore_type != null? keystore_type : KeyStore.getDefaultType());

        if(key_password == null && store_password != null) {
            key_password=store_password;
            log.debug("%s: key_password used is same as store_password", local_addr);
        }

        try (InputStream inputStream = getKeyStoreSource()) {
            store.load(inputStream, store_password.toCharArray());
        }

        // loaded keystore - get the key
        if (!store.entryInstanceOf(alias, KeyStore.SecretKeyEntry.class)) {
            throw new Exception("Key '" + alias + "' from keystore " + keystore_name + " is not a secret key");
        }
        KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) store.getEntry(alias, new KeyStore.PasswordProtection(key_password.toCharArray()));
        if (entry == null) {
            throw new Exception("Key '" + alias + "' not found in keystore " + keystore_name);
        }

        this.setKeyStoreEntry(entry);
    }

    protected InputStream getKeyStoreSource() throws FileNotFoundException {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(keystore_name);
        return (inputStream == null) ? new FileInputStream(keystore_name) : inputStream;
    }
}
