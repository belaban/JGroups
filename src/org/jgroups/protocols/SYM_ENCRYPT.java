package org.jgroups.protocols;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.util.Util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

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
public class SYM_ENCRYPT extends Encrypt {

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




    public void init() throws Exception {
        if(key_password == null && store_password != null) {
            key_password=store_password;
            log.debug("%s: key_password used is same as store_password", local_addr);
        }
        readSecretKeyFromKeystore();
        super.init();
    }

    /**
     * Initialisation if a supplied key is defined in the properties. This supplied key must be in a keystore which
     * can be generated using the keystoreGenerator file in demos. The keystore must be on the classpath to find it.
     */
    protected void readSecretKeyFromKeystore() throws Exception {
        InputStream inputStream=null;
        // must not use default keystore type - as it does not support secret keys
        KeyStore store=KeyStore.getInstance(keystore_type != null? keystore_type : KeyStore.getDefaultType());

        Key tempKey=null;
        try {
            if(this.secret_key == null) { // in case the secret key was set before, e.g. via injection in a unit test
                // load in keystore using this thread's classloader
                inputStream=Thread.currentThread().getContextClassLoader().getResourceAsStream(keystore_name);
                if(inputStream == null)
                    inputStream=new FileInputStream(keystore_name);
                // we can't find a keystore here -
                if(inputStream == null)
                    throw new Exception("Unable to load keystore " + keystore_name + " ensure file is on classpath");
                // we have located a file lets load the keystore
                try {
                    store.load(inputStream, store_password.toCharArray());
                    // loaded keystore - get the key
                    tempKey=store.getKey(alias, key_password.toCharArray());
                }
                catch(IOException e) {
                    throw new Exception("Unable to load keystore " + keystore_name + ": " + e);
                }
                catch(NoSuchAlgorithmException e) {
                    throw new Exception("No Such algorithm " + keystore_name + ": " + e);
                }
                catch(CertificateException e) {
                    throw new Exception("Certificate exception " + keystore_name + ": " + e);
                }

                if(tempKey == null)
                    throw new Exception("Unable to retrieve key '" + alias + "' from keystore " + keystore_name);
                this.secret_key=tempKey;
                if(sym_algorithm.equals(DEFAULT_SYM_ALGO))
                    sym_algorithm=tempKey.getAlgorithm();
            }
        }
        finally {
            Util.close(inputStream);
        }
    }


}
