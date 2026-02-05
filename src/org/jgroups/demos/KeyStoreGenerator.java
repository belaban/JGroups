
package org.jgroups.demos;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;

/**
 * Generates a keystore file that has a SecretKey in it. It is not possible to
 * use keytool to achieve this. This is a simple way to generate a
 * JCEKS format keystore and SecretKey.
 * <br/>
 * Usage is --alg ALGNAME --size ALGSIZE --storeName FILENAME --storePass
 * PASSWORD --alias KEYALIAS
 * <br/>
 * Any of args are optional and will default to
 * <ul>
 * <li>ALGNAME = Blowfish
 * <li>ALGSIZE = 56
 * <li>FILENAME = defaultStore.keystore
 * <li>PASSWORD = changeit
 * <li>ALIAS = mykey
 * </ul>
 * 
 * @author S Woodcock
 * 
 */
public final class KeyStoreGenerator {
    private static final Log log = LogFactory.getLog(KeyStoreGenerator.class);

    public static final String  DEFAULT_SYM_ALGORITHM="AES";
    public static final Integer DEFAULT_KEY_SIZE=128;
    public static final String  DEFAULT_KEY_STORE_NAME="defaultStore.keystore";
    public static final String  DEFAULT_KEY_STORE_PASS="changeit";
    public static final String  DEFAULT_KEY_ALIAS="myKey";
    public static final String  DEFAULT_KEY_STORE_TYPE="pkcs12";
    
    private String symAlg=DEFAULT_SYM_ALGORITHM;
    private int keySize=DEFAULT_KEY_SIZE;
    private String keyStoreName=DEFAULT_KEY_STORE_NAME;
    private String storePass=DEFAULT_KEY_STORE_PASS;
    private String alias=DEFAULT_KEY_ALIAS;
    private String storeType=DEFAULT_KEY_STORE_TYPE;

    public KeyStoreGenerator() {
    }

    public KeyStoreGenerator(KeyStoreGenerator other) {
        this.symAlg=other.symAlg;
        this.keySize=other.keySize;
        this.keyStoreName=other.keyStoreName;
        this.storePass=other.storePass;
        this.alias=other.alias;
        this.storeType=other.storeType;
    }

    public static void main (String[] args) {
        KeyStoreGeneratorBuilder keystoreGenerator = newKeyStoreGeneratorBuilder();
        int i=0;
        String arg=null;

        while(i < args.length && args[i].startsWith("-")) {
            arg=args[i++];
            if(arg.equalsIgnoreCase("--alg")) {
                keystoreGenerator.symAlg(args[i++]);
                continue;
            }
            if(arg.equalsIgnoreCase("--size")) {
                keystoreGenerator.keySize(Integer.parseInt(args[i++]));
                continue;
            }
            if(arg.equalsIgnoreCase("--storeName")) {
                keystoreGenerator.keyStoreName(args[i++]);
                continue;
            }
            if(arg.equalsIgnoreCase("--storeType")) {
                keystoreGenerator.storeType(args[i++]);
                continue;
            }
            if(arg.equalsIgnoreCase("--storePass")) {
                keystoreGenerator.storePass(args[i++]);
                continue;
            }
            if(arg.equalsIgnoreCase("--alias")) {
                keystoreGenerator.alias(args[i++]);
                continue;
            }
            help();
            return;
        }
        keystoreGenerator.build().generate();
    }

    public static KeyStoreGeneratorBuilder newKeyStoreGeneratorBuilder() {
        return new KeyStoreGenerator().new KeyStoreGeneratorBuilder();
    }

    public class KeyStoreGeneratorBuilder {
        public KeyStoreGeneratorBuilder symAlg(String symAlg) {
            KeyStoreGenerator.this.symAlg = symAlg;
            return this;
        }
        public KeyStoreGeneratorBuilder keySize(Integer keySize) {
            KeyStoreGenerator.this.keySize = keySize;
            return this;
        }
        public KeyStoreGeneratorBuilder keyStoreName(String keyStoreName) {
            KeyStoreGenerator.this.keyStoreName = keyStoreName;
            return this;
        }
        public KeyStoreGeneratorBuilder storeType(String storeType) {
            KeyStoreGenerator.this.storeType = storeType;
            return this;
        }
        public KeyStoreGeneratorBuilder storePass(String storePass) {
            KeyStoreGenerator.this.storePass = storePass;
            return this;
        }
        public KeyStoreGeneratorBuilder alias(String alias) {
            KeyStoreGenerator.this.alias = alias;
            return this;
        }
        public KeyStoreGenerator build() {
            // we create an immutable key store generator
            return new KeyStoreGenerator(KeyStoreGenerator.this);
        }
    }

    public void generate() {
        Path path = Path.of(keyStoreName);
        createFolders(path.getParent());
        log.info("Creating file '%s' using algorithm '%s' size '%d'\n", path, symAlg, keySize);
        try(OutputStream stream=new FileOutputStream(path.toString())) {
            SecretKey key=createSecretKey(symAlg, keySize);
            KeyStore store=KeyStore.getInstance(storeType);
            store.load(null, null);
            store.setKeyEntry(alias, key, storePass.toCharArray(), null);
            store.store(stream, storePass.toCharArray());
            log.info("Finished keystore creation");
        }
        catch(Exception e) {
            log.error("An error happened while saving the key store", e);
        }

    }

    private void createFolders(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            log.error("Error creating %s", path);
        }
    }

    protected static void help() {
        System.out.println("KeyStoreGenerator [-help] [--alg algorithm] [--size size] [--storeName name] " +
                             "[--storeType type (e.g. JKS)] [--storePass password] [--alias alias]");
    }

    public static SecretKey createSecretKey() throws Exception {
        return createSecretKey(DEFAULT_SYM_ALGORITHM, DEFAULT_KEY_SIZE);
    }

    public static SecretKey createSecretKey(String sym_alg, int key_size) throws NoSuchAlgorithmException {
        KeyGenerator keyGen=KeyGenerator.getInstance(sym_alg);
        keyGen.init(key_size);
        return keyGen.generateKey();
    }
}
