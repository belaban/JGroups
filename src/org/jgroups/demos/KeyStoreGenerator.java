
package org.jgroups.demos;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;

/**
 * Generates a keystore file that has a SecretKey in it. It is not possible to
 * use keytool to achieve this. This is a simple way to generate a
 * JCEKS format keystore and SecretKey.
 * 
 * Usage is --alg ALGNAME --size ALGSIZE --storeName FILENAME --storePass
 * PASSWORD --alias KEYALIAS
 * 
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
    static String symAlg="AES";
    static int keySize=128;
    static String keyStoreName="defaultStore.keystore";
    static String storePass="changeit";
    static String alias="myKey";
    static String storeType="JCEKS";

	private KeyStoreGenerator() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    public static void main(String[] args) {
        int i=0;
        String arg=null;

        while(i < args.length && args[i].startsWith("-")) {
            arg=args[i++];
            if(arg.equalsIgnoreCase("--alg")) {
                symAlg=args[i++];
                continue;
            }
            if(arg.equalsIgnoreCase("--size")) {
                keySize=Integer.parseInt(args[i++]);
                continue;
            }
            if(arg.equalsIgnoreCase("--storeName")) {
                keyStoreName=args[i++];
                continue;
            }
            if(arg.equalsIgnoreCase("--storeType")) {
                storeType=args[i++];
                continue;
            }
            if(arg.equalsIgnoreCase("--storePass")) {
                    storePass=args[i++];
                continue;
            }
            if(arg.equalsIgnoreCase("--alias")) {
                alias=args[i++];
                continue;
            }
            help();
            return;
        }
        System.out.printf("Creating file '%s' using algorithm '%s' size '%d'\n", keyStoreName, symAlg, keySize);

        try(OutputStream stream=new FileOutputStream(keyStoreName)) {
            SecretKey key=createSecretKey();
            KeyStore store=KeyStore.getInstance(storeType);
            store.load(null, null);
            store.setKeyEntry(alias, key, storePass.toCharArray(), null);
            store.store(stream, storePass.toCharArray());

        }
        catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println("Finished keystore creation");
    }

    protected static void help() {
        System.out.println("KeyStoreGenerator [-help] [--alg algorithm] [--size size] [--storeName name] " +
                             "[--storeType type (e.g. JKS)] [--storePass password] [--alias alias]");
    }

    public static SecretKey createSecretKey() throws Exception {
        return createSecretKey(symAlg, keySize);
    }

    public static SecretKey createSecretKey(String sym_alg, int key_size) throws NoSuchAlgorithmException {
        // KeyGenerator keyGen=KeyGenerator.getInstance(getAlgorithm(sym_alg));
        KeyGenerator keyGen=KeyGenerator.getInstance(sym_alg);
        keyGen.init(key_size);
        return keyGen.generateKey();
    }

    private static String getAlgorithm(String s) {
        int index=s.indexOf('/');
        if(index == -1)
            return s;
        return s.substring(0, index);
    }
}
