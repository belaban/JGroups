
package org.jgroups.demos;

import org.jgroups.util.Util;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.security.KeyStore;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

/**
 * Generates a keystore file that has a SecretKey in it. It is not possible to
 * use the keytool JDk tool to achieve this. This is a simple way to generate a
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

	private KeyStoreGenerator() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    public static void main(String[] args) {

        int i=0;
        String arg=null;

        while(i < args.length && args[i].startsWith("-")) {
            arg=args[i++];
            System.out.println("Found arg of " + arg);
            if(arg.equalsIgnoreCase("--alg")) {
                if(i < args.length) {
                    symAlg=args[i++];
                }
                else {
                    System.out.println("No Algorithm supplied using default of " + symAlg);
                }
            }
            else if(arg.equalsIgnoreCase("--size")) {
                if(i < args.length) {
                    keySize=Integer.parseInt(args[i++]);
                }
                else {
                    System.out.println("No Size supplied using default of " + keySize);
                }
            }
            else if(arg.equalsIgnoreCase("--storeName")) {

                if(i < args.length) {
                    keyStoreName=args[i++];
                }
                else {
                    System.out.println("No keystore supplied using default of " + keyStoreName);
                }
            }
            else if(arg.equalsIgnoreCase("--storePass")) {
                if(i < args.length) {
                    storePass=args[i++];
                }
                else {
                    System.out.println("No password supplied using default of " + storePass);
                }
            }
            else if(arg.equalsIgnoreCase("--alias")) {
                if(i < args.length) {
                    alias=args[i++];
                }
                else {
                    System.out.println("No alias supplied using default of " + alias);
                }
            }
        }
        System.out.println("Creating file '" + keyStoreName
                           + "' using Algorithm '"
                           + symAlg
                           + "' size '"
                           + keySize
                           + "'");

        OutputStream stream=null;
        try {
            stream=new FileOutputStream(keyStoreName);
            SecretKey key=initSymKey();
            KeyStore store=KeyStore.getInstance("JCEKS");
            store.load(null, null);
            store.setKeyEntry(alias, key, storePass.toCharArray(), null);
            store.store(stream, storePass.toCharArray());

        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                Util.close(stream);
            }
            catch(Exception e) {

            }
        }
        System.out.println("Finished keystore creation");
    }

    public static SecretKey initSymKey() throws Exception {
        KeyGenerator keyGen=KeyGenerator.getInstance(getAlgorithm(symAlg));
        keyGen.init(keySize);
        return keyGen.generateKey();

    }

    private static String getAlgorithm(String s) {
        int index=s.indexOf("/");
        if(index == -1)
            return s;

        return s.substring(0, index);
    }
}
