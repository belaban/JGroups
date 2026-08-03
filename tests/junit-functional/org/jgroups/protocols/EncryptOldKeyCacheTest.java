package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.BytesMessage;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.demos.KeyStoreGenerator;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Regression test: after a group-key rotation, ASYM_ENCRYPT drops messages encrypted with the previous group
 * key, because the previous key is never cached in {@code key_map}.
 *
 * <p>{@code cacheGroupKey(version)} is only ever called <em>after</em> {@code secret_key} has already been overwritten
 * with the new key (see {@code createNewKey()}/{@code installSharedGroupKey()}), so {@code key_map} only ever holds
 * the (current_version -> current_key) mapping. {@link Encrypt#decrypt} only consults {@code key_map} for a message
 * whose version differs from {@code sym_version} - i.e. it looks for the OLD key -, which is never present. Such a
 * message is therefore silently dropped.</p>
 *
 * <p>This test encrypts a message with key #1 (version V1), rotates the group key to #2 (version V2), and then tries
 * to decrypt the still-in-flight V1 message. It expects the message to be recovered with the cached old key; with the
 * buggy code it is dropped, so the test FAILS - reproducing the bug.</p>
 *
 * @see Encrypt#decrypt
 */
@Test(groups=Global.ENCRYPT, singleThreaded=true)
public class EncryptOldKeyCacheTest {

    protected static final String PAYLOAD="hello world, encrypted with the old group key";

    public void testOldKeyStillDecryptsAfterRotation() throws Exception {
        Harness enc=new Harness().asymKeylength(512)
          .setId(ClassConfigurator.getProtocolId(ASYM_ENCRYPT.class));
        enc.setup(KeyStoreGenerator.createSecretKey());  // group key #1 -> sym_version V1
        byte[] v1=enc.currentVersion();
        System.out.printf("%s: initial symmetry version: %s\n", enc.name(), Util.byteArrayToHexString(v1));

        // 1) A message encrypted with key #1 / version V1 (as if sent just before the key rotation)
        Message in_flight=enc.encryptMessage(new BytesMessage(null).setArray(PAYLOAD.getBytes()));

        // sanity check: as long as the key hasn't rotated, the message decrypts fine
        Message dec1=enc.decryptBatch(in_flight);
        assert dec1 != null;
        assert Arrays.equals(dec1.getArray(), PAYLOAD.getBytes());

        // 2) rotate the group key (as done on member churn / coord change) -> key #2, version V2
        enc.rotateKey();
        byte[] v2=enc.currentVersion();
        System.out.printf("  %s: after rotation: %s (keys cached: %s)\n",
                          enc.name(), Util.byteArrayToHexString(v2), enc.keyMap());
        assert !Arrays.equals(v1, v2);

        // 3) the in-flight message (still stamped V1) arrives *after* the rotation. Per the comment on
        //    cacheGroupKey("put the previous key into the map"), the old key should have been cached so it can be
        //    decrypted. The current code drops it instead.
        Message dec2=enc.decryptBatch(in_flight);
        System.out.printf("  %s: decrypting in-flight old-key message -> %s%n",
                          enc.name(), dec2 == null? "DROPPED (null)" : "recovered");
        assert dec2 != null : "B1: in-flight message encrypted with the old group key was DROPPED after key rotation " +
          "(the old key was never cached in key_map; key_map holds only: " + enc.keyMap() + ")";
        assert Arrays.equals(dec2.getArray(), PAYLOAD.getBytes());
    }


    /**
     * Subclasses ASYM_ENCRYPT to drive the real (buggy) code path without spinning up a full cluster:
     * {@code secret_key}, {@code sym_version}, {@code key_map} and {@code createNewKey()}/cacheGroupKey() are
     * exercised exactly as in production.
     */
    protected static class Harness extends ASYM_ENCRYPT {
        void setup(SecretKey key) throws Exception {
            secret_key=key; // the group key
            init();         // base init(): creates key_map and initializes ciphers + sym_version from secret_key
        }

        String name() {return "ASYM_ENCRYPT";}
        byte[] currentVersion() {return sym_version;}
        String keyMap() {return printCachedGroupKeys();}

        /** Rotates the group key; exercises createNewKey() -> initSymCiphers() -> cacheGroupKey() (the buggy call). */
        void rotateKey() {
            createNewKey("");
        }

        Message encryptMessage(Message msg) throws Exception {
            return encrypt(msg); // base Encrypt.encrypt(): stamps EncryptHeader with current sym_version + iv
        }

        /** Decrypts via the same path used for message batches (passing the looked-up key into the cipher). */
        Message decryptBatch(Message msg) throws Exception {
            BlockingQueue<Cipher> q=decoding_ciphers;
            Cipher cipher=q.take();
            try {
                return decrypt(cipher, msg.copy(true, true));
            }
            finally {
                q.offer(cipher);
            }
        }
    }
}