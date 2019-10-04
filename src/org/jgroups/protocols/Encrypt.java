package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AsciiString;
import org.jgroups.util.BoundedHashMap;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Super class of symmetric ({@link SYM_ENCRYPT}) and asymmetric ({@link ASYM_ENCRYPT}) encryption protocols.
 * @author Bela Ban
 */
public abstract class Encrypt<E extends KeyStore.Entry> extends Protocol {
    protected static final String DEFAULT_SYM_ALGO="AES";


    /* -----------------------------------------    Properties     -------------------------------------------------- */
    @Property(description="Cryptographic Service Provider")
    protected String                        provider;

    @Property(description="Cipher engine transformation for asymmetric algorithm. Default is RSA")
    protected String                        asym_algorithm="RSA";

    @Property(description="Cipher engine transformation for symmetric algorithm. Default is AES")
    protected String                        sym_algorithm=DEFAULT_SYM_ALGO;

    @Property(description="Initialization vector length for symmetric encryption. A value must be specified here " +
      "if the configured sym_algorithm requires an initialization vector.")
    protected int                           sym_iv_length;

    @Property(description="Initial public/private key length. Default is 2048")
    protected int                           asym_keylength=2048;

    @Property(description="Initial key length for matching symmetric algorithm. Default is 128")
    protected int                           sym_keylength=128;

    @Property(description="Number of ciphers in the pool to parallelize encrypt and decrypt requests",writable=false)
    protected int                           cipher_pool_size=8;

    @Property(description="If true, the entire message (including payload and headers) is encrypted, else only the payload",
      deprecatedMessage="ignored (always false)")
    @Deprecated
    protected boolean                       encrypt_entire_message;

    @Property(description="If true, all messages are digitally signed by adding an encrypted checksum of the encrypted " +
      "message to the header. Ignored if encrypt_entire_message is false",deprecatedMessage="ignored (always false)")
    @Deprecated
    protected boolean                       sign_msgs;

    @Property(description="When sign_msgs is true, by default CRC32 is used to create the checksum. If use_adler is " +
      "true, Adler32 will be used",deprecatedMessage="ignored as sign_msgs has been deprecated")
    @Deprecated
    protected boolean                       use_adler;

    @Property(description="Max number of keys in key_map")
    protected int                           key_map_max_size=20;

    protected volatile Address              local_addr;

    protected volatile View                 view;

    // Cipher pools used for encryption and decryption. Size is cipher_pool_size
    protected volatile BlockingQueue<Cipher> encoding_ciphers, decoding_ciphers;

    // version filed for secret key
    protected volatile byte[]               sym_version;

    // shared secret key to encrypt/decrypt messages
    protected volatile Key                  secret_key;

    // map to hold previous keys so we can decrypt some earlier messages if we need to
    protected Map<AsciiString,Key>          key_map;

    // SecureRandom instance for generating IV's
    protected SecureRandom                  secure_random = new SecureRandom();

    /**
     * Sets the key store entry used to configure this protocol.
     * @param entry a key store entry
     */
    public abstract void setKeyStoreEntry(E entry);


    public int                      asymKeylength()                 {return asym_keylength;}
    public <T extends Encrypt<E>> T asymKeylength(int len)          {this.asym_keylength=len; return (T)this;}
    public int                      symKeylength()                  {return sym_keylength;}
    public <T extends Encrypt<E>> T symKeylength(int len)           {this.sym_keylength=len; return (T)this;}
    public Key                      secretKey()                     {return secret_key;}
    public String                   symAlgorithm()                  {return sym_algorithm;}
    public <T extends Encrypt<E>> T symAlgorithm(String alg)        {this.sym_algorithm=alg; return (T)this;}
    public String                   symKeyAlgorithm()               {return getAlgorithm(sym_algorithm);}
    public int                      simIvLength()                   {return sym_iv_length;}
    public <T extends Encrypt<E>> T symIvLength(int len)            {this.sym_iv_length=len; return (T)this;}
    public String                   asymAlgorithm()                 {return asym_algorithm;}
    public <T extends Encrypt<E>> T asymAlgorithm(String alg)       {this.asym_algorithm=alg; return (T)this;}
    public byte[]                   symVersion()                    {return sym_version;}
    public <T extends Encrypt<E>> T localAddress(Address addr)      {this.local_addr=addr; return (T)this;}
    public SecureRandom             secureRandom()                  {return this.secure_random;}
    /** Allows callers to replace secure_random with impl of their choice, e.g. for performance reasons. */
    public <T extends Encrypt<E>> T secureRandom(SecureRandom sr)   {this.secure_random = sr; return (T)this;}
    @ManagedAttribute public String version()                       {return Util.byteArrayToHexString(sym_version);}


    @ManagedOperation(description="Prints the versions of the shared group keys cached in the key map")
    public String printCachedGroupKeys() {
        return key_map.keySet().stream().map(v -> Util.byteArrayToHexString(v.chars()))
          .collect(Collectors.joining(", "));
    }


    public void init() throws Exception {
        int tmp=Util.getNextHigherPowerOfTwo(cipher_pool_size);
        if(tmp != cipher_pool_size) {
            log.warn("%s: setting cipher_pool_size (%d) to %d (power of 2) for faster modulo operation", local_addr, cipher_pool_size, tmp);
            cipher_pool_size=tmp;
        }
        key_map=new BoundedHashMap<>(key_map_max_size);
        initSymCiphers(sym_algorithm, secret_key);
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                Object retval=down_prot.down(evt); // Start keyserver socket in SSL_KEY_EXCHANGE, for instance
                handleView(evt.getArg());
                return retval;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    public Object down(Message msg) {
        try {
            if(secret_key == null) {
                log.trace("%s: discarded %s message to %s as secret key is null, hdrs: %s",
                          local_addr, msg.dest() == null? "mcast" : "unicast", msg.dest(), msg.printHeaders());
                return null;
            }
            encryptAndSend(msg);
        }
        catch(Exception e) {
            log.warn("%s: unable to send message down", local_addr, e);
        }
        return null;
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        EncryptHeader hdr=msg.getHeader(this.id);
        if(hdr == null) {
            log.error("%s: received message without encrypt header from %s; dropping it", local_addr, msg.src());
            return null;
        }
        try {
            return handleEncryptedMessage(msg);
        }
        catch(Exception e) {
            log.warn("%s: exception occurred decrypting message", local_addr, e);
        }
        return null;
    }

    public void up(MessageBatch batch) {
        if(secret_key == null) {
            log.trace("%s: discarded %s batch from %s as secret key is null",
                      local_addr, batch.dest() == null? "mcast" : "unicast", batch.sender());
            return;
        }
        BlockingQueue<Cipher> cipherQueue = decoding_ciphers;
        if(cipherQueue == null)
            return;
        try {
            Cipher cipher=cipherQueue.take();
            try {
                BiConsumer<Message,MessageBatch> decrypter=new Decrypter(cipher);
                batch.forEach(decrypter);
            }
            finally {
                cipherQueue.offer(cipher);
            }
        }
        catch(InterruptedException e) {
            log.error("%s: failed processing batch; discarding batch", local_addr, e);
            // we need to drop the batch if we for example have a failure fetching a cipher, or else other messages
            // in the batch might make it up the stack, bypassing decryption! This is not an issue because encryption
            // is below NAKACK2 or UNICAST3, so messages will get retransmitted
            return;
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    /** Initialises the ciphers for both encryption and decryption using the generated or supplied secret key */
    protected void initSymCiphers(String algorithm, Key secret) throws Exception {
        if(secret == null)
            return;

        BlockingQueue<Cipher> tmp_encoding_ciphers=new ArrayBlockingQueue<>(cipher_pool_size);
        BlockingQueue<Cipher> tmp_decoding_ciphers=new ArrayBlockingQueue<>(cipher_pool_size);
        for(int i=0; i < cipher_pool_size; i++ ) {
            tmp_encoding_ciphers.offer(createCipher(algorithm));
            tmp_decoding_ciphers.offer(createCipher(algorithm));
        }

        // set the version
        MessageDigest digest=MessageDigest.getInstance("MD5");
        byte[] tmp_sym_version=digest.digest(secret.getEncoded());

        this.encoding_ciphers = tmp_encoding_ciphers;
        this.decoding_ciphers = tmp_decoding_ciphers;
        this.sym_version = tmp_sym_version;
    }


    protected Cipher createCipher(String algorithm) throws Exception {
        Cipher cipher=provider != null && !provider.trim().isEmpty()?
          Cipher.getInstance(algorithm, provider) : Cipher.getInstance(algorithm);
        return cipher;
    }

    protected void initCipher(Cipher cipher, int mode, Key secret_key, byte[] iv) throws Exception
    {
        if(iv != null)
            cipher.init(mode, secret_key, new IvParameterSpec(iv));
        else
            cipher.init(mode, secret_key);
    }

    protected byte[] makeIv() {
        if (sym_iv_length > 0) {
            byte[] iv = new byte[sym_iv_length];
            secure_random.nextBytes(iv);
            return iv;
        }
        return null;
    }

    protected Object handleEncryptedMessage(Message msg) throws Exception {
        // decrypt the message; we need to copy msg as we modify its buffer (http://jira.jboss.com/jira/browse/JGRP-538)
        Message tmpMsg=decryptMessage(null, msg.copy()); // need to copy for possible xmits
        return tmpMsg != null? up_prot.up(tmpMsg) : null;
    }


    protected void handleView(View view) {
        this.view=view;
    }

    protected boolean inView(Address sender, String error_msg) {
        View curr_view=this.view;
        if(curr_view == null || curr_view.containsMember(sender))
            return true;
        log.error(error_msg, sender, curr_view);
        return false;
    }


    /** Does the actual work for decrypting - if version does not match current cipher then tries the previous cipher */
    protected Message decryptMessage(Cipher cipher, Message msg) throws Exception {
        EncryptHeader hdr=msg.getHeader(this.id);
        // If the versions of the group keys don't match, we only try to use a previous version if the sender is in
        // the current view
        if(!Arrays.equals(hdr.version(), sym_version)) {
            if(!inView(msg.src(),
                       String.format("%s: rejected decryption of %s message from non-member %s",
                                     local_addr, msg.dest() == null? "multicast" : "unicast", msg.getSrc())))
                return null;
            Key key = key_map.get(new AsciiString(hdr.version()));
            if(key == null) {
                log.trace("%s: message from %s (version: %s) dropped, as a key matching that version wasn't found " +
                            "(current version: %s)",
                          local_addr, msg.src(), Util.byteArrayToHexString(hdr.version()), Util.byteArrayToHexString(sym_version));
                return null;
            }
            log.trace("%s: decrypting msg from %s using previous key version %s",
                      local_addr, msg.src(), Util.byteArrayToHexString(hdr.version()));
            return _decrypt(cipher, key, msg, hdr.iv());
        }
        return _decrypt(cipher, secret_key, msg, hdr.iv());
    }

    protected Message _decrypt(final Cipher cipher, Key key, Message msg, byte[] iv) throws Exception {
        if(msg.getLength() == 0)
            return msg;

        byte[] decrypted_msg;
        if(cipher == null)
            decrypted_msg=code(msg.getRawBuffer(), msg.getOffset(), msg.getLength(), iv, true);
        else {
            initCipher(cipher, Cipher.DECRYPT_MODE, key, iv);
            decrypted_msg=cipher.doFinal(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
        }
        return msg.setBuffer(decrypted_msg);
    }

    protected Message encrypt(Message msg) throws Exception {
        EncryptHeader hdr=new EncryptHeader((byte)0, symVersion(), makeIv());

        // copy needed because same message (object) may be retransmitted -> prevent double encryption
        Message msgEncrypted=msg.copy(false).putHeader(this.id, hdr);
        byte[] payload=msg.getRawBuffer();
        if(payload != null) {
            if(msg.getLength() > 0)
                msgEncrypted.setBuffer(code(payload, msg.getOffset(), msg.getLength(), hdr.iv(), false));
            else // length is 0, but buffer may be "" (empty, but *not null* buffer)! [JGRP-2153]
                msgEncrypted.setBuffer(payload, msg.getOffset(), msg.getLength());
        }
        return msgEncrypted;
    }

    protected void encryptAndSend(Message msg) throws Exception {
        down_prot.down(encrypt(msg));
    }


    protected byte[] code(byte[] buf, int offset, int length, byte[] iv, boolean decode) throws Exception {
        BlockingQueue<Cipher> queue=decode? decoding_ciphers : encoding_ciphers;
        Cipher cipher=queue.take();
        try {
            initCipher(cipher, decode ? Cipher.DECRYPT_MODE : Cipher.ENCRYPT_MODE, secret_key, iv);
            return cipher.doFinal(buf, offset, length);
        }
        finally {
            queue.offer(cipher);
        }
    }


    /* Get the algorithm name from "algorithm/mode/padding"  taken from original ENCRYPT */
    protected static String getAlgorithm(String s) {
        int index=s.indexOf('/');
        return index == -1? s : s.substring(0, index);
    }

    /* Get the mode/padding part of the transformation, if present */
    protected static String getModeAndPadding(String s) {
        int index=s.indexOf('/');
        String modeAndPadding = index == -1? null : s.substring(index + 1);
        if (modeAndPadding == null || modeAndPadding.isEmpty())
            return null;
        return modeAndPadding;
    }

    /** Decrypts all messages in a batch, replacing encrypted messages in-place with their decrypted versions */
    protected class Decrypter implements BiConsumer<Message,MessageBatch> {
        protected final Cipher cipher;

        public Decrypter(Cipher cipher) {
            this.cipher=cipher;
        }

        public void accept(Message msg, MessageBatch batch) {
            if(msg.getHeader(id) == null) {
                log.error("%s: received message without encrypt header from %s; dropping it", local_addr, batch.sender());
                batch.remove(msg); // remove from batch to prevent passing the message further up as part of the batch
                return;
            }
            try {
                Message tmpMsg=decryptMessage(cipher, msg.copy()); // need to copy for possible xmits
                if(tmpMsg != null)
                    batch.replace(msg, tmpMsg);
                else
                    batch.remove(msg);
            }
            catch(Exception e) {
                log.error("%s: failed decrypting message from %s (offset=%d, length=%d, buf.length=%d): %s, headers are %s",
                          local_addr, msg.getSrc(), msg.getOffset(), msg.getLength(), msg.getRawBuffer().length, e, msg.printHeaders());
                batch.remove(msg);
            }
        }
    }

}
