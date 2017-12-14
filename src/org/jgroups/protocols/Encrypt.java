package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import javax.crypto.Cipher;
import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

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

    @Property(description="Initial public/private key length. Default is 512")
    protected int                           asym_keylength=512;

    @Property(description="Initial key length for matching symmetric algorithm. Default is 128")
    protected int                           sym_keylength=128;

    @Property(description="Number of ciphers in the pool to parallelize encrypt and decrypt requests",writable=false)
    protected int                           cipher_pool_size=8;

    @Property(description="If true, the entire message (including payload and headers) is encrypted, else only the payload")
    protected boolean                       encrypt_entire_message=true;

    @Property(description="If true, all messages are digitally signed by adding an encrypted checksum of the encrypted " +
      "message to the header. Ignored if encrypt_entire_message is false")
    protected boolean                       sign_msgs=true;

    @Property(description="When sign_msgs is true, by default CRC32 is used to create the checksum. If use_adler is " +
      "true, Adler32 will be used")
    protected boolean                       use_adler;

    @Property(description="Max number of keys in key_map")
    protected int                           key_map_max_size=20;

    protected volatile Address              local_addr;

    protected volatile View                 view;

    // Cipher pools used for encryption and decryption. Size is cipher_pool_size
    protected BlockingQueue<Cipher>         encoding_ciphers, decoding_ciphers;

    // version filed for secret key
    protected volatile byte[]               sym_version;

    // shared secret key to encrypt/decrypt messages
    protected volatile Key                  secret_key;

    // map to hold previous keys so we can decrypt some earlier messages if we need to
    protected Map<AsciiString,Cipher>       key_map;

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
    public String                   asymAlgorithm()                 {return asym_algorithm;}
    public <T extends Encrypt<E>> T asymAlgorithm(String alg)       {this.asym_algorithm=alg; return (T)this;}
    public byte[]                   symVersion()                    {return sym_version;}
    public <T extends Encrypt<E>> T localAddress(Address addr)      {this.local_addr=addr; return (T)this;}
    public boolean                  encryptEntireMessage()          {return encrypt_entire_message;}
    public <T extends Encrypt<E>> T encryptEntireMessage(boolean b) {this.encrypt_entire_message=b; return (T)this;}
    public boolean                  signMessages()                  {return this.sign_msgs;}
    public <T extends Encrypt<E>> T signMessages(boolean flag)      {this.sign_msgs=flag; return (T)this;}
    public boolean                  adler()                         {return use_adler;}
    public <T extends Encrypt<E>> T adler(boolean flag)             {this.use_adler=flag; return (T)this;}
    @ManagedAttribute public String version()                       {return Util.byteArrayToHexString(sym_version);}

    public void init() throws Exception {
        int tmp=Util.getNextHigherPowerOfTwo(cipher_pool_size);
        if(tmp != cipher_pool_size) {
            log.warn("%s: setting cipher_pool_size (%d) to %d (power of 2) for faster modulo operation", local_addr, cipher_pool_size, tmp);
            cipher_pool_size=tmp;
        }
        key_map=new BoundedHashMap<>(key_map_max_size);
        encoding_ciphers=new ArrayBlockingQueue<>(cipher_pool_size);
        decoding_ciphers=new ArrayBlockingQueue<>(cipher_pool_size);
        initSymCiphers(sym_algorithm, secret_key);
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;

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
        try {
            return handleUpMessage(msg);
        }
        catch(Exception e) {
            log.warn("%s: exception occurred decrypting message", local_addr, e);
        }
        return null;
    }

    public void up(MessageBatch batch) {
        Cipher cipher=null;
        try {
            if(secret_key == null) {
                log.trace("%s: discarded %s batch from %s as secret key is null",
                          local_addr, batch.dest() == null? "mcast" : "unicast", batch.sender());
                return;
            }
            BiConsumer<Message,MessageBatch> decrypter=new Decrypter(cipher=decoding_ciphers.take());
            batch.forEach(decrypter);
        }
        catch(InterruptedException e) {
            log.error("%s: failed processing batch; discarding batch", local_addr, e);
            // we need to drop the batch if we for example have a failure fetching a cipher, or else other messages
            // in the batch might make it up the stack, bypassing decryption! This is not an issue because encryption
            // is below NAKACK2 or UNICAST3, so messages will get retransmitted
            return;
        }
        finally {
            if(cipher != null)
                decoding_ciphers.offer(cipher);
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }



    /** Initialises the ciphers for both encryption and decryption using the generated or supplied secret key */
    protected synchronized void initSymCiphers(String algorithm, Key secret) throws Exception {
        if(secret == null)
            return;
        encoding_ciphers.clear();
        decoding_ciphers.clear();
        for(int i=0; i < cipher_pool_size; i++ ) {
            encoding_ciphers.offer(createCipher(Cipher.ENCRYPT_MODE, secret, algorithm));
            decoding_ciphers.offer(createCipher(Cipher.DECRYPT_MODE, secret, algorithm));
        };

        // set the version
        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(secret.getEncoded());

        byte[] tmp=digest.digest();
        sym_version=Arrays.copyOf(tmp, tmp.length);
        // log.debug("%s: created %d symmetric ciphers with secret key (%d bytes)", local_addr, cipher_pool_size, sym_version.length);
    }


    protected Cipher createCipher(int mode, Key secret_key, String algorithm) throws Exception {
        Cipher cipher=provider != null && !provider.trim().isEmpty()?
          Cipher.getInstance(algorithm, provider) : Cipher.getInstance(algorithm);
        cipher.init(mode, secret_key);
        return cipher;
    }


    protected Object handleUpMessage(Message msg) throws Exception {
        EncryptHeader hdr=msg.getHeader(this.id);
        if(hdr == null) {
            log.error("%s: received message without encrypt header from %s; dropping it", local_addr, msg.src());
            return null;
        }
        switch(hdr.type()) {
            case EncryptHeader.ENCRYPT:
                return handleEncryptedMessage(msg);
            default:
                return handleUpEvent(msg,hdr);
        }
    }


    protected Object handleEncryptedMessage(Message msg) throws Exception {
        if(!process(msg))
            return null;

        // try and decrypt the message - we need to copy msg as we modify its
        // buffer (http://jira.jboss.com/jira/browse/JGRP-538)
        Message tmpMsg=decryptMessage(null, msg.copy()); // need to copy for possible xmits
        if(tmpMsg != null)
            return up_prot.up(tmpMsg);
        log.warn("%s: unrecognized cipher; discarding message from %s", local_addr, msg.src());
        return null;
    }

    protected Object handleUpEvent(Message msg, EncryptHeader hdr) {
        return null;
    }

    /** Whether or not to process this received message */
    protected boolean process(Message msg) {return true;}

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

    protected Checksum createChecksummer() {return use_adler? new Adler32() : new CRC32();}


    /** Does the actual work for decrypting - if version does not match current cipher then tries the previous cipher */
    protected Message decryptMessage(Cipher cipher, Message msg) throws Exception {
        EncryptHeader hdr=msg.getHeader(this.id);
        if(!Arrays.equals(hdr.version(), sym_version)) {
            cipher=key_map.get(new AsciiString(hdr.version()));
            if(cipher == null) {
                handleUnknownVersion(hdr.version);
                return null;
            }
            log.trace("%s: decrypting msg from %s using previous cipher version", local_addr, msg.src());
            return _decrypt(cipher, msg, hdr);
        }
        return _decrypt(cipher, msg, hdr);
    }

    protected Message _decrypt(final Cipher cipher, Message msg, EncryptHeader hdr) throws Exception {
        byte[] decrypted_msg;

        if(!encrypt_entire_message && msg.getLength() == 0)
            return msg;

        if(encrypt_entire_message && sign_msgs) {
            byte[] signature=hdr.signature();
            if(signature == null) {
                log.error("%s: dropped message from %s as the header did not have a checksum", local_addr, msg.src());
                return null;
            }

            long msg_checksum=decryptChecksum(cipher, signature, 0, signature.length);
            long actual_checksum=computeChecksum(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            if(actual_checksum != msg_checksum) {
                log.error("%s: dropped message from %s as the message's checksum (%d) did not match the computed checksum (%d)",
                          local_addr, msg.src(), msg_checksum, actual_checksum);
                return null;
            }
        }

        if(cipher == null)
            decrypted_msg=code(msg.getRawBuffer(), msg.getOffset(), msg.getLength(), true);
        else
            decrypted_msg=cipher.doFinal(msg.getRawBuffer(), msg.getOffset(), msg.getLength());

        if(!encrypt_entire_message) {
            msg.setBuffer(decrypted_msg);
            return msg;
        }

        Message ret=Util.streamableFromBuffer(Message::new,decrypted_msg,0,decrypted_msg.length);
        if(ret.getDest() == null)
            ret.setDest(msg.getDest());
        if(ret.getSrc() == null)
            ret.setSrc(msg.getSrc());
        return ret;
    }


    protected void encryptAndSend(Message msg) throws Exception {
        EncryptHeader hdr=new EncryptHeader(EncryptHeader.ENCRYPT, symVersion());
        if(encrypt_entire_message) {
            if(msg.getSrc() == null)
                msg.setSrc(local_addr);

            Buffer serialized_msg=Util.streamableToBuffer(msg);
            byte[] encrypted_msg=code(serialized_msg.getBuf(),serialized_msg.getOffset(),serialized_msg.getLength(),false);

            if(sign_msgs) {
                long checksum=computeChecksum(encrypted_msg, 0, encrypted_msg.length);
                byte[] checksum_array=encryptChecksum(checksum);
                hdr.signature(checksum_array);
            }

            // exclude existing headers, they will be seen again when we decrypt and unmarshal the msg at the receiver
            Message tmp=msg.copy(false, false).setBuffer(encrypted_msg).putHeader(this.id,hdr);
            down_prot.down(tmp);
            return;
        }

        // copy neeeded because same message (object) may be retransmitted -> prevent double encryption
        Message msgEncrypted=msg.copy(false).putHeader(this.id, hdr);
        if(msg.getLength() > 0)
            msgEncrypted.setBuffer(code(msg.getRawBuffer(),msg.getOffset(),msg.getLength(),false));
        else { // length is 0
            byte[] payload=msg.getRawBuffer();
            if(payload != null) // we don't encrypt empty buffers (https://issues.jboss.org/browse/JGRP-2153)
                msgEncrypted.setBuffer(payload, msg.getOffset(), msg.getLength());
        }
        down_prot.down(msgEncrypted);
    }


    protected byte[] code(byte[] buf, int offset, int length, boolean decode) throws Exception {
        BlockingQueue<Cipher> queue=decode? decoding_ciphers : encoding_ciphers;
        Cipher cipher=queue.take();
        try {
            return cipher.doFinal(buf, offset, length);
        }
        finally {
            queue.offer(cipher);
        }
    }

    protected long computeChecksum(byte[] input, int offset, int length) {
        Checksum checksummer=createChecksummer();
        checksummer.update(input, offset, length);
        return checksummer.getValue();
    }

    protected byte[] encryptChecksum(long checksum) throws Exception {
        byte[] checksum_array=new byte[Global.LONG_SIZE];
        Bits.writeLong(checksum, checksum_array, 0);
        return code(checksum_array, 0, checksum_array.length, false);
    }

    protected long decryptChecksum(final Cipher cipher, byte[] input, int offset, int length) throws Exception {
        byte[] decrypted_checksum;
        if(cipher == null)
            decrypted_checksum=code(input, offset, length, true);
        else
            decrypted_checksum=cipher.doFinal(input, offset, length);
        return Bits.readLong(decrypted_checksum, 0);
    }


    /* Get the algorithm name from "algorithm/mode/padding"  taken from original ENCRYPT */
    protected static String getAlgorithm(String s) {
        int index=s.indexOf('/');
        return index == -1? s : s.substring(0, index);
    }


    /** Called when the version shipped in the header can't be found */
    protected void handleUnknownVersion(byte[] version) {

    }


    /** Decrypts all messages in a batch, replacing encrypted messages in-place with their decrypted versions */
    protected class Decrypter implements BiConsumer<Message,MessageBatch> {
        protected final Cipher cipher;

        public Decrypter(Cipher cipher) {
            this.cipher=cipher;
        }

        public void accept(Message msg, MessageBatch batch) {
            EncryptHeader hdr;
            if((hdr=msg.getHeader(id)) == null) {
                log.error("%s: received message without encrypt header from %s; dropping it", local_addr, batch.sender());
                batch.remove(msg); // remove from batch to prevent passing the message further up as part of the batch
                return;
            }

            if(hdr.type() == EncryptHeader.ENCRYPT) {
                try {
                    if(!process(msg)) {
                        batch.remove(msg);
                        return;
                    }
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
            else {
                batch.remove(msg); // a control message will get handled by ENCRYPT and should not be passed up
                handleUpEvent(msg, hdr);
            }
        }
    }

}
