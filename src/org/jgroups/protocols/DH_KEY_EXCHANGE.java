package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.MessageIterator;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.function.Supplier;

/**
 * Key exchange based on Diffie-Hellman-Merkle (https://en.wikipedia.org/wiki/Diffie%E2%80%93Hellman_key_exchange).<br/>
 * Diffie-Hellman is used between a member and a key server (the coordinator) to obtain a session key
 * (only known to the key server and the joiner) which is used by the key server to encrypt the shared secret symmetric
 * (group) key and by the requester to decrypt the group key it gets in the response of the key server.
 * <br/>
 * Note that this implementation is not immune against man-in-the-middle attacks.
 * @author Bela Ban
 * @since  4.0.5
 */
@MBean(description="Key exchange protocol to fetch a shared secret group key from the key server." +
  "That shared (symmetric) key is subsequently used to encrypt communication between cluster members")
public class DH_KEY_EXCHANGE extends KeyExchange {

    protected enum Type {
        // sent from joiner to key server, carries dh_key
        SECRET_KEY_REQ,
        // sent from key server to joiner, carries dh_key of key server, encrypted secret key and version of secret key
        SECRET_KEY_RSP
    }

    @Property(description="The type of secret key to be sent up the stack (converted from DH). " +
      "Should be the same as the algorithm part of ASYM_ENCRYPT.sym_algorithm if ASYM_ENCRYPT is used")
    protected String                        secret_key_algorithm="AES";

    @Property(description="The length of the secret key (in bits) to be sent up the stack. AES requires 128 bits. " +
      "Should be the same as ASYM_ENCRYPT.sym_keylength if ASYM_ENCRYPT is used.")
    protected int                           secret_key_length=128; // used for AES

    @Property(description="Max time (in ms) that a FETCH_SECRET_KEY down event will be ignored (if an existing " +
      "request is in progress) until a new request for the secret key is sent to the keyserver",type=AttributeType.TIME)
    protected long                          timeout=2000;

    /** Diffie-Hellman protocol engine */
    protected KeyAgreement                  key_agreement;

    /** The public key used for the Diffie-Hellman key exchange to obtain the session key (used to encrypt the
     * keyserver's secret key) */
    protected PublicKey                     dh_key;

    /** Time (ms) when the last key request was sent, prevents too many requests */
    protected long                          last_key_request;

    protected static final KeyPairGenerator key_pair_gen;
    protected static final KeyFactory       dh_key_factory;


    static {
        try {
            key_pair_gen=KeyPairGenerator.getInstance("DH");
            dh_key_factory=KeyFactory.getInstance("DH");
        }
        catch(NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


    public void init() throws Exception {
        super.init();
        if(secret_key_length % 8 != 0)
            throw new IllegalStateException(String.format("secret_key_length (%d) must be a multiple of 8", secret_key_length));

        ASYM_ENCRYPT asym_encrypt=findProtocolAbove(ASYM_ENCRYPT.class);
        if(asym_encrypt != null) {
            String sym_alg=asym_encrypt.symKeyAlgorithm();
            int sym_keylen=asym_encrypt.symKeylength();
            if(!Util.match(sym_alg, secret_key_algorithm)) {
                log.warn("overriding %s=%s to %s from %s", "secret_key_algorithm", secret_key_algorithm,
                         sym_alg, ASYM_ENCRYPT.class.getSimpleName());
                secret_key_algorithm=sym_alg;
            }
            if(sym_keylen != secret_key_length) {
                log.warn("overriding %s=%d to %d from %s", "secret_key_length", secret_key_length,
                         sym_keylen, ASYM_ENCRYPT.class.getSimpleName());
                secret_key_length=sym_keylen;
            }
        }
        key_agreement=KeyAgreement.getInstance("DH");
    }



    public void fetchSecretKeyFrom(Address target) throws NoSuchAlgorithmException, InvalidKeyException {
        byte[] encoded_dh_key=null;
        synchronized(this) {
            if(dh_key != null) {
                long curr_time;
                if((curr_time=System.currentTimeMillis()) - last_key_request >= timeout) {
                    last_key_request=curr_time;
                    encoded_dh_key=dh_key.getEncoded();
                }
            }
            else {
                KeyPair kp=key_pair_gen.generateKeyPair();
                PrivateKey private_key=kp.getPrivate();
                dh_key=kp.getPublic(); // to be sent to target
                encoded_dh_key=dh_key.getEncoded();
                key_agreement.init(private_key);
                log.debug("%s: sending public key %s.. to %s", local_addr, print16(dh_key), target);
            }
        }
        if(encoded_dh_key != null) {
            Message msg=new EmptyMessage(target).putHeader(id, DhHeader.createSecretKeyRequest(encoded_dh_key));
            down_prot.down(msg);
        }
    }

    public Address getServerLocation() {return null;}

    public Object up(Message msg) {
        DhHeader hdr=msg.getHeader(id);
        if(hdr != null) {
            handle(hdr, msg.getSrc());
            return null;
        }
        return up_prot.up(msg);
    }



    public void up(MessageBatch batch) {
        MessageIterator it=batch.iterator();
        while(it.hasNext()) {
            Message msg=it.next();
            DhHeader hdr=msg.getHeader(id);
            if(hdr != null) {
                it.remove();
                handle(hdr, msg.getSrc());
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    protected void handle(DhHeader hdr, Address sender) {
        try {
            PublicKey pub_key=dh_key_factory.generatePublic(new X509EncodedKeySpec(hdr.dh_key));
            switch(hdr.type) {
                case SECRET_KEY_REQ:
                    handleSecretKeyRequest(pub_key, sender);
                    break;
                case SECRET_KEY_RSP:
                    handleSecretKeyResponse(pub_key, hdr.encrypted_secret_key, hdr.secret_key_version, sender);
                    break;
                default:
                    log.warn("unknown header type %d", hdr.type);
            }
        }
        catch(Throwable t) {
            log.error(String.format("failed handling request %s", hdr), t);
        }
    }

    protected void handleSecretKeyRequest(PublicKey dh_public_key, Address sender) throws Exception {
        KeyPair    kp=key_pair_gen.generateKeyPair();
        PrivateKey private_key=kp.getPrivate();
        PublicKey  public_key_rsp=kp.getPublic(); // sent back as part of the response
        byte[]     version;
        byte[]     encrypted_secret_key;

        log.debug("%s: received public key %s.. from %s", local_addr, print16(dh_public_key), sender);

        synchronized(this) {
            key_agreement.init(private_key);
            key_agreement.doPhase(dh_public_key, true);
            // Diffie-Hellman secret session key, to encrypt secret key
            byte[] secret_session_key=key_agreement.generateSecret();

            SecretKey hashed_session_key=hash(secret_session_key);
            Cipher encrypter=Cipher.getInstance(secret_key_algorithm);
            encrypter.init(Cipher.ENCRYPT_MODE, hashed_session_key);

            Tuple<SecretKey,byte[]> tuple=(Tuple<SecretKey,byte[]>)up_prot.up(new Event(Event.GET_SECRET_KEY));
            SecretKey secret_key=tuple.getVal1();
            version=tuple.getVal2();
            encrypted_secret_key=encrypter.doFinal(secret_key.getEncoded());
        }

        log.debug("%s: sending public key rsp %s.. to %s", local_addr, print16(public_key_rsp), sender);

        // send response to sender with public_key_rsp, encrypted secret key and secret key version
        Message rsp=new EmptyMessage(sender)
          .putHeader(id, DhHeader.createSecretKeyResponse(public_key_rsp.getEncoded(),
                                                          encrypted_secret_key, version));
        down_prot.down(rsp);
    }

    protected void handleSecretKeyResponse(PublicKey dh_public_key, byte[] encrypted_secret_key,
                                           byte[] version, Address sender) throws Exception {
        Tuple<SecretKey,byte[]> tuple=null;

        log.debug("%s: received public key rsp %s.. from %s", local_addr, print16(dh_public_key), sender);

        synchronized(this) {
            key_agreement.doPhase(dh_public_key, true);

            // Diffie-Hellman secret session key, to decrypt secret key
            byte[] secret_session_key=key_agreement.generateSecret();
            SecretKey hashed_session_key=hash(secret_session_key);

            Cipher encrypter=Cipher.getInstance(secret_key_algorithm);
            encrypter.init(Cipher.DECRYPT_MODE, hashed_session_key);

            byte[] secret_key=encrypter.doFinal(encrypted_secret_key); // <-- this is the shared group key
            SecretKey sk=new SecretKeySpec(secret_key, secret_key_algorithm);
            tuple=new Tuple<>(sk, version);
            dh_key=null;
        }
        log.debug("%s: sending up secret key (version: %s)", local_addr, Util.byteArrayToHexString(version));
        up_prot.up(new Event(Event.SET_SECRET_KEY, tuple));
    }

    protected SecretKey hash(byte[] key) throws Exception {
        // use SHA256 to create a hash of secret_key and only then truncate it to secret_key_length
        MessageDigest digest=MessageDigest.getInstance("SHA-256");
        digest.update(key);
        byte[] hashed_key=digest.digest();
        return new SecretKeySpec(hashed_key, 0, secret_key_length/8, secret_key_algorithm);
    }

    protected static String print16(PublicKey pub_key) {
        // use SHA256 to create a hash of secret_key and only then truncate it to secret_key_length
        MessageDigest digest=null;
        try {
            digest=MessageDigest.getInstance("SHA-256");
            digest.update(pub_key.getEncoded());
            return Util.byteArrayToHexString(digest.digest(), 0, 16);
        }
        catch(NoSuchAlgorithmException e) {
            return e.toString();
        }
    }




    public static class DhHeader extends Header {
        protected Type   type;
        protected byte[] dh_key;
        protected byte[] encrypted_secret_key;
        protected byte[] secret_key_version;

        public DhHeader() {
        }

        public static DhHeader createSecretKeyRequest(byte[] dh_key) {
            DhHeader hdr=new DhHeader();
            hdr.type=Type.SECRET_KEY_REQ;
            hdr.dh_key=dh_key;
            return hdr;
        }

        public static DhHeader createSecretKeyResponse(byte[] dh_pub_key, byte[] encrypted_secret_key,
                                                       byte[] version) {
            DhHeader hdr=new DhHeader();
            hdr.type=Type.SECRET_KEY_RSP;
            hdr.dh_key=dh_pub_key;
            hdr.encrypted_secret_key=encrypted_secret_key;
            hdr.secret_key_version=version;
            return hdr;
        }

        public Supplier<? extends Header> create()          {return DhHeader::new;}
        public short                      getMagicId()      {return 92;}
        public byte[]                     dhKey()           {return dh_key;}
        public byte[]                     encryptedSecret() {return encrypted_secret_key;}
        public byte[]                     version()         {return secret_key_version;}

        @Override
        public int serializedSize() {
            switch(type) {
                case SECRET_KEY_REQ:
                    return Global.BYTE_SIZE + Global.INT_SIZE
                      + (dh_key != null? dh_key.length : 0);
                case SECRET_KEY_RSP:
                    return Global.BYTE_SIZE + Global.INT_SIZE*3
                      + (dh_key != null? dh_key.length : 0)
                      + (encrypted_secret_key != null? encrypted_secret_key.length : 0)
                      + (secret_key_version != null? secret_key_version.length : 0);
                default: return  0; // should never happen!
            }
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type.ordinal());
            switch(type) {
                case SECRET_KEY_REQ:
                    int size=dh_key != null? dh_key.length : 0;
                    out.writeInt(size);
                    if(dh_key != null)
                        out.write(dh_key);
                    break;
                case SECRET_KEY_RSP:
                    size=dh_key != null? dh_key.length : 0;
                    out.writeInt(size);
                    if(size > 0)
                        out.write(dh_key);
                    size=encrypted_secret_key != null? encrypted_secret_key.length : 0;
                    out.writeInt(size);
                    if(encrypted_secret_key != null)
                        out.write(encrypted_secret_key);
                    size=secret_key_version != null? secret_key_version.length : 0;
                    out.writeInt(size);
                    if(secret_key_version != null)
                        out.write(secret_key_version);
                    break;
            }
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            byte ordinal=in.readByte();
            type=Type.values()[ordinal];

            int size=in.readInt();
            if(size > 0) {
                dh_key=new byte[size];
                in.readFully(dh_key);
            }

            switch(type) {
                case SECRET_KEY_REQ:
                    break;
                case SECRET_KEY_RSP:
                    size=in.readInt();
                    if(size > 0) {
                        encrypted_secret_key=new byte[size];
                        in.readFully(encrypted_secret_key);
                    }
                    size=in.readInt();
                    if(size > 0) {
                        secret_key_version=new byte[size];
                        in.readFully(secret_key_version);
                    }
                    break;
            }
        }

        public String toString() {
            if(type == null) return "n/a";
            switch(type) {
                case SECRET_KEY_REQ:
                    return String.format("%s dh-key %d bytes", type, dh_key.length);
                case SECRET_KEY_RSP:
                    return String.format("%s dh-key %d bytes, encrypted secret %d bytes, version: %s", type, dh_key.length,
                                         encrypted_secret_key.length, Util.byteArrayToHexString(secret_key_version));
                    default: return "n/a";
            }
        }
    }


}

