 package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.*;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Encrypts and decrypts communication in JGroups by using a secret key distributed to all cluster members by the
 * key server (coordinator) using asymmetric (public/private key) encryption.<br>
 *
 * The secret key is identical for all cluster members and is used to encrypt messages when sending and decrypt them
 * when receiving messages.
 *
 * This protocol is typically placed under {@link org.jgroups.protocols.pbcast.NAKACK2}.<br>
 *
 * The current keyserver (always the coordinator) generates a secret key. When a new member joins, it asks the keyserver
 * for the secret key. The keyserver encrypts the secret key with the joiner's public key and the joiner decrypts it with
 * its private key and then installs it and starts encrypting and decrypting messages with the secret key.<br>
 *
 * View changes that identify a new keyserver will result in a new secret key being generated and then distributed to
 * all cluster members. This overhead can be substantial in an application with a reasonable member churn.<br>
 *
 * This protocol is suited for an application that does not ship with a known key but instead it is generated and
 * distributed by the keyserver.
 *
 * Since messages can only get encrypted and decrypted when the secret key was received from the keyserver, messages
 * are dropped when the secret key hasn't been installed yet.
 *
 * @author Bela Ban
 * @author Steve Woodcock
 */
@MBean(description="Asymmetric encryption protocol. The secret key for encryption and decryption of messages is fetched " +
  "from a key server (the coordinator) via asymmetric encryption")
public class ASYM_ENCRYPT extends Encrypt<KeyStore.PrivateKeyEntry> {
    protected static final short                GMS_ID=ClassConfigurator.getProtocolId(GMS.class);

    @Property(description="When a node leaves, change the secret group key, preventing old members from eavesdropping")
    protected boolean                           change_key_on_leave;

    @Property(description="Change the secret group key when the coordinator changes. If enabled, this will take " +
      "place even if change_key_on_leave is disabled.")
    protected boolean                           change_key_on_coord_leave=true;

    @Property(description="If true, a separate KeyExchange protocol (somewhere in the stack) is used to" +
      " fetch the shared secret key. If false, the default (built-in) key exchange protocol will be used.")
    protected boolean                           use_external_key_exchange;
    protected KeyExchange                       key_exchange;
    protected volatile Address                  key_server_addr;
    protected volatile boolean                  send_group_keys;    // set by handleView()
    protected KeyPair                           key_pair;     // to store own's public/private Key
    protected Cipher                            asym_cipher;  // decrypting cypher for secret key requests
    protected final Map<Address,byte[]>         pub_map=new ConcurrentHashMap<>(); // map of members and their public keys

    // cache server address between reception of INSTALL_MERGE_VIEW and sending of VIEW (MergeView)
    protected static final ThreadLocal<Address> srv_addr=new ThreadLocal<>();


    @Override
    public ASYM_ENCRYPT setKeyStoreEntry(KeyStore.PrivateKeyEntry entry) {
        this.key_pair = new KeyPair(entry.getCertificate().getPublicKey(), entry.getPrivateKey());
        return this;
    }

    public boolean       getChangeKeyOnLeave()                {return change_key_on_leave;}
    public ASYM_ENCRYPT  setChangeKeyOnLeave(boolean c)       {change_key_on_leave=c; return this;}
    public boolean       getChangeKeyOnCoordLeave()           {return change_key_on_coord_leave;}
    public ASYM_ENCRYPT  setChangeKeyOnCoordLeave(boolean c)  {change_key_on_coord_leave=c; return this;}
    public boolean       getUseExternalKeyExchange()          {return use_external_key_exchange;}
    public ASYM_ENCRYPT  setUseExternalKeyExchange(boolean u) {use_external_key_exchange=u; return this;}
    public KeyPair       keyPair()                            {return key_pair;}
    public Cipher        asymCipher()                         {return asym_cipher;}
    public Address       keyServerAddr()                      {return key_server_addr;}
    public ASYM_ENCRYPT  keyServerAddr(Address ks)            {this.key_server_addr=ks; return this;}

    public List<Integer> providedDownServices() {
        return Arrays.asList(Event.GET_SECRET_KEY, Event.SET_SECRET_KEY);
    }


    @ManagedAttribute(description="Keys in the public key map")
    public String getPublicKeys() {
        return pub_map.keySet().toString();
    }

    @ManagedAttribute(description="The current key server")
    public String getKeyServerAddress() {return key_server_addr != null? key_server_addr.toString() : "null";}

    @ManagedAttribute(description="True if this member is the current key server, false otherwise")
    public boolean isKeyServer() {
        return Objects.equals(key_server_addr, local_addr);
    }

    public void init() throws Exception {
        send_group_keys=false;
        initKeyPair();
        super.init();
        if(use_external_key_exchange)
            fetchAndSetKeyExchange();
    }


    public void start() throws Exception {
        super.start();
        pub_map.put(local_addr, key_pair.getPublic().getEncoded());
    }

    public Object down(Event evt) {
        if(evt.type() == Event.INSTALL_MERGE_VIEW) { // only received by the merge *leader*
            // the new group key will be added to the next INSTALL_MERGE_VIEW message (if !use_external_key_exchange)
            createNewKey("because of an INSTALL_MERGE_VIEW event");
        }
        return super.down(evt);
    }

    public Object down(Message msg) {
        Processing processing=skipDownMessage(msg);
        if(processing == Processing.PROCESS)
            return super.down(msg);
        if(processing == Processing.SKIP)
            return down_prot.down(msg);
        return null; // DROP
    }

    public Object up(Event evt) {
        switch(evt.type()) {
            case Event.GET_SECRET_KEY:
                return new Tuple<>(secret_key, sym_version);
            case Event.SET_SECRET_KEY:
                Tuple<SecretKey,byte[]> tuple=evt.arg();
                try {
                    installSharedGroupKey(null, tuple.getVal1(), tuple.getVal2());
                }
                catch(Exception ex) {
                    log.error("failed setting group key", ex);
                }
                return null;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        if(dropMulticastMessageFromNonMember(msg))
            return null;
        if(skipUpMessage(msg))
            return up_prot.up(msg);
        return super.up(msg);
    }

    public void up(MessageBatch batch) {
        MessageIterator it=batch.iterator();
        while(it.hasNext()) {
            Message msg=it.next();
            if(dropMulticastMessageFromNonMember(msg)) {
                it.remove();
                continue;
            }
            if(skipUpMessage(msg)) {
                try {
                    up_prot.up(msg);
                    it.remove();
                }
                catch(Throwable t) {
                    log.error("failed passing up message from %s: %s, ex=%s", msg.src(), msg.printHeaders(), t);
                }
            }
        }
        if(!batch.isEmpty())
            super.up(batch); // decrypt the rest of the messages in the batch (if any)
    }

    protected boolean dropMulticastMessageFromNonMember(Message msg) {
        return msg.dest() == null &&
          !inView(msg.src(), String.format("%s: dropped multicast message from non-member %s", local_addr, msg.getSrc()));
    }

    public ASYM_ENCRYPT fetchAndSetKeyExchange() {
        if((key_exchange=stack.findProtocol(KeyExchange.class)) == null)
            throw new IllegalStateException(KeyExchange.class.getSimpleName() + " not found in stack");
        return this;
    }

    protected static void cacheServerAddress(Address srv) {
        srv_addr.set(srv);
    }

    protected static Address getCachedServerAddress() {
        Address retval=srv_addr.get();
        srv_addr.remove();
        return retval;
    }


    /**
     * Processes a message with a GMS header (e.g. by adding the secret key to a JOIN response) and returns true if
     * the message should be passed down (not encrypted) or false if the message needs to be encrypted
     * @return Processing {@link Processing#DROP} if the message needs to be dropped, {@link Processing#SKIP} if the
     *           message needs to be skipped (not encrypted), or {@link Processing#PROCESS} if the message needs to be
     *           processed (= encrypted)
     */
    protected Processing skipDownMessage(Message msg) {
        GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
        if(hdr == null)
            return Processing.PROCESS;
        switch(hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
            case GMS.GmsHeader.MERGE_RSP:
                if(!use_external_key_exchange) {
                    // attach our public key to the JOIN-REQ
                    Message copy=addKeysToMessage(msg, true, false, null);
                    down_prot.down(copy);
                    return Processing.DROP;
                }
                return Processing.SKIP;
            case GMS.GmsHeader.JOIN_RSP:
                return addMetadata(msg,false, msg.getDest(), true);
            case GMS.GmsHeader.VIEW:
                boolean tmp=send_group_keys;
                send_group_keys=false;
                return addMetadata(msg, tmp, null, tmp);
            case GMS.GmsHeader.INSTALL_MERGE_VIEW:
                // a new group key was created in down(INSTALL_MERGE_VIEW)
                if(Objects.equals(local_addr, msg.dest()))
                    break;
                return addMetadata(msg, true, null, true);
            case GMS.GmsHeader.MERGE_REQ:
            case GMS.GmsHeader.VIEW_ACK:
            case GMS.GmsHeader.GET_DIGEST_REQ:
            case GMS.GmsHeader.GET_DIGEST_RSP:
                return Processing.SKIP;
        }
        return Processing.PROCESS;
    }

    /** Checks if the message contains a public key (and adds it to pub_map if present) or an encrypted group key
     * (and installs it if present) */
    protected boolean skipUpMessage(Message msg) {
        GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
        if(hdr == null)
            return false;

        EncryptHeader h=msg.getHeader(id);
        switch(hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
            case GMS.GmsHeader.MERGE_RSP:
                return processEncryptMessage(msg, h, true);
            case GMS.GmsHeader.JOIN_RSP:
            case GMS.GmsHeader.VIEW:
            case GMS.GmsHeader.INSTALL_MERGE_VIEW:
                if(hdr.getType() == GMS.GmsHeader.INSTALL_MERGE_VIEW)
                    cacheServerAddress(h.server());
                return processEncryptMessage(msg, h, false);
            case GMS.GmsHeader.MERGE_REQ:
            case GMS.GmsHeader.VIEW_ACK:
            case GMS.GmsHeader.GET_DIGEST_REQ:
            case GMS.GmsHeader.GET_DIGEST_RSP:
                return true;
        }
        return false;
    }

    protected boolean processEncryptMessage(Message msg, EncryptHeader hdr, boolean retval) {
        if(hdr == null)
            return retval;
        switch(hdr.type) {
            case EncryptHeader.INSTALL_KEYS:
                removeKeysFromMessageAndInstall(msg, hdr.version());
                break;
            case EncryptHeader.FETCH_SHARED_KEY:
                if(!Objects.equals(local_addr, msg.getSrc())) {
                    try {
                        Address key_server=hdr.server() != null? hdr.server() : msg.src();
                        if(log.isTraceEnabled())
                            log.trace("%s: fetching group key from %s", local_addr, key_server);
                        key_exchange.fetchSecretKeyFrom(key_server);
                    }
                    catch(Exception e) {
                        log.warn("%s: failed fetching group key from %s: %s", local_addr, msg.src(), e);
                    }
                }
                break;
        }
        return retval;
    }

    protected void installPublicKeys(Address sender, byte[] buf, int offset, int length) {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, offset, length);
        try {
            int num_keys=in.readInt();
            for(int i=0; i < num_keys; i++) {
                Address mbr=Util.readAddress(in);
                int len=in.readInt();
                byte[] key=new byte[len];
                in.readFully(key, 0, key.length);
                pub_map.put(mbr, key);
            }
            log.trace("%s: added %d public keys to local cache", local_addr, num_keys);
        }
        catch(Exception ex) {
            log.error("%s: failed reading public keys received from %s: %s", local_addr, sender, ex);
        }
    }


    protected Processing addMetadata(Message msg, boolean add_secret_keys,
                                     Address include_secret_key_only_for, boolean attach_fetch_key_header) {
        try {
            if(use_external_key_exchange && !attach_fetch_key_header)
                return Processing.PROCESS;

            Message encr_msg=encrypt(msg); // makes a copy
            if(use_external_key_exchange) {
                // attach a FETCH_SHARED_KEY to the message; this causes the recipient to fetch and install the
                // shared key *before* delivering the message (so it can be decrypted)
                Address srv=key_exchange.getServerLocation();
                if(srv == null)
                    srv=getCachedServerAddress();
                log.trace("%s: asking %s to fetch the shared group key %s via an external key exchange protocol (srv=%s)",
                          local_addr, encr_msg.getDest() == null? "all members" : encr_msg.getDest(),
                          Util.byteArrayToHexString(sym_version), srv);
                encr_msg.putHeader(id, new EncryptHeader(EncryptHeader.FETCH_SHARED_KEY, symVersion(), getIv(encr_msg)).server(srv));
            }
            else {
                encr_msg=addKeysToMessage(encr_msg, false, add_secret_keys, include_secret_key_only_for);
                if(add_secret_keys || include_secret_key_only_for != null)
                    log.trace("%s: sending encrypted group key to %s (version: %s)", local_addr,
                              encr_msg.getDest() == null? "all members" : encr_msg.getDest(),
                              Util.byteArrayToHexString(sym_version));
            }
            down_prot.down(encr_msg);
            return Processing.DROP; // the encrypted msg was already sent; no need to send the un-encrypted msg
        }
        catch(Exception ex) {
            log.warn("%s: unable to send message down: %s", local_addr, ex.getMessage());
            return Processing.PROCESS;
        }
    }


    /**
     * Adds the public and/or encrypted shared keys to the payload of msg. If msg already has a payload, the message
     * will be copied and the new payload consists of the keys and the original payload
     * @param msg The original message
     * @return A copy of the message
     */
    protected Message addKeysToMessage(Message msg, boolean copy, boolean add_secret_keys, Address serialize_only) {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(pub_map.size() * 200 + msg.getLength());
        try {
            serializeKeys(out, add_secret_keys, serialize_only);
            if(msg.getLength() > 0) // add the original buffer
                out.write(msg.getArray(), msg.getOffset(), msg.getLength());
            return (copy? msg.copy(true, true) : msg).setArray(out.getBuffer())
              .putHeader(id, new EncryptHeader(EncryptHeader.INSTALL_KEYS, symVersion(), getIv(msg)));
        }
        catch(Throwable t) {
            log.error("%s: failed adding keys to message: %s", local_addr, t);
            return null;
        }
    }

    /**
     * Removes the public and/or private keys from the payload of msg and installs them. If there is some payload left
     * (the original payload), the offset of the message will be changed. Otherwise, the payload will be nulled, to
     * re-create the original message
     */
    protected void removeKeysFromMessageAndInstall(Message msg, byte[] version) {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(msg.getArray(), msg.getOffset(), msg.getLength());
        unserializeAndInstallKeys(msg.getSrc(), version, in);
        int len=msg.getLength(), offset=msg.getOffset(), bytes_read=in.position();
        // we can modify the original message as the sender sends a copy (even on retransmissions)
        if(offset + bytes_read == len)
            msg.setArray(null, 0, 0); // the original payload must have been null
        else
            msg.setArray(msg.getArray(), offset+bytes_read, len-bytes_read);
    }

    /** Serializes all public keys and their corresponding encrypted shared group keys into a buffer */
    protected void serializeKeys(ByteArrayDataOutputStream out, boolean serialize_shared_keys,
                                 Address serialize_only) throws Exception {
        out.writeInt(pub_map.size()); // number of entries (actual value is written after serialization)
        int num=0;
        for(Map.Entry<Address,byte[]> e: pub_map.entrySet()) {
            Address mbr=e.getKey();
            byte[] public_key=e.getValue(); // the encoded public key
            Util.writeAddress(mbr, out);
            // public keys
            out.writeInt(public_key.length);
            out.write(public_key, 0, public_key.length);

            if(serialize_shared_keys || Objects.equals(mbr, serialize_only)) {
                PublicKey pk=makePublicKey(public_key);
                byte[] encrypted_shared_key=encryptSecretKey(secret_key, pk); // the encrypted shared group key
                out.writeInt(encrypted_shared_key.length);
                out.write(encrypted_shared_key, 0, encrypted_shared_key.length);
            }
            else
                out.writeInt(0);
            num++;
        }
        int curr_pos=out.position();
        out.position(0).writeInt(num);
        out.position(curr_pos);
    }

    /** Unserializes public keys and installs them to pub_map, then reads encrypted shared keys and install our own */
    protected void unserializeAndInstallKeys(Address sender, byte[] version, ByteArrayDataInputStream in) {
        try {
            int num_keys=in.readInt();
            for(int i=0; i < num_keys; i++) {
                Address mbr=Util.readAddress(in);
                int len=in.readInt();
                if(len > 0) {
                    byte[] public_key=new byte[len];
                    in.readFully(public_key, 0, public_key.length);
                    pub_map.put(mbr, public_key);
                }
                if((len=in.readInt()) > 0) {
                    byte[] encrypted_shared_group_key=new byte[len];
                    in.readFully(encrypted_shared_group_key, 0, encrypted_shared_group_key.length);
                    if(local_addr.equals(mbr)) {
                        try {
                            SecretKey tmp=decodeKey(encrypted_shared_group_key);
                            if(tmp != null)
                                installSharedGroupKey(sender, tmp, version); // otherwise set the received key as the shared key
                        }
                        catch(Exception e) {
                            log.warn("%s: unable to process key received from %s: %s", local_addr, sender, e);
                        }
                    }
                }
            }
        }
        catch(Exception ex) {
            log.error("%s: failed reading keys received from %s: %s", local_addr, sender, ex);
        }
    }


    protected static ByteArray serializeKeys(Map<Address,byte[]> keys) throws Exception {
        int num_keys=keys.size();
        if(num_keys == 0)
            return null;
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(num_keys * 100);
        out.writeInt(num_keys);
        for(Map.Entry<Address,byte[]> e: keys.entrySet()) {
            Util.writeAddress(e.getKey(), out);
            byte[] val=e.getValue();
            out.writeInt(val.length);
            out.write(val, 0, val.length);
        }
        return out.getBuffer();
    }

    protected Map<Address,byte[]> unserializeKeys(Address sender, byte[] buf, int offset, int length) {
        Map<Address,byte[]> map=new HashMap<>();
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, offset, length);
        try {
            int num_keys=in.readInt();
            for(int i=0; i < num_keys; i++) {
                Address mbr=Util.readAddress(in);
                int len=in.readInt();
                byte[] key=new byte[len];
                in.readFully(key, 0, key.length);
                map.put(mbr, key);
            }
        }
        catch(Exception ex) {
            log.error("%s: failed reading keys received from %s: %s", local_addr, sender, ex);
        }
        return map;
    }


    /** Initialise the symmetric key if none is supplied in a keystore */
    protected SecretKey createSecretKey() throws Exception {
        KeyGenerator keyGen=null;
        // see if we have a provider specified
        if(provider != null && !provider.trim().isEmpty())
            keyGen=KeyGenerator.getInstance(getAlgorithm(sym_algorithm), provider);
        else
            keyGen=KeyGenerator.getInstance(getAlgorithm(sym_algorithm));
        // generate the key using the defined init properties
        keyGen.init(sym_keylength);
        return keyGen.generateKey();
    }



    /** Generates the public/private key pair from the init params */
    protected void initKeyPair() throws Exception {
        if (this.key_pair == null) {
            // generate keys according to the specified algorithms
            // generate publicKey and Private Key
            KeyPairGenerator KpairGen=null;
            if(provider != null && !provider.trim().isEmpty())
                KpairGen=KeyPairGenerator.getInstance(getAlgorithm(asym_algorithm), provider);
            else
                KpairGen=KeyPairGenerator.getInstance(getAlgorithm(asym_algorithm));
            KpairGen.initialize(asym_keylength,new SecureRandom());
            key_pair=KpairGen.generateKeyPair();
        }

        // set up the Cipher to decrypt secret key responses encrypted with our key
        if(provider != null && !provider.trim().isEmpty())
            asym_cipher=Cipher.getInstance(asym_algorithm, provider);
        else
            asym_cipher=Cipher.getInstance(asym_algorithm);
        asym_cipher.init(Cipher.DECRYPT_MODE, key_pair.getPrivate());
    }


    @Override protected void handleView(View v) {
        boolean left_mbrs, create_new_key, key_server_changed;

        pub_map.keySet().retainAll(v.getMembers());
        synchronized(this) {
            key_server_changed=!Objects.equals(v.getCoord(), key_server_addr);
            left_mbrs=this.view != null && !v.containsMembers(this.view.getMembersRaw());
            super.handleView(v);
            key_server_addr=v.getCoord(); // the coordinator is the keyserver
            create_new_key=secret_key == null // always create a group key the first time (key is null)
              || change_key_on_leave && left_mbrs // && has higher precedence than ||
              || change_key_on_coord_leave && key_server_changed;
            // if we get a MergeView, the secret key has already been created (in down(INSTALL_MERGE_VIEW)
            create_new_key&=!(v instanceof MergeView);

            // keys will be added to VIEW in down()
            send_group_keys=create_new_key  || v instanceof MergeView;

            if(!Objects.equals(key_server_addr, local_addr))
                return; // all non-coordinators are done at this point

            if(key_server_changed)
                log.debug("%s: I'm the new key server", local_addr);
            if(create_new_key)
                createNewKey("because of new view " + v);
        }
    }


    protected void createNewKey(String message) {
        try {
            this.secret_key=createSecretKey();
            initSymCiphers(sym_algorithm, secret_key);
            log.debug("%s: created new group key (version: %s) %s", local_addr, Util.byteArrayToHexString(sym_version), message);
            cacheGroupKey(sym_version);
        }
        catch(Exception ex) {
            log.error("%s: failed creating group key and initializing ciphers", local_addr, ex);
        }
    }


    protected synchronized void installSharedGroupKey(Address sender, SecretKey key, byte[] version) throws Exception {
        if(Arrays.equals(this.sym_version, version)) {
            log.debug("%s: ignoring group key received from %s (version: %s); it has already been installed",
                      local_addr, sender != null? sender : "key exchange protocol", Util.byteArrayToHexString(version));
            return;
        }
        log.debug("%s: installing group key received from %s (version: %s)",
                  local_addr, sender != null? sender : "key exchange protocol", Util.byteArrayToHexString(version));
        secret_key=key;
        initSymCiphers(sym_algorithm, key);
        sym_version=version;
        cacheGroupKey(version);
    }

    /** Cache the current shared key to decrypt messages encrypted with the old shared group key */
    protected void cacheGroupKey(byte[] version) throws Exception {
        // put the previous key into the map
        if(secret_key != null)
            key_map.putIfAbsent(new AsciiString(version), secret_key);
    }

    /** Encrypts the current secret key with the requester's public key (the requester will decrypt it with its private key) */
    protected byte[] encryptSecretKey(Key secret_key, PublicKey public_key) throws Exception {
        Cipher tmp;
        if (provider != null && !provider.trim().isEmpty())
            tmp=Cipher.getInstance(asym_algorithm, provider);
        else
            tmp=Cipher.getInstance(asym_algorithm);
        tmp.init(Cipher.ENCRYPT_MODE, public_key);

        // encrypt current secret key
        return tmp.doFinal(secret_key.getEncoded());
    }


    protected SecretKeySpec decodeKey(byte[] encodedKey) throws Exception {
        byte[] keyBytes;

        synchronized(this) {
            try {
                keyBytes=asym_cipher.doFinal(encodedKey);
            }
            catch (BadPaddingException | IllegalBlockSizeException e) {
                //  if any exception is thrown, this cipher object may need to be reset before it can be used again.
                asym_cipher.init(Cipher.DECRYPT_MODE, key_pair.getPrivate());
                throw e;
            }
        }

        try {
            SecretKeySpec keySpec=new SecretKeySpec(keyBytes, getAlgorithm(sym_algorithm));
            Cipher temp;
            if (provider != null && !provider.trim().isEmpty())
                temp=Cipher.getInstance(sym_algorithm, provider);
            else
                temp=Cipher.getInstance(sym_algorithm);
            temp.init(Cipher.SECRET_KEY, keySpec);
            return keySpec;
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedDecodingKey"), e);
            return null;
        }
    }

    /** Used to reconstitute public key sent in byte form from peer */
    protected PublicKey makePublicKey(byte[] encodedKey) {
        PublicKey pubKey=null;
        try {
            KeyFactory KeyFac=KeyFactory.getInstance(getAlgorithm(asym_algorithm));
            X509EncodedKeySpec x509KeySpec=new X509EncodedKeySpec(encodedKey);
            pubKey=KeyFac.generatePublic(x509KeySpec);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        return pubKey;
    }

    protected byte[] getIv(Message msg) {
        EncryptHeader h=msg.getHeader(id);
        if (h == null)
            return null;
        return h.iv();
    }

    protected enum Processing {SKIP, PROCESS, DROP}
}
