package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.util.*;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Encrypts and decrypts communication in JGroups by using a secret key distributed to all cluster members by the
 * key server (coordinator) using asymmetric (public/private key) encryption.<br>
 *
 * The secret key is identical for all cluster members and is used to encrypt messages when sending and decrypt them
 * when receiving messages.
 *
 * This protocol is typically placed under {@link org.jgroups.protocols.pbcast.NAKACK2}, so that most important
 * headers are encrypted as well, to prevent replay attacks.<br>
 *
 * The current keyserver (always the coordinator) generates a secret key. When a new member joins, it asks the keyserver
 * for the secret key. The keyserver encrypts the secret key with the joiner's public key and the joiner decrypts it with
 * its private key and then installs it and starts encrypting and decrypting messages with the secret key.<br>
 *
 * View changes that identify a new keyserver will result in a new secret key being generated and then distributed to
 * all cluster members. This overhead can be substantial in an application with a reasonable member churn.<br>
 *
 * This protocol is suited to an application that does not ship with a known key but instead it is generated and
 * distributed by the keyserver.
 *
 * Since messages can only get encrypted and decrypted when the secret key was received from the keyserver, messages
 * other then join and merge requests/responses are dropped when the secret key isn't yet available. Join and merge
 * requests / responses are handled by {@link AUTH}.
 *
 * @author Bela Ban
 * @author Steve Woodcock
 */
@MBean(description="Asymmetric encryption protocol. The secret key for encryption and decryption of messages is fetched " +
  "from a key server (the coordinator) via asymmetric encryption")
public class ASYM_ENCRYPT extends EncryptBase {
    protected static final short                   GMS_ID=ClassConfigurator.getProtocolId(GMS.class);

    @Property(description="When a member leaves, change the secret key, preventing old members from eavesdropping")
    protected boolean                              change_key_on_leave=true;

    @Property(description="If true, a separate KeyExchange protocol (somewhere below in ths stack) is used to" +
      " fetch the shared secret key. If false, the default (built-in) key exchange protocol will be used.")
    protected boolean                              use_external_key_exchange;

    @Property(description="Interval (in ms) to send out announcements when the key server changed. Members will then " +
      "start the key exchange protocol. When all members have acked, the task is cancelled.")
    protected long                                 key_server_interval=1000;

    protected volatile Address                     key_server_addr;
    protected KeyPair                              key_pair;     // to store own's public/private Key
    protected Cipher                               asym_cipher;  // decrypting cypher for secret key requests
    protected final Lock                           queue_lock=new ReentrantLock();

    // queue all up msgs until the secret key has been received/created
    @ManagedAttribute(description="whether or not to queue received messages (until the secret key was received)")
    protected boolean                              queue_up_msgs=true;

    // queues a bounded number of messages received during a null secret key (for fetching the key from a new coord)
    protected final BlockingQueue<Message>         up_queue=new ArrayBlockingQueue<>(100);

    @Property(description="Min time (in millis) between key requests")
    protected long                                 min_time_between_key_requests=2000;
    protected volatile long                        last_key_request;

    protected ResponseCollectorTask<Boolean>       key_requesters;


    public KeyPair      keyPair()                         {return key_pair;}
    public Cipher       asymCipher()                      {return asym_cipher;}
    public Address      keyServerAddr()                   {return key_server_addr;}
    public ASYM_ENCRYPT keyServerAddr(Address key_srv)    {this.key_server_addr=key_srv; return this;}
    public long         minTimeBetweenKeyRequests()       {return min_time_between_key_requests;}
    public ASYM_ENCRYPT minTimeBetweenKeyRequests(long t) {this.min_time_between_key_requests=t; return this;}

    public List<Integer> providedDownServices() {
        return Arrays.asList(Event.GET_SECRET_KEY, Event.SET_SECRET_KEY);
    }

    @ManagedAttribute(description="Number of received messages currently queued")
    public int queueSize() {return up_queue.size();}

    @ManagedAttribute(description="The current key server")
    public String getKeyServerAddress() {return key_server_addr != null? key_server_addr.toString() : "null";}

    @ManagedOperation(description="Triggers a request for the secret key to the current keyserver")
    public void sendKeyRequest() {
        if(key_server_addr == null) {
            log.debug("%s: sending secret key request failed as the key server is currently not set", local_addr);
            return;
        }
        sendKeyRequest(key_server_addr);
    }

    @ManagedAttribute(description="True if this member is the current key server, false otherwise")
    public boolean isKeyServer() {
        return Objects.equals(key_server_addr, local_addr);
    }

    public void init() throws Exception {
        initKeyPair();
        super.init();
        if(use_external_key_exchange) {
            List<Integer> provided_up_services=getDownServices();
            if(provided_up_services == null || !provided_up_services.contains(Event.FETCH_SECRET_KEY))
                throw new IllegalStateException("found no key exchange protocol below servicing event FETCH_SECRET_KEY");
        }
    }

    public void stop() {
        if(key_requesters != null)
            key_requesters.stop();
        stopQueueing();
        super.stop();
    }

    public Object down(Event evt) {
        if(evt.type() == Event.MSG) {
            Message msg=evt.arg();
            if(skip(msg))
                return down_prot.down(evt);
        }
        return super.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.type()) {
            case Event.MSG:
                Message msg=evt.arg();
                if(skip(msg)) {
                    GMS.GmsHeader hdr=(GMS.GmsHeader)msg.getHeader(GMS_ID);
                    Address key_server=getCoordinator(msg, hdr);
                    if(key_server != null) {
                        if(this.key_server_addr == null)
                            this.key_server_addr=key_server;
                        sendKeyRequest(key_server);
                    }
                    return up_prot.up(evt);
                }
                break;
            case Event.GET_SECRET_KEY:
                return new Tuple<>(secret_key, sym_version);
            case Event.SET_SECRET_KEY:
                Tuple<SecretKey,byte[]> tuple=evt.arg();
                try {
                    setKeys(tuple.getVal1(), tuple.getVal2());
                }
                catch(Exception ex) {
                    log.error("failed setting secret key", ex);
                }
                return null;
        }
        return super.up(evt);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(skip(msg)) {
                try {
                    up_prot.up(new Event(Event.MSG, msg));
                    batch.remove(msg);
                    GMS.GmsHeader hdr=(GMS.GmsHeader)msg.getHeader(GMS_ID);
                    Address key_server=getCoordinator(msg, hdr);
                    if(key_server != null)
                        sendKeyRequest(key_server);
                }
                catch(Throwable t) {
                    log.error("failed passing up message from %s: %s, ex=%s", msg.src(), msg.printHeaders(), t);
                }
            }
            EncryptHeader hdr=(EncryptHeader)msg.getHeader(this.id);
            if(hdr != null && hdr.type != EncryptHeader.ENCRYPT) {
                handleUpEvent(msg,hdr);
                batch.remove(msg);
            }
        }
        if(!batch.isEmpty())
            super.up(batch); // decrypt the rest of the messages in the batch (if any)
    }

    /** Tries to find out if this is a JOIN_RSP or INSTALL_MERGE_VIEW message and returns the coordinator of the view */
    protected Address getCoordinator(Message msg, GMS.GmsHeader hdr) {
        switch(hdr.getType()) {
            case GMS.GmsHeader.JOIN_RSP:
                try {
                    JoinRsp join_rsp=Util.streamableFromBuffer(JoinRsp.class, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    View new_view=join_rsp != null? join_rsp.getView() : null;
                    return new_view != null? new_view.getCoord() : null;
                }
                catch(Throwable t) {
                    log.error("%s: failed getting coordinator (keyserver) from JoinRsp: %s", local_addr, t);
                }
                break;
            case GMS.GmsHeader.INSTALL_MERGE_VIEW:
                try {
                    Tuple<View,Digest> tuple=GMS._readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    View new_view=tuple != null? tuple.getVal1() : null;
                    return new_view != null? new_view.getCoord() : null;
                }
                catch(Throwable t) {
                    log.error("%s: failed getting coordinator (keyserver) from INSTALL_MERGE_VIEW: %s", local_addr, t);
                }
                break;
        }
        return null;
    }


    /** Checks if a message needs to be encrypted/decrypted. Join and merge requests/responses don't need to be
     * encrypted as they're authenticated by {@link AUTH} */
    protected static boolean skip(Message msg) {
        GMS.GmsHeader hdr=(GMS.GmsHeader)msg.getHeader(GMS_ID);
        if(hdr == null)
            return false;
        switch(hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
            case GMS.GmsHeader.JOIN_RSP:
            case GMS.GmsHeader.MERGE_REQ:
            case GMS.GmsHeader.MERGE_RSP:
            case GMS.GmsHeader.VIEW_ACK:
            case GMS.GmsHeader.INSTALL_MERGE_VIEW:
            case GMS.GmsHeader.GET_DIGEST_REQ:
            case GMS.GmsHeader.GET_DIGEST_RSP:
                return true;
        }
        return false;
    }


    @Override protected Object handleUpEvent(Message msg, EncryptHeader hdr) {
        switch(hdr.type()) {
            case EncryptHeader.SECRET_KEY_REQ:
                handleSecretKeyRequest(msg);
                break;
            case EncryptHeader.SECRET_KEY_RSP:
                handleSecretKeyResponse(msg, hdr.version());
                sendNewKeyserverAck(msg.src());
                break;
            case EncryptHeader.NEW_KEYSERVER:
                Address sender=msg.src();
                if(!Objects.equals(key_server_addr, sender))
                    key_server_addr=sender;

                if(!Arrays.equals(sym_version, hdr.version)) // only send if sym_versions differ
                    sendKeyRequest(sender);
                else
                    sendNewKeyserverAck(sender);
                break;
            case EncryptHeader.NEW_KEYSERVER_ACK:
                if(key_requesters != null)
                    key_requesters.add(msg.src(), true);
                break;
        }
        return null;
    }

    @Override protected boolean process(Message msg) {
        if(enqueue(msg)) {
            log.trace("%s: queuing %s message from %s as secret key hasn't been retrieved from keyserver %s yet, hdrs: %s",
                      local_addr, msg.dest() == null? "mcast" : "unicast", msg.src(), key_server_addr, msg.printHeaders());
            sendKeyRequest(key_server_addr);
            return false;
        }
        return true;
    }

    protected void handleSecretKeyRequest(final Message msg) {
        if(!inView(msg.src(), "key requester %s is not in current view %s; ignoring key request"))
            return;
        log.debug("%s: received secret key request from %s", local_addr, msg.getSrc());
        try {
            PublicKey tmpKey=generatePubKey(msg.getBuffer());
            sendSecretKey(secret_key, tmpKey, msg.getSrc());
        }
        catch(Exception e) {
            log.warn("%s: unable to reconstitute peer's public key", local_addr);
        }
    }


    protected void handleSecretKeyResponse(final Message msg, final byte[] key_version) {
        if(!inView(msg.src(), "ignoring secret key sent by %s which is not in current view %s"))
            return;

        if(Arrays.equals(sym_version, key_version)) {
            log.debug("%s: secret key (version %s) already installed, ignoring key response",
                      local_addr, Util.byteArrayToHexString(key_version));
            stopQueueing();
            return;
        }
        try {
            SecretKey tmp=decodeKey(msg.getBuffer());
            if(tmp == null)
                sendKeyRequest(key_server_addr); // unable to understand response, let's try again
            else {
                // otherwise set the returned key as the shared key
                log.debug("%s: installing secret key received from %s (version: %s)",
                          local_addr, msg.getSrc(), Util.byteArrayToHexString(key_version));
                setKeys(tmp, key_version);
            }
        }
        catch(Exception e) {
            log.warn("%s: unable to process received public key", local_addr, e);
        }
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
        // generate keys according to the specified algorithms
        // generate publicKey and Private Key
        KeyPairGenerator KpairGen=null;
        if(provider != null && !provider.trim().isEmpty())
            KpairGen=KeyPairGenerator.getInstance(getAlgorithm(asym_algorithm), provider);
        else
            KpairGen=KeyPairGenerator.getInstance(getAlgorithm(asym_algorithm));
        KpairGen.initialize(asym_keylength,new SecureRandom());
        key_pair=KpairGen.generateKeyPair();

        // set up the Cipher to decrypt secret key responses encrypted with our key
        if(provider != null && !provider.trim().isEmpty())
            asym_cipher=Cipher.getInstance(asym_algorithm, provider);
        else
            asym_cipher=Cipher.getInstance(asym_algorithm);
        asym_cipher.init(Cipher.DECRYPT_MODE, key_pair.getPrivate());
    }


    @Override protected synchronized void handleView(View v) {
        boolean left_mbrs=change_key_on_leave && this.view != null && !v.containsMembers(this.view.getMembersRaw());
        boolean create_new_key=secret_key == null || left_mbrs;
        super.handleView(v);

        if(key_requesters != null)
            key_requesters.retainAll(v.getMembers());

        Address old_key_server=key_server_addr;
        key_server_addr=v.getCoord(); // the coordinator is the keyserver
        if(Objects.equals(key_server_addr, local_addr)) {
            if(!Objects.equals(key_server_addr, old_key_server))
                log.debug("%s: I'm the new key server", local_addr);
            if(create_new_key) {
                createNewKey();
                if(key_requesters != null)
                    key_requesters.stop();
                List<Address> targets=new ArrayList<>(v.getMembers());
                targets.remove(local_addr);

                if(!targets.isEmpty()) {  // https://issues.jboss.org/browse/JGRP-2203
                    key_requesters=new ResponseCollectorTask<Boolean>(targets)
                      .setPeriodicTask(new ResponseCollectorTask.Consumer<ResponseCollectorTask<Boolean>>() {
                          public void accept(ResponseCollectorTask<Boolean> t) {
                              Message msg=new Message(null).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK)
                                .putHeader(id, new EncryptHeader(EncryptHeader.NEW_KEYSERVER, sym_version));
                              down_prot.down(new Event(Event.MSG, msg));
                          }
                      })
                      .start(getTransport().getTimer(), 0, key_server_interval);
                }
            }
        }
        else
            handleNewKeyServer(old_key_server, v instanceof MergeView, left_mbrs);
    }


    protected void createNewKey() {
        try {
            this.secret_key=createSecretKey();
            initSymCiphers(sym_algorithm, secret_key);
            log.debug("%s: created new secret key (version: %s)", local_addr, Util.byteArrayToHexString(sym_version));
            stopQueueing();
        }
        catch(Exception ex) {
            log.error("%s: failed creating secret key and initializing ciphers", local_addr, ex);
        }
    }

    /** If the keyserver changed, send a request for the secret key to the keyserver */
    protected void handleNewKeyServer(Address old_key_server, boolean merge_view, boolean left_mbrs) {
        if(change_key_on_leave && (keyServerChanged(old_key_server) || merge_view || left_mbrs)) {
            startQueueing();
            log.debug("%s: sending request for secret key to the new keyserver %s", local_addr, key_server_addr);
            sendKeyRequest(key_server_addr);
        }
    }

	protected boolean keyServerChanged(Address old_keyserver) {
		return !Objects.equals(key_server_addr, old_keyserver);
	}


    protected void setKeys(SecretKey key, byte[] version) throws Exception {
        synchronized(this) {
            if(Arrays.equals(this.sym_version, version)) {
                stopQueueing();
                return;
            }
            Cipher decoding_cipher=secret_key != null? decoding_ciphers.take() : null;
            // put the previous key into the map, keep the cipher: no leak, as we'll clear decoding_ciphers in initSymCiphers()
            if(decoding_cipher != null)
                key_map.put(new AsciiString(version), decoding_cipher);
            secret_key=key;
            initSymCiphers(key.getAlgorithm(), key);
            sym_version=version;
        }
        stopQueueing();
    }


    protected void sendSecretKey(Key secret_key, PublicKey public_key, Address source) throws Exception {
        byte[] encryptedKey=encryptSecretKey(secret_key, public_key);
        Message newMsg=new Message(source, encryptedKey).src(local_addr)
          .putHeader(this.id, new EncryptHeader(EncryptHeader.SECRET_KEY_RSP, symVersion()));
        log.debug("%s: sending secret key response to %s (version: %s)", local_addr, source, Util.byteArrayToHexString(sym_version));
        down_prot.down(new Event(Event.MSG, newMsg));
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


    /** send client's public key to server and request server's public key */
    protected void sendKeyRequest(Address key_server) {
        if(key_server == null)
            return;
        if(last_key_request == 0 || System.currentTimeMillis() - last_key_request > min_time_between_key_requests)
            last_key_request=System.currentTimeMillis();
        else
            return;

        if(use_external_key_exchange) {
            log.debug("%s: asking key exchange protocol to get secret key", local_addr);
            down_prot.down(new Event(Event.FETCH_SECRET_KEY, key_server));
            return;
        }

        log.debug("%s: asking %s for the secret key (my version: %s)",
                  local_addr, key_server, Util.byteArrayToHexString(sym_version));
        Message newMsg=new Message(key_server, key_pair.getPublic().getEncoded()).src(local_addr)
          .putHeader(this.id,new EncryptHeader(EncryptHeader.SECRET_KEY_REQ, null));
        down_prot.down(new Event(Event.MSG,newMsg));
    }

    protected void sendNewKeyserverAck(Address dest) {
        Message msg=new Message(dest).putHeader(id, new EncryptHeader(EncryptHeader.NEW_KEYSERVER_ACK, null));
        down_prot.down(new Event(Event.MSG, msg));
    }


    protected SecretKeySpec decodeKey(byte[] encodedKey) throws Exception {
        byte[] keyBytes;

        synchronized(this) {
            keyBytes=asym_cipher.doFinal(encodedKey);
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

    protected void startQueueing() {
        queue_lock.lock();
        try {
            queue_up_msgs=true;
        }
        finally {
            queue_lock.unlock();
        }
    }

    protected boolean enqueue(Message msg) {
        queue_lock.lock();
        try {
            return queue_up_msgs && up_queue.offer(msg);
        }
        finally {
            queue_lock.unlock();
        }
    }


    // Drains the queued messages. Doesn't have to be 100% correct: leftover messages wll be delivered later and will
    // be discarded as dupes, as retransmission is likely to have kicked in before, anyway
    protected void stopQueueing() {
        List<Message> sink=new ArrayList<>(up_queue.size());
        queue_lock.lock();
        try {
            queue_up_msgs=false;
            up_queue.drainTo(sink);
        }
        finally {
            queue_lock.unlock();
        }

        for(Message queued_msg: sink) {
            try {
                Message decrypted_msg=decryptMessage(null, queued_msg.copy());
                if(decrypted_msg != null)
                    up_prot.up(new Event(Event.MSG, decrypted_msg));
            }
            catch(Exception ex) {
                log.error("failed decrypting message from %s: %s", queued_msg.src(), ex);
            }
        }
    }


    @Override protected void handleUnknownVersion(byte[] version) {
        if(!isKeyServer()) {
            log.debug("%s: received msg encrypted with version %s (my version: %s), getting new secret key from %s",
                      local_addr, Util.byteArrayToHexString(version), Util.byteArrayToHexString(sym_version), key_server_addr);
            sendKeyRequest(key_server_addr);
        }
    }

    /** Used to reconstitute public key sent in byte form from peer */
    protected PublicKey generatePubKey(byte[] encodedKey) {
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

}
