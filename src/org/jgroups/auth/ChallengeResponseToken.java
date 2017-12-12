package org.jgroups.auth;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.ASYM_ENCRYPT;
import org.jgroups.protocols.AUTH;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

/**
 * AuthToken implementation which shows how to do challenge-response based authentication. This could for example be
 * used by a coordinator A to authenticate joiner D.<p/>
 * When D wants to join, it sends a join request to A. A sends a challenge to D, which transforms the challenge into a
 * response and sends the hash of the response back to A.<p/>
 * A uses the same algorithm to transform the challenge, hashes the result and compares it to the hash received by
 * D. If they're equal, D is authenticated successfully.<p/>
 * The algorithm to transform the challenge into a response is {@link #encrypt(byte[])}.<p/
 * To prevent man-in-the-middle attacks, the public key of A is sent with the challenge and D encrypts the response
 * with it. On reception of the response, A uses its private key to decrypt it.<p/>
 * Note that  challenge-response authentication does not prevent MIM attacks, see
 * <a href="https://en.wikipedia.org/wiki/Man-in-the-middle_attack">Wikipedia</a>.
 * @author Bela Ban
 * @since  3.3
 */
@SuppressWarnings("unused")
public class ChallengeResponseToken extends AuthToken implements AUTH.UpHandler {
    protected static final short       ID=1555; // the ID to fetch a ChallengeResponseHeader from a message

    @Property(description="How long to wait (in ms) for a response to a challenge")
    protected long                     block_time=5000;

    @Property(description="Number of bytes in a challenge")
    protected int                      challenge_size=16;

    // Used to correlate pending challenge requests sent with responses received
    protected final Map<Address,Entry> pending_requests=new HashMap<>();

    protected static final BiPredicate<Message,Boolean> BYPASSER_FUNCTION=(msg,up) -> {
        ChallengeResponseHeader hdr=msg.getHeader(ID);
        return hdr != null && (hdr.type == ChallengeResponseHeader.CHALLENGE || hdr.type == ChallengeResponseHeader.RESPONSE);
    };


    public String getName() {return ChallengeResponseToken.class.getName();}

    public void init()    {
        auth.register(this);
        registerBypasser(auth);
    }

    public void destroy() {
        super.destroy();
        unregisterBypasser(auth);
    }

    public boolean authenticate(AuthToken token, Message msg) {
        Address sender=msg.getSrc();

        // 1. send a challenge to the sender
        byte[] buf=generateRandomBytes(challenge_size);
        Message challenge=new Message(sender).setFlag(Message.Flag.OOB)
          .putHeader(ID, new ChallengeResponseHeader(buf));

        Entry entry=new Entry(buf);
        pending_requests.put(sender, entry); // here we'd have to check if a latch already exists...
        log.trace("%s: sending challenge to %s", auth.getAddress(), sender);

        try {
            auth.getDownProtocol().down(challenge);
            long hash=entry.future.get(block_time, TimeUnit.MILLISECONDS);
            boolean result=hash > 0 && hash == hash(encrypt(entry.challenge));
            log.trace("%s: authentication of %s: %b (hash=%d)", auth.getAddress(), sender, result, hash);
            return result;
        }
        catch(Exception e) {
            return false;
        }
        finally {
            pending_requests.remove(sender);
        }
    }

    @Override
    public void writeTo(DataOutput out) {}
    @Override
    public void readFrom(DataInput in) {}
    public int  size() {return 0;}


    public boolean handleUpMessage(Message msg) {
        ChallengeResponseHeader hdr=msg.getHeader(ID);
        if(hdr == null)
            return true;
        switch(hdr.type) {
            case ChallengeResponseHeader.CHALLENGE:
                long hash=hash(encrypt(hdr.payload));
                Message response=new Message(msg.getSrc()).setFlag(Message.Flag.OOB)
                  .putHeader(ID, new ChallengeResponseHeader(hash));
                log.trace("%s: received CHALLENGE from %s; sending RESPONSE (hash=%d)", auth.getAddress(), msg.src(), hash);
                auth.getDownProtocol().down(response);
                break;
            case ChallengeResponseHeader.RESPONSE:
                log.trace("%s: received RESPONSE from %s", auth.getAddress(), msg.getSrc());
                Entry entry=pending_requests.get(msg.getSrc());
                if(entry != null)
                    entry.setResponse(hdr.hash);
                break;
        }
        return false; // don't pass up
    }

    protected static void registerBypasser(AUTH auth) {
        ASYM_ENCRYPT asym_encr=auth.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
        if(asym_encr != null)
            asym_encr.registerBypasser(BYPASSER_FUNCTION);
    }

    protected static void unregisterBypasser(AUTH auth) {
        ASYM_ENCRYPT asym_encr=auth.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
        if(asym_encr != null)
            asym_encr.unregisterBypasser(BYPASSER_FUNCTION);
    }


    protected static byte[] generateRandomBytes(int size) {
        byte[] retval=new byte[size]; // here we'd have to generate a buffer with random contents
        for(int i=0; i < retval.length; i++)
            retval[i]=(byte)Util.random(Byte.MAX_VALUE);
        return retval;
    }

    /**
     * Simplistic example of a transformation by incrementing each byte in the array.
     */
    protected static byte[] encrypt(byte[] buf) {
        for(int i=0; i < buf.length; i++)
            buf[i]=(byte)(buf[i]+1);
        return buf;
    }

    // A real hash would have to be provided here...
    protected static long hash(byte[] buf) {
        long retval=0;
        for(int i=0; i < buf.length; i++)
            retval+=buf[i];
        return retval;
    }


    protected static class Entry {
        protected final CompletableFuture<Long> future=new CompletableFuture<>();
        protected final byte[]                  challenge;

        public Entry(byte[] challenge) {
            this.challenge=challenge;
        }

        public void setResponse(long hash) {
            future.complete(hash);
        }
    }

}
