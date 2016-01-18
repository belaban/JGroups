package org.jgroups.auth;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.AUTH;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * AuthToken implementation which shows how to send messages back and forth in order to perform authentication. Could
 * be used as a template for a challenge-response based AuthToken impl.
 * @author Bela Ban
 * @since  3.3
 */
public class DemoToken extends AuthToken implements AUTH.UpHandler {
    protected static final short ID=1555; // the ID to fetch a DemoHeader from a message

    @Property(description="How long to wait (in ms) for a response to a challenge")
    protected long block_time=5000;

    // Used to correlate pending challenge requests sent with responses received
    protected final Map<Address,Entry> pending_requests=new HashMap<>();


    static {
        ClassConfigurator.add((short)1555, DemoHeader.class);
    }


    public String getName() {return "org.jgroups.auth.DemoToken";}
    public void   init()    {auth.register(this);}


    public boolean authenticate(AuthToken token, Message msg) {
        Address sender=msg.getSrc();

        // 1. send a challenge to the sender
        Message    challenge=new Message(sender).setFlag(Message.Flag.OOB);
        byte[]     buf=generateRandomBytes();
        DemoHeader hdr=new DemoHeader(buf);
        challenge.putHeader(ID, hdr);

        Entry entry=new Entry(buf);
        pending_requests.put(sender, entry); // here we'd have to check if a latch already exists...
        if(log.isTraceEnabled())
            log.trace(auth.getAddress() + ": sending challenge to " + sender);
        auth.getDownProtocol().down(new Event(Event.MSG, challenge));
        try {
            entry.latch.await(block_time, TimeUnit.MILLISECONDS);
            pending_requests.remove(sender);
            boolean result=entry.hash > 0 && entry.hash == hash(encrypt(entry.challenge));
            if(log.isTraceEnabled())
                log.trace(auth.getAddress() + ": authentication of " + sender + ": " + result + " (hash=" + entry.hash + ")");
            return result;
        }
        catch(InterruptedException e) {
            return false;
        }
    }

    public void writeTo(DataOutput out) throws Exception {}
    public void readFrom(DataInput in) throws Exception {}
    public int  size() {return 0;}


    public boolean handleUpEvent(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                DemoHeader hdr=(DemoHeader)msg.getHeader(ID);
                if(hdr == null)
                    break;
                switch(hdr.type) {
                    case DemoHeader.CHALLENGE:
                        if(log.isTraceEnabled())
                            log.trace(auth.getAddress() + ": received CHALLENGE from " + msg.getSrc());
                        long hash=hash(encrypt(hdr.payload));
                        Message response=new Message(msg.getSrc()).setFlag(Message.Flag.OOB);
                        response.putHeader(ID, new DemoHeader(hash));
                        if(log.isTraceEnabled())
                            log.trace(auth.getAddress() + ": sending RESPONSE to " + msg.getSrc());
                        auth.getDownProtocol().down(new Event(Event.MSG, response));
                        break;
                    case DemoHeader.RESPONSE:
                        if(log.isTraceEnabled())
                            log.trace(auth.getAddress() + ": received RESPONSE from " + msg.getSrc());
                        Entry entry=pending_requests.get(msg.getSrc());
                        if(entry == null) {
                            // error message
                            break;
                        }
                        entry.setResponse(hdr.hash);
                        break;
                }
                return false; // don't pass up
        }
        return true;
    }


    protected static byte[] generateRandomBytes() {
        byte[] retval=new byte[10]; // here we'd have to generate a buffer with random contents
        for(int i=0; i < retval.length; i++)
            retval[i]=(byte)Util.random(Byte.MAX_VALUE);
        return retval;
    }

    protected static byte[] encrypt(byte[] buf) {
        return  buf; // here real encryption would have to be provided...
    }

    // A real hash would have to be provided here...
    protected static long hash(byte[] buf) {
        long retval=0;
        for(int i=0; i < buf.length; i++)
            retval+=buf[i];
        return retval;
    }


    protected static class Entry {
        protected final CountDownLatch latch=new CountDownLatch(1);
        protected final byte[]         challenge;
        protected long                 hash; // set by response

        public Entry(byte[] challenge) {
            this.challenge=challenge;
        }

        public void setResponse(long hash) {
            this.hash=hash;
            latch.countDown();
        }
    }


    public static class DemoHeader extends Header {
        protected static final byte CHALLENGE = 1;
        protected static final byte RESPONSE  = 2;

        protected byte   type;
        protected byte[] payload;  // CHALLENGE
        protected long   hash;     // RESPONSE

        public DemoHeader() {
        }

        public DemoHeader(byte[] payload) {
            this.type=CHALLENGE;
            this.payload=payload;
        }

        public DemoHeader(long hash) {
            type=RESPONSE;
            this.hash=hash;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            switch(type) {
                case CHALLENGE:
                    Util.writeByteBuffer(payload, out);
                    break;
                case RESPONSE:
                    out.writeLong(hash);
                    break;
            }
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            switch(type) {
                case CHALLENGE:
                    payload=Util.readByteBuffer(in);
                    break;
                case RESPONSE:
                    hash=in.readLong();
                    break;
            }
        }

        public int size() {
            int retval=Global.BYTE_SIZE; // type
            switch(type) {
                case CHALLENGE:
                    retval+=Util.size(payload);
                    break;
                case RESPONSE:
                    retval+=Global.LONG_SIZE;
                    break;
            }
            return retval;
        }

        public String toString() {
            return type == CHALLENGE? "CHALLENGE" : "RESPONSE" + ", payload=" + (payload != null? payload.length : 0) + " bytes";
        }
    }
}
