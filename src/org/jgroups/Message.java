package org.jgroups;

import org.jgroups.util.ByteArray;
import org.jgroups.util.SizeStreamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * A Message is used to send data to members of a group. It contains the address of the sender, the destination address,
 * (typically) a payload, flags and a list of headers.
 * <br/>
 * Subclasses define different types of payloads, e.g. byte arrays, ByteBuffers, Objects etc.
 * @author Bela Ban
 * @since  5.0
 */
public interface Message extends SizeStreamable, Constructable<Message> {

    // The type of the message. Cannot be an enum, as users can register additional types
    short BYTES_MSG        = 0,
      NIO_MSG              = 1,
      EMPTY_MSG            = 2,
      OBJ_MSG              = 3,
      LONG_MSG             = 4,
      COMPOSITE_MSG        = 5,
      FRAG_MSG             = 6,
      EARLYBATCH_MSG       = 7;



    /** Returns the type of the message, e.g. BYTES_MSG, OBJ_MSG etc */
    short                getType();

    /** Returns the destination address to send the message to. A null value sends the message to all cluster members */
    Address              getDest();
    default Address      dest() {return getDest();}

    /** Sets the destination address to send the message to. A null value sends the message to all cluster members */
    Message              setDest(Address new_dest);
    default              Message dest(Address new_dest) {return setDest(new_dest);}

    /** Returns the address of the sender */
    Address              getSrc();
    default Address      src() {return getSrc();}

    /** Sets the address of the sender of this message */
    Message              setSrc(Address new_src);
    default Message      src(Address new_src) {return setSrc(new_src);}

    /** Adds a header to the message */
    Message              putHeader(short id, Header hdr);

    /** Gets a header from the message */
    <T extends Header> T getHeader(short id);

    /** Returns a hashmap of all header IDs and their associated headers */
    Map<Short,Header>    getHeaders();

    /** Returns the number of headers */
    int                  getNumHeaders();

    /** Returns a pretty-printed string of the headers */
    String               printHeaders();

    /** Sets one or more flags */
    Message              setFlag(Flag... flags);


    /** Sets the flags as a short; this way, multiple flags can be set in one operation
     * @param flag The flag to be set (as a short). Overrides existing flags (no xor-ing)
     * @param transient_flags True if the flag is transient, false otherwise
     */
    Message              setFlag(short flag, boolean transient_flags);

    /** Sets one or more transient flags. Transient flags are not marshalled */
    Message              setFlag(TransientFlag... flags);

    /** Atomically sets a transient flag if not set. Returns true if the flags was set, else false (already set) */
    boolean              setFlagIfAbsent(TransientFlag flag);

    /** Returns the flags as an or-ed short
     * @param transient_flags Returns transient flags if true, else regular flags
     */
    short                getFlags(boolean transient_flags);
    default short        getFlags() {return getFlags(false);}

    /** Removes a number of flags from the message. No-op for a flag if it is not set */
    Message              clearFlag(Flag... flags);

    /** Removes a number of transient flags from the message. No-op for a flag if it is not set */
    Message              clearFlag(TransientFlag... flags);

    /** Returns true if a flag is set, false otherwise */
    boolean              isFlagSet(Flag flag);

    /** Returns true if a transient flag is set, false otherwise */
    boolean              isFlagSet(TransientFlag flag);

    /**
     * Copies a message
     * @param copy_payload If true, the payload is copied, else it is null in the copied message
     * @param copy_headers If true, the headers are copied
     */
    Message              copy(boolean copy_payload, boolean copy_headers);

    /**
     * Returns true if the message has a payload, e.g. a byte[] array in a {@link BytesMessage} or an object in
     * an {@link ObjectMessage}. This is more generic than {@link #hasArray()}, as it is not just applicable to
     * a byte array.
     * @return True if the message has a payload
     */
    boolean              hasPayload();


    /** Returns true if this message has a byte[] array as payload (even if it's null!), false otherwise */
    boolean              hasArray();

    /**
     * Returns a <em>reference</em> to the payload (byte array). Note that this array should not be
     * modified as we do not copy the array on copy() or clone(): the array of the copied message
     * is simply a reference to the old array.<br/>
     * Even if offset and length are used: we return the <em>entire</em> array, not a subset.<br/>
     * Throws an exception if the message does not have a byte[] array payload ({@link #hasArray()} is false).<br/>
     * Note that this is a convenience method, as most messages are of type {@link BytesMessage}. It is recommended to
     * downcast a {@link Message} to the correct subtype and use the methods available there to get/set the payload.
     */
    byte[]               getArray();

    /** Returns the offset of the byte[] array at which user data starts. Throws an exception if the message
     * does not have a byte[] array payload (if {@link #hasArray()} is false).<br/>
     * Note that this is a convenience method, as most messages are of type {@link BytesMessage}. */
    int                  getOffset();

    /** Returns the length of the byte[] array payload. If the message does not have a byte[] array payload
     * ({@link #hasArray()} is false), then the serialized size may be returned, or an implementation may choose
     * to throw an exception */
    int                  getLength();

    /**
     * Sets the byte array in a message.<br/>
     * Throws an exception if the message does not have a byte[] array payload ({@link #hasArray()} is false).<br/>
     * Note that this is a convenience method, as most messages are of type {@link BytesMessage}. It is recommended to
     * downcast a {@link Message} to the correct subtype and use the methods available there to get/set the payload.
     */
    Message              setArray(byte[] b, int offset, int length);
    default Message      setArray(byte[] b) {return setArray(b, 0, b.length);}


    /**
     * Sets the byte array in a message.<br/>
     * Throws an exception if the message does not have a byte[] array payload ({@link #hasArray()} is false).<br/>
     * Note that this is a convenience method, as most messages are of type {@link BytesMessage}. It is recommended to
     * downcast a {@link Message} to the correct subtype and use the methods available there to get/set the payload.
     */
    Message              setArray(ByteArray buf);

    /**
     * Gets an object from the payload. If the payload is a byte[] array (e.g. as in {@link BytesMessage}),
     * an attempt to de-serialize the array into an object is made, and the object returned.<br/>
     * If the payload is an object (e.g. as is the case in {@link ObjectMessage}), the object will be returned directly.
     */
    <T extends Object> T getObject();

    /**
     * Sets an object in a message. In a {@link ObjectMessage}, the object is set directly. In a {@link BytesMessage},
     * the object is serialized into a byte[] array and then the array is set as the payload of the message
     */
    Message              setObject(Object obj);

    /**
     * Returns the payload without any conversion (e.g. as in {@link #getObject()} or {@link #getArray()})
     * @param <T> The type of the object
     * @return The payload
     */
    <T extends Object> T getPayload();

    /**
     * Sets the payload
     * @param pl The paylolad
     */
    Message              setPayload(Object pl);


    /**
     * Returns the exact size of the marshalled message
     * @return The number of bytes for the marshalled message
     */
    int                  size();

    /** Writes the message to an output stream excluding the destination (and possibly source) address, plus a number of headers */
    void                 writeToNoAddrs(Address src, DataOutput out, short... excluded_headers) throws IOException;

    void                 writePayload(DataOutput out) throws IOException;

    void                 readPayload(DataInput in) throws IOException, ClassNotFoundException;


    // =============================== Flags ====================================
    enum Flag {
        OOB((short)            1),           // message is out-of-band
        DONT_BUNDLE(   (short)(1 <<  1)),    // don't bundle message at the transport
        NO_FC(         (short)(1 <<  2)),    // bypass flow control
        NO_RELIABILITY((short)(1 <<  4)),    // bypass UNICAST(2) and NAKACK
        NO_TOTAL_ORDER((short)(1 <<  5)),    // bypass total order (e.g. SEQUENCER)
        NO_RELAY(      (short)(1 <<  6)),    // bypass relaying (RELAY)
        RSVP(          (short)(1 <<  7)),    // ack of a multicast (https://issues.jboss.org/browse/JGRP-1389)
        RSVP_NB(       (short)(1 <<  8)),    // non blocking RSVP
        SKIP_BARRIER(  (short)(1 << 10)),    // passing messages through a closed BARRIER
        SERIALIZED(    (short)(1 << 11));    // used by BytesMessage/NioMessage (internal flag)

        final short value;
        Flag(short value) {this.value=value;}


        public short value() {return value;}
    }

    // =========================== Transient flags ==============================
    enum TransientFlag {
        OOB_DELIVERED( (short)(1)),
        DONT_LOOPBACK( (short)(1 << 1));   // don't loop back up if this flag is set and it is a multicast message

        final short value;
        TransientFlag(short flag) {value=flag;}


        public short value() {return value;}
    }
}
