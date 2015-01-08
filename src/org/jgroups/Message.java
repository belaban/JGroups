
package org.jgroups;


import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Map;

/**
 * A Message encapsulates data sent to members of a group. It contains among other things the
 * address of the sender, the destination address, a payload (byte buffer) and a list of headers.
 * Headers are added by protocols on the sender side and removed by protocols on the receiver's
 * side.
 * <p>
 * The byte buffer can point to a reference, and we can subset it using index and length. However,
 * when the message is serialized, we only write the bytes between index and length.
 *
 * @since 2.0
 * @author Bela Ban
 */
public class Message implements Streamable {
    protected Address          dest_addr;
    protected Address          src_addr;

    /** The payload */
    protected byte[]           buf;

    /** The index into the payload (usually 0) */
    protected int              offset;

    /** The number of bytes in the buffer (usually buf.length is buf not equal to null). */
    protected int              length;

    /** All headers are placed here */
    protected Headers          headers;

    protected volatile short   flags;

    protected volatile byte    transient_flags; // transient_flags is neither marshalled nor copied



    static final byte DEST_SET         =  1;
    static final byte SRC_SET          =  1 << 1;
    static final byte BUF_SET          =  1 << 2;


    // =============================== Flags ====================================
    public static enum Flag {
        OOB((short)            1),           // message is out-of-band
        DONT_BUNDLE(   (short)(1 <<  1)),    // don't bundle message at the transport
        NO_FC(         (short)(1 <<  2)),    // bypass flow control
        SCOPED(        (short)(1 <<  3)),    // when a message has a scope
        NO_RELIABILITY((short)(1 <<  4)),    // bypass UNICAST(2) and NAKACK
        NO_TOTAL_ORDER((short)(1 <<  5)),    // bypass total order (e.g. SEQUENCER)
        NO_RELAY(      (short)(1 <<  6)),    // bypass relaying (RELAY)
        RSVP(          (short)(1 <<  7)),    // ack of a multicast (https://issues.jboss.org/browse/JGRP-1389)
        RSVP_NB(       (short)(1 <<  8)),    // non blocking RSVP
        INTERNAL(      (short)(1 <<  9)),    // for internal use by JGroups only, don't use !
        SKIP_BARRIER(  (short)(1 << 10));    // passing messages through a closed BARRIER


        final short value;
        Flag(short value) {this.value=value;}

        public short value() {return value;}
    }

    @Deprecated public static final Flag OOB=Flag.OOB;
    @Deprecated public static final Flag DONT_BUNDLE=Flag.DONT_BUNDLE;
    @Deprecated public static final Flag NO_FC=Flag.NO_FC;
    @Deprecated public static final Flag SCOPED=Flag.SCOPED;
    @Deprecated public static final Flag NO_RELIABILITY=Flag.NO_RELIABILITY;
    @Deprecated public static final Flag NO_TOTAL_ORDER=Flag.NO_TOTAL_ORDER;
    @Deprecated public static final Flag NO_RELAY=Flag.NO_RELAY;
    @Deprecated public static final Flag RSVP=Flag.RSVP;



    // =========================== Transient flags ==============================
    public static enum TransientFlag {
        OOB_DELIVERED( (short)(1)),
        DONT_LOOPBACK( (short)(1 << 1));   // don't loop back up if this flag is set and it is a multicast message

        final short value;
        TransientFlag(short flag) {value=flag;}

        public short value() {return value;}
    }

    @Deprecated
    public static final TransientFlag OOB_DELIVERED=TransientFlag.OOB_DELIVERED; // OOB which has already been delivered up the stack




   /**
    * Constructs a Message given a destination Address
    *
    * @param dest
    *           Address of receiver. If it is <em>null</em> then the message sent to the group.
    *           Otherwise, it contains a single destination and is sent to that member.
    *           <p>
    */
    public Message(Address dest) {
        setDest(dest);
        headers=createHeaders(3);
    }

   /**
    * Constructs a Message given a destination Address, a source Address and the payload byte buffer
    *
    * @param dest
    *           Address of receiver. If it is <em>null</em> then the message sent to the group.
    *           Otherwise, it contains a single destination and is sent to that member.
    *           <p>
    * @param src
    *           Address of sender
    * @param buf
    *           Message to be sent. Note that this buffer must not be modified (e.g. buf[0]=0 is not
    *           allowed), since we don't copy the contents on clopy() or clone().
    */
    public Message(Address dest, Address src, byte[] buf) {
        this(dest);
        setSrc(src);
        setBuffer(buf);
    }

    public Message(Address dest, byte[] buf) {
        this(dest, null, buf);
    }


   /**
    * Constructs a message. The index and length parameters allow to provide a <em>reference</em> to
    * a byte buffer, rather than a copy, and refer to a subset of the buffer. This is important when
    * we want to avoid copying. When the message is serialized, only the subset is serialized.<br/>
    * <em>
    * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
    * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
    * retransmit a changed byte[] buffer !
    * </em>
    *
    * @param dest
    *           Address of receiver. If it is <em>null</em> then the message sent to the group.
    *           Otherwise, it contains a single destination and is sent to that member.
    *           <p>
    * @param src
    *           Address of sender
    * @param buf
    *           A reference to a byte buffer
    * @param offset
    *           The index into the byte buffer
    * @param length
    *           The number of bytes to be used from <tt>buf</tt>. Both index and length are checked
    *           for array index violations and an ArrayIndexOutOfBoundsException will be thrown if
    *           invalid
    */
    public Message(Address dest, Address src, byte[] buf, int offset, int length) {
        this(dest);
        setSrc(src);
        setBuffer(buf, offset, length);
    }

    public Message(Address dest, byte[] buf, int offset, int length) {
        this(dest, null, buf, offset, length);
    }


   /**
    * Constructs a Message given a destination Address, a source Address and the payload Object
    *
    * @param dest
    *           Address of receiver. If it is <em>null</em> then the message sent to the group.
    *           Otherwise, it contains a single destination and is sent to that member.
    *           <p>
    * @param src
    *           Address of sender
    * @param obj
    *           The object will be marshalled into the byte buffer.
    *           <em>Obj has to be serializable (e.g. implementing
    *              Serializable, Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).</em>
    *           ! The resulting buffer must not be modified (e.g. buf[0]=0 is not allowed), since we
    *           don't copy the contents on clopy() or clone().
    *           <p/>
    */
    public Message(Address dest, Address src, Object obj) {
        this(dest);
        setSrc(src);
        setObject(obj);
    }

    public Message(Address dest, Object obj) {
        this(dest, null, obj);
    }


    public Message() {
        headers=createHeaders(3);
    }


    public Message(boolean create_headers) {
        if(create_headers)
            headers=createHeaders(3);
    }

    public Address getDest()                 {return dest_addr;}
    public Address dest()                    {return dest_addr;}
    public void    setDest(Address new_dest) {dest_addr=new_dest;}
    public Message dest(Address new_dest)    {dest_addr=new_dest; return this;}
    public Address getSrc()                  {return src_addr;}
    public Address src()                     {return src_addr;}
    public void    setSrc(Address new_src)   {src_addr=new_src;}
    public Message src(Address new_src)      {src_addr=new_src; return this;}

   /**
    * Returns a <em>reference</em> to the payload (byte buffer). Note that this buffer should not be
    * modified as we do not copy the buffer on copy() or clone(): the buffer of the copied message
    * is simply a reference to the old buffer.<br/>
    * Even if offset and length are used: we return the <em>entire</em> buffer, not a subset.
    */
   public byte[] getRawBuffer() {
        return buf;
    }

   /**
    * Returns a copy of the buffer if offset and length are used, otherwise a reference.
    *
    * @return byte array with a copy of the buffer.
    */
    final public byte[] getBuffer() {
        if(buf == null)
            return null;
        if(offset == 0 && length == buf.length)
            return buf;
        else {
            byte[] retval=new byte[length];
            System.arraycopy(buf, offset, retval, 0, length);
            return retval;
        }
    }

    /**
     * <em>
     * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
     * retransmit a changed byte[] buffer !
     * </em>
     */
    final public Message setBuffer(byte[] b) {
        buf=b;
        if(buf != null) {
            offset=0;
            length=buf.length;
        }
        else
            offset=length=0;
        return this;
    }

    /**
     * Sets the internal buffer to point to a subset of a given buffer.<p/>
     * <em>
     * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
     * retransmit a changed byte[] buffer !
     * </em>
     *
     * @param b The reference to a given buffer. If null, we'll reset the buffer to null
     * @param offset The initial position
     * @param length The number of bytes
     */
    final public Message setBuffer(byte[] b, int offset, int length) {
        buf=b;
        if(buf != null) {
            if(offset < 0 || offset > buf.length)
                throw new ArrayIndexOutOfBoundsException(offset);
            if((offset + length) > buf.length)
                throw new ArrayIndexOutOfBoundsException((offset+length));
            this.offset=offset;
            this.length=length;
        }
        else
            this.offset=this.length=0;
        return this;
    }

    /**
     * <em>
     * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
     * retransmit a changed byte[] buffer !
     * </em>
     */
    public final Message setBuffer(Buffer buf) {
        if(buf != null) {
            this.buf=buf.getBuf();
            this.offset=buf.getOffset();
            this.length=buf.getLength();
        }
        return this;
    }

    /**
     *
     * Returns the offset into the buffer at which the data starts
     *
     */
    public int getOffset() {
        return offset;
    }

   /**
    *
    * Returns the number of bytes in the buffer
    *
    */
    public int getLength() {
        return length;
    }

   /**
    * Returns a reference to the headers hashmap, which is <em>immutable</em>. Any attempt to modify
    * the returned map will cause a runtime exception
    */
    public Map<Short,Header> getHeaders() {
        return headers.getHeaders();
    }

    public String printHeaders() {
        return headers.printHeaders();
    }

    public int getNumHeaders() {
        return headers != null? headers.size() : 0;
    }

    /**
     * Takes an object and uses Java serialization to generate the byte[] buffer which is set in the
     * message. Parameter 'obj' has to be serializable (e.g. implementing Serializable,
     * Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).
     */
    final public Message setObject(Object obj) {
        if(obj == null) return this;
        if(obj instanceof byte[])
            return setBuffer((byte[])obj);
        if(obj instanceof Buffer)
            return setBuffer((Buffer)obj);
        try {
            return setBuffer(Util.objectToByteBuffer(obj));
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }


    final public Object getObject() {
        return getObject(null);
    }

    /**
     * Uses custom serialization to create an object from the buffer of the message. Note that this
     * is dangerous when using your own classloader, e.g. inside of an application server ! Most
     * likely, JGroups will use the system classloader to deserialize the buffer into an object,
     * whereas (for example) a web application will want to use the webapp's classloader, resulting
     * in a ClassCastException. The recommended way is for the application to use their own
     * serialization and only pass byte[] buffer to JGroups.<p/>
     * As of 3.5, a classloader can be passed in. It will be used first to find a class, before contacting
     * the other classloaders in the list. If null, the default list of classloaders will be used.
     *
     * @return the object
     */
    final public Object getObject(ClassLoader loader) {
        try {
            return Util.objectFromByteBuffer(buf, offset, length, loader);
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Sets a number of flags in a message
     * @param flags The flag or flags
     * @return A reference to the message
     */
    public Message setFlag(Flag ... flags) {
        if(flags != null)
            for(Flag flag: flags)
                if(flag != null)
                    this.flags |= flag.value();
        return this;
    }

    /**
     * Same as {@link #setFlag(Flag...)} except that transient flags are not marshalled
     * @param flags The flag
     */
    public Message setTransientFlag(TransientFlag ... flags) {
        if(flags != null)
            for(TransientFlag flag: flags)
                if(flag != null)
                    transient_flags|=flag.value();
        return this;
    }

    /**
     * Sets the flags from a short. <em>Not recommended</em> (use {@link #setFlag(org.jgroups.Message.Flag...)} instead),
     * as the internal representation of flags might change anytime.
     * @param flag
     * @return
     */
    public Message setFlag(short flag) {
        flags |= flag;
        return this;
    }

    public Message setTransientFlag(short flag) {
        transient_flags |= flag;
        return this;
    }


    /**
     * Returns the internal representation of flags. Don't use this, as the internal format might change at any time !
     * This is only used by unit test code
     * @return
     */
    public short getFlags()          {return flags;}
    public short getTransientFlags() {return transient_flags;}

    /**
     * Clears a number of flags in a message
     * @param flags The flags
     * @return A reference to the message
     */
    public Message clearFlag(Flag ... flags) {
        if(flags != null)
            for(Flag flag: flags)
                if(flag != null)
                    this.flags &= ~flag.value();
        return this;
    }

    public Message clearTransientFlag(TransientFlag ... flags) {
        if(flags != null)
            for(TransientFlag flag: flags)
                if(flag != null)
                    transient_flags &= ~flag.value();
        return this;
    }

    public static boolean isFlagSet(short flags, Flag flag) {
        return flag != null && ((flags & flag.value()) == flag.value());
    }

    /**
     * Checks if a given flag is set
     * @param flag The flag
     * @return Whether or not the flag is curently set
     */
    public boolean isFlagSet(Flag flag) {
        return isFlagSet(flags, flag);
    }

    public static boolean isTransientFlagSet(short flags, TransientFlag flag) {
        return flag != null && (flags & flag.value()) == flag.value();
    }

    public boolean isTransientFlagSet(TransientFlag flag) {
        return isTransientFlagSet(transient_flags, flag);
    }

    /**
    * Atomically checks if a given flag is set and - if not - sets it. When multiple threads
    * concurrently call this method with the same flag, only one of them will be able to set the
    * flag
    *
    * @param flag
    * @return True if the flag could be set, false if not (was already set)
    */
    public synchronized boolean setTransientFlagIfAbsent(TransientFlag flag) {
        if(isTransientFlagSet(flag))
            return false;
        setTransientFlag(flag);
        return true;
    }


    public Message setScope(short scope) {
        Util.setScope(this, scope);
        return this;
    }

    public short getScope() {
        return Util.getScope(this);
    }

    /*---------------------- Used by protocol layers ----------------------*/

    /** Puts a header given an ID into the hashmap. Overwrites potential existing entry. */
    public Message putHeader(short id, Header hdr) {
        if(id < 0)
            throw new IllegalArgumentException("An ID of " + id + " is invalid");
        headers.putHeader(id, hdr);
        return this;
    }

   /**
    * Puts a header given a key into the map, only if the key doesn't exist yet
    *
    * @param id
    * @param hdr
    * @return the previous value associated with the specified key, or <tt>null</tt> if there was no
    *         mapping for the key. (A <tt>null</tt> return can also indicate that the map previously
    *         associated <tt>null</tt> with the key, if the implementation supports null values.)
    */
    public Header putHeaderIfAbsent(short id, Header hdr) {
        if(id <= 0)
            throw new IllegalArgumentException("An ID of " + id + " is invalid");
        return headers.putHeaderIfAbsent(id, hdr);
    }


    public Header getHeader(short id) {
        if(id <= 0)
            throw new IllegalArgumentException("An ID of " + id + " is invalid. Add the protocol which calls " +
                                                 "getHeader() to jg-protocol-ids.xml");
        return headers.getHeader(id);
    }
    /*---------------------------------------------------------------------*/


    public Message copy() {
        return copy(true);
    }

   /**
    * Create a copy of the message. If offset and length are used (to refer to another buffer), the
    * copy will contain only the subset offset and length point to, copying the subset into the new
    * copy.
    *
    * @param copy_buffer
    * @return Message with specified data
    */
    public Message copy(boolean copy_buffer) {
        return copy(copy_buffer, true);
    }

   /**
    * Create a copy of the message. If offset and length are used (to refer to another buffer), the
    * copy will contain only the subset offset and length point to, copying the subset into the new
    * copy.<p/>
    * Note that for headers, only the arrays holding references to the headers are copied, not the headers themselves !
    * The consequence is that the headers array of the copy hold the *same* references as the original, so do *not*
    * modify the headers ! If you want to change a header, copy it and call {@link Message#putHeader(short,Header)} again.
    *
    * @param copy_buffer
    * @param copy_headers
    *           Copy the headers
    * @return Message with specified data
    */
    public Message copy(boolean copy_buffer, boolean copy_headers) {
        Message retval=new Message(false);
        retval.dest_addr=dest_addr;
        retval.src_addr=src_addr;
        retval.flags=flags;
        retval.transient_flags=transient_flags;

        if(copy_buffer && buf != null) {

            // change bela Feb 26 2004: we don't resolve the reference
            retval.setBuffer(buf, offset, length);
        }

        retval.headers=copy_headers? createHeaders(headers) : createHeaders(3);
        return retval;
    }

   /**
    * Doesn't copy any headers except for those with ID >= copy_headers_above
    *
    * @param copy_buffer
    * @param starting_id
    * @return A message with headers whose ID are >= starting_id
    */
    public Message copy(boolean copy_buffer, short starting_id) {
        return copy(copy_buffer, starting_id, (short[])null);
    }

    /**
     * Copies a message. Copies only headers with IDs >= starting_id or IDs which are in the copy_only_ids list
     * @param copy_buffer
     * @param starting_id
     * @param copy_only_ids
     * @return
     */
    public Message copy(boolean copy_buffer, short starting_id, short ... copy_only_ids) {
        Message retval=copy(copy_buffer, false);
        for(Map.Entry<Short,Header> entry: getHeaders().entrySet()) {
            short id=entry.getKey();
            if(id >= starting_id || Util.containsId(id, copy_only_ids))
                retval.putHeader(id, entry.getValue());
        }
        return retval;
    }


    public Message makeReply() {
        Message retval=new Message(src_addr);
        if(dest_addr != null)
            retval.setSrc(dest_addr);
        return retval;
    }


    public String toString() {
        StringBuilder ret=new StringBuilder(64);
        ret.append("[dst: ");
        if(dest_addr == null)
            ret.append("<null>");
        else
            ret.append(dest_addr);
        ret.append(", src: ");
        if(src_addr == null)
            ret.append("<null>");
        else
            ret.append(src_addr);

        int size;
        if((size=getNumHeaders()) > 0)
            ret.append(" (").append(size).append(" headers)");

        ret.append(", size=");
        if(buf != null && length > 0)
            ret.append(length);
        else
            ret.append('0');
        ret.append(" bytes");
        if(flags > 0)
            ret.append(", flags=").append(flagsToString(flags));
        if(transient_flags > 0)
            ret.append(", transient_flags=" + transientFlagsToString(transient_flags));
        ret.append(']');
        return ret.toString();
    }




    /** Tries to read an object from the message's buffer and prints it */
    public String toStringAsObject() {
        if(buf == null) return null;
        try {
            Object obj=getObject();
            return obj != null ? obj.toString() : "";
        }
        catch(Exception e) {  // it is not an object
            return "";
        }
    }



    public String printObjectHeaders() {
        return headers.printObjectHeaders();
    }



    /* ----------------------------------- Interface Streamable  ------------------------------- */

    /**
     * Streams all members (dest and src addresses, buffer and headers) to the output stream.
     *
     *
     * @param out
     * @throws Exception
     */
    public void writeTo(DataOutput out) throws Exception {
        byte leading=0;

        if(dest_addr != null)
            leading=Util.setFlag(leading, DEST_SET);

        if(src_addr != null)
            leading=Util.setFlag(leading, SRC_SET);

        if(buf != null)
            leading=Util.setFlag(leading, BUF_SET);

        // 1. write the leading byte first
        out.write(leading);

        // 2. the flags (e.g. OOB, LOW_PRIO), skip the transient flags
        out.writeShort(flags);

        // 3. dest_addr
        if(dest_addr != null)
            Util.writeAddress(dest_addr, out);

        // 4. src_addr
        if(src_addr != null)
            Util.writeAddress(src_addr, out);

        // 5. headers
        int size=headers.size();
        out.writeShort(size);
        final short[]  ids=headers.getRawIDs();
        final Header[] hdrs=headers.getRawHeaders();
        for(int i=0; i < ids.length; i++) {
            if(ids[i] > 0) {
                out.writeShort(ids[i]);
                writeHeader(hdrs[i], out);
            }
        }

        // 6. buf
        if(buf != null) {
            out.writeInt(length);
            out.write(buf, offset, length);
        }
    }

   /**
    * Writes the message to the output stream, but excludes the dest and src addresses unless the
    * src address given as argument is different from the message's src address
    *
    * @param src
    * @param out
    * @param excluded_headers Don't marshal headers that are part of excluded_headers
    * @throws Exception
    */
    public void writeToNoAddrs(Address src, DataOutput out, short ... excluded_headers) throws Exception {
        byte leading=0;

        boolean write_src_addr=src == null || src_addr != null && !src_addr.equals(src);

        if(write_src_addr)
            leading=Util.setFlag(leading, SRC_SET);

        if(buf != null)
            leading=Util.setFlag(leading, BUF_SET);

        // 1. write the leading byte first
        out.write(leading);

        // 2. the flags (e.g. OOB, LOW_PRIO)
        out.writeShort(flags);

        // 4. src_addr
        if(write_src_addr)
            Util.writeAddress(src_addr, out);

        // 5. headers
        int size=headers.size(excluded_headers);
        out.writeShort(size);
        final short[]  ids=headers.getRawIDs();
        final Header[] hdrs=headers.getRawHeaders();
        for(int i=0; i < ids.length; i++) {
            if(ids[i] > 0) {
                if(excluded_headers != null && Util.containsId(ids[i], excluded_headers))
                    continue;
                out.writeShort(ids[i]);
                writeHeader(hdrs[i], out);
            }
        }

        // 6. buf
        if(buf != null) {
            out.writeInt(length);
            out.write(buf, offset, length);
        }
    }


    public void readFrom(DataInput in) throws Exception {

        // 1. read the leading byte first
        byte leading=in.readByte();

        // 2. the flags
        flags=in.readShort();

        // 3. dest_addr
        if(Util.isFlagSet(leading, DEST_SET))
            dest_addr=Util.readAddress(in);

        // 4. src_addr
        if(Util.isFlagSet(leading, SRC_SET))
            src_addr=Util.readAddress(in);

        // 5. headers
        int len=in.readShort();
        headers=createHeaders(len);

        short[]  ids=headers.getRawIDs();
        Header[] hdrs=headers.getRawHeaders();

        for(int i=0; i < len; i++) {
            short id=in.readShort();
            Header hdr=readHeader(in);
            ids[i]=id;
            hdrs[i]=hdr;
        }

        // 6. buf
        if(Util.isFlagSet(leading, BUF_SET)) {
            len=in.readInt();
            buf=new byte[len];
            in.readFully(buf, 0, len);
            length=len;
        }
    }


    /** Reads the message's contents from an input stream, but skips the buffer and instead returns the
     * position (offset) at which the buffer starts */
    public int readFromSkipPayload(ByteArrayDataInputStream in) throws Exception {

        // 1. read the leading byte first
        byte leading=in.readByte();

        // 2. the flags
        flags=in.readShort();

        // 3. dest_addr
        if(Util.isFlagSet(leading, DEST_SET))
            dest_addr=Util.readAddress(in);

        // 4. src_addr
        if(Util.isFlagSet(leading, SRC_SET))
            src_addr=Util.readAddress(in);

        // 5. headers
        int len=in.readShort();
        headers=createHeaders(len);

        short[]  ids=headers.getRawIDs();
        Header[] hdrs=headers.getRawHeaders();

        for(int i=0; i < len; i++) {
            short id=in.readShort();
            Header hdr=readHeader(in);
            ids[i]=id;
            hdrs[i]=hdr;
        }

        // 6. buf
        if(!Util.isFlagSet(leading, BUF_SET))
            return -1;

        length=in.readInt();
        return in.position();
    }

    /* --------------------------------- End of Interface Streamable ----------------------------- */

    /**
     * Returns the exact size of the marshalled message. Uses method size() of each header to compute
     * the size, so if a Header subclass doesn't implement size() we will use an approximation.
     * However, most relevant header subclasses have size() implemented correctly. (See
     * org.jgroups.tests.SizeTest).
     *
    * @return The number of bytes for the marshalled message
    */
    public long size() {
        long retval=Global.BYTE_SIZE   // leading byte
                + Global.SHORT_SIZE;   // flags
        if(dest_addr != null)
            retval+=Util.size(dest_addr);
        if(src_addr != null)
            retval+=Util.size(src_addr);

        retval+=Global.SHORT_SIZE;  // number of headers
        retval+=headers.marshalledSize();

        if(buf != null)
            retval+=Global.INT_SIZE // length (integer)
              + length;       // number of bytes in the buffer
        return retval;
    }



    /* ----------------------------------- Private methods ------------------------------- */

    public static String flagsToString(short flags) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;

        Flag[] all_flags=Flag.values();
        for(Flag flag: all_flags) {
            if(isFlagSet(flags, flag)) {
                if(first)
                    first=false;
                else
                    sb.append("|");
                sb.append(flag);
            }
        }
        return sb.toString();
    }


    public static String transientFlagsToString(short flags) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;

        TransientFlag[] all_flags=TransientFlag.values();
        for(TransientFlag flag: all_flags) {
            if(isTransientFlagSet(flags, flag)) {
                if(first)
                    first=false;
                else
                    sb.append("|");
                sb.append(flag);
            }
        }
        return sb.toString();
    }

    protected static void writeHeader(Header hdr, DataOutput out) throws Exception {
        short magic_number=ClassConfigurator.getMagicNumber(hdr.getClass());
        out.writeShort(magic_number);
        hdr.writeTo(out);
    }



    protected static Header readHeader(DataInput in) throws Exception {
        short magic_number=in.readShort();
        Class clazz=ClassConfigurator.get(magic_number);
        if(clazz == null)
            throw new IllegalArgumentException("magic number " + magic_number + " is not available in magic map");

        Header hdr=(Header)clazz.newInstance();
        hdr.readFrom(in);
        return hdr;
    }

    protected static Headers createHeaders(int size) {
        return size > 0? new Headers(size) : new Headers(3);
    }


    protected static Headers createHeaders(Headers m) {
        return new Headers(m);
    }

    /* ------------------------------- End of Private methods ---------------------------- */


}
