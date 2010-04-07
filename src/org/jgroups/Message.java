
package org.jgroups;


import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Buffer;
import org.jgroups.util.Headers;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;


/**
 * A Message encapsulates data sent to members of a group. It contains among other things the
 * address of the sender, the destination address, a payload (byte buffer) and a list of
 * headers. Headers are added by protocols on the sender side and removed by protocols
 * on the receiver's side.
 * <p>
 * The byte buffer can point to a reference, and we can subset it using index and length. However,
 * when the message is serialized, we only write the bytes between index and length.
 * @author Bela Ban
 * @version $Id: Message.java,v 1.115 2010/04/07 15:34:29 belaban Exp $
 */
public class Message implements Streamable {
    protected Address dest_addr;
    protected Address src_addr;

    /** The payload */
    private byte[]    buf;

    /** The index into the payload (usually 0) */
    protected int     offset;

    /** The number of bytes in the buffer (usually buf.length is buf not equal to null). */
    protected int     length;

    /** All headers are placed here */
    protected Headers headers;

    private volatile byte      flags;

    private volatile byte      transient_flags; // transient_flags is neither marshalled nor copied

    protected static final Log log=LogFactory.getLog(Message.class);



    static final byte DEST_SET         =  1;
    static final byte SRC_SET          =  2;
    static final byte BUF_SET          =  4;
    // static final byte HDRS_SET=8; // bela July 15 2005: not needed, we always create headers


    // =============================== Flags ====================================
    public static final byte OOB               =  1 << 0;
    public static final byte LOOPBACK          =  1 << 1; // if message was sent to self
    public static final byte DONT_BUNDLE       =  1 << 2; // don't bundle message at the transport
    public static final byte NO_FC             =  1 << 3; // bypass flow control
    public static final byte SCOPED            =  1 << 4; // when a message has a scope 


    // =========================== Transient flags ==============================
    public static final byte OOB_DELIVERED     =  1 << 0; // OOB which has already been delivered up the stack




    /** Public constructor
     *  @param dest Address of receiver. If it is <em>null</em> then the message sent to the group.
     *              Otherwise, it contains a single destination and is sent to that member.<p>
     */
    public Message(Address dest) {
        setDest(dest);
        headers=createHeaders(3);
    }

    /** Public constructor
     *  @param dest Address of receiver. If it is <em>null</em> then the message sent to the group.
     *              Otherwise, it contains a single destination and is sent to that member.<p>
     *  @param src  Address of sender
     *  @param buf  Message to be sent. Note that this buffer must not be modified (e.g. buf[0]=0 is
     *              not allowed), since we don't copy the contents on clopy() or clone().
     */
    public Message(Address dest, Address src, byte[] buf) {
        this(dest);
        setSrc(src);
        setBuffer(buf);
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
     * @param dest Address of receiver. If it is <em>null</em> then the message sent to the group.
     *             Otherwise, it contains a single destination and is sent to that member.<p>
     * @param src    Address of sender
     * @param buf    A reference to a byte buffer
     * @param offset The index into the byte buffer
     * @param length The number of bytes to be used from <tt>buf</tt>. Both index and length are checked for
     *               array index violations and an ArrayIndexOutOfBoundsException will be thrown if invalid
     */
    public Message(Address dest, Address src, byte[] buf, int offset, int length) {
        this(dest);
        setSrc(src);
        setBuffer(buf, offset, length);
    }


    /** Public constructor
     *  @param dest Address of receiver. If it is <em>null</em> then the message sent to the group.
     *              Otherwise, it contains a single destination and is sent to that member.<p>
     *  @param src  Address of sender
     *  @param obj  The object will be serialized into the byte buffer. <em>Object
     *              has to be serializable </em>! The resulting buffer must not be modified
     *              (e.g. buf[0]=0 is not allowed), since we don't copy the contents on clopy() or clone().<p/>
     *              Note that this is a convenience method and JGroups will use default Java serialization to
     *              serialize <code>obj</code> into a byte buffer.
     */
    public Message(Address dest, Address src, Serializable obj) {
        this(dest);
        setSrc(src);
        setObject(obj);
    }


    public Message() {
        headers=createHeaders(3);
    }


    public Message(boolean create_headers) {
        if(create_headers)
            headers=createHeaders(3);
    }

    public Address getDest() {
        return dest_addr;
    }

    public void setDest(Address new_dest) {
        dest_addr=new_dest;
    }

    public Address getSrc() {
        return src_addr;
    }

    public void setSrc(Address new_src) {
        src_addr=new_src;
    }

    /**
     * Returns a <em>reference</em> to the payload (byte buffer). Note that this buffer should not be modified as
     * we do not copy the buffer on copy() or clone(): the buffer of the copied message is simply a reference to
     * the old buffer.<br/>
     * Even if offset and length are used: we return the <em>entire</em> buffer, not a subset.
     */
    public byte[] getRawBuffer() {
        return buf;
    }

    /**
     * Returns a copy of the buffer if offset and length are used, otherwise a reference.
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

    final public void setBuffer(byte[] b) {
        buf=b;
        if(buf != null) {
            offset=0;
            length=buf.length;
        }
        else {
            offset=length=0;
        }
    }

    /**
     * Set the internal buffer to point to a subset of a given buffer
     * @param b The reference to a given buffer. If null, we'll reset the buffer to null
     * @param offset The initial position
     * @param length The number of bytes
     */
    final public void setBuffer(byte[] b, int offset, int length) {
        buf=b;
        if(buf != null) {
            if(offset < 0 || offset > buf.length)
                throw new ArrayIndexOutOfBoundsException(offset);
            if((offset + length) > buf.length)
                throw new ArrayIndexOutOfBoundsException((offset+length));
            this.offset=offset;
            this.length=length;
        }
        else {
            this.offset=this.length=0;
        }
    }

    /**
     <em>
     * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
     * retransmit a changed byte[] buffer !
     * </em>
     */
     public final void setBuffer(Buffer buf) {
        if(buf != null) {
            this.buf=buf.getBuf();
            this.offset=buf.getOffset();
            this.length=buf.getLength();
        }
    }

    /** Returns the offset into the buffer at which the data starts */
    public int getOffset() {
        return offset;
    }

    /** Returns the number of bytes in the buffer */
    public int getLength() {
        return length;
    }

    /** Returns a reference to the headers hashmap, which is <em>immutable</em>. Any attempt to
     * modify the returned map will cause a runtime exception */
    public Map<Short,Header> getHeaders() {
        return headers.getHeaders();
    }

    public String printHeaders() {
        return headers.printHeaders();
    }

    public int getNumHeaders() {
        return headers.size();
    }

    /**
     * Takes an object and uses Java serialization to generate the byte[] buffer which is set in the message.
     */
    final public void setObject(Serializable obj) {
        if(obj == null) return;
        try {
            byte[] tmp=Util.objectToByteBuffer(obj);
            setBuffer(tmp);
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Uses Java serialization to create an object from the buffer of the message. Note that this is dangerous when
     * using your own classloader, e.g. inside of an application server ! Most likely, JGroups will use the system
     * classloader to deserialize the buffer into an object, whereas (for example) a web application will want to
     * use the webapp's classloader, resulting in a ClassCastException. The recommended way is for the application to
     * use their own serialization and only pass byte[] buffer to JGroups.
     * @return
     */
    final public Object getObject() {
        try {
            return Util.objectFromByteBuffer(buf, offset, length);
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }


    public void setFlag(byte flag) {
        if(flag > Byte.MAX_VALUE || flag < 0)
            throw new IllegalArgumentException("flag has to be >= 0 and <= " + Byte.MAX_VALUE);
        flags |= flag;
    }

    public void clearFlag(byte flag) {
        if(flag > Byte.MAX_VALUE || flag < 0)
            throw new IllegalArgumentException("flag has to be >= 0 and <= " + Byte.MAX_VALUE);
        flags &= ~flag;
    }

    public boolean isFlagSet(byte flag) {
        return isFlagSet(flags, flag);
    }

    /**
     * Same as {@link #setFlag(byte)} but transient flags are never marshalled
     * @param flag
     */
    public void setTransientFlag(byte flag) {
        if(flag > Byte.MAX_VALUE || flag < 0)
            throw new IllegalArgumentException("flag has to be >= 0 and <= " + Byte.MAX_VALUE);
        transient_flags |= flag;
    }

    /**
     * Atomically checks if a given flag is set and - if not - sets it. When multiple threads concurrently call this
     * method with the same flag, only one of them will be able to set the flag
     * @param flag
     * @return True if the flag could be set, false if not (was already set)
     */
    public boolean setTransientFlagIfAbsent(byte flag) {
        if(flag > Byte.MAX_VALUE || flag < 0)
            throw new IllegalArgumentException("flag has to be >= 0 and <= " + Byte.MAX_VALUE);
        synchronized(this) {
            if(isTransientFlagSet(flag))
                return false;
            else
                setTransientFlag(flag);
            return true;
        }
    }

    public void clearTransientFlag(byte flag) {
        if(flag > Byte.MAX_VALUE || flag < 0)
            throw new IllegalArgumentException("flag has to be >= 0 and <= " + Byte.MAX_VALUE);
        transient_flags &= ~flag;
    }

    public boolean isTransientFlagSet(byte flag) {
        return isFlagSet(transient_flags, flag);
    }


    protected static boolean isFlagSet(byte flags, byte flag) {
        return (flags & flag) == flag;
    }

    public byte getFlags() {
        return flags;
    }

    public byte getTransientFlags() {
        return transient_flags;
    }


    public void setScope(short scope) {
        Util.setScope(this, scope);
    }

    public short getScope() {
        return Util.getScope(this);
    }

    /*---------------------- Used by protocol layers ----------------------*/

    /** Puts a header given an ID into the hashmap. Overwrites potential existing entry. */
    public void putHeader(short id, Header hdr) {
        if(id < 0)
            throw new IllegalArgumentException("An ID of " + id + " is invalid");
        headers.putHeader(id, hdr);
    }

    /**
     * Puts a header given a key into the map, only if the key doesn't exist yet
     * @param id
     * @param hdr
     * @return the previous value associated with the specified key, or <tt>null</tt> if there was no mapping for the key.
     *         (A <tt>null</tt> return can also indicate that the map previously associated <tt>null</tt> with the key,
     *         if the implementation supports null values.)
     */
    public Header putHeaderIfAbsent(short id, Header hdr) {
        if(id < 0)
            throw new IllegalArgumentException("An ID of " + id + " is invalid");
        return headers.putHeaderIfAbsent(id, hdr);
    }

    /**
     *
     * @param key
     * @return the header associated with key
     * @deprecated Use getHeader() instead. The issue with removing a header is described in
     * http://jira.jboss.com/jira/browse/JGRP-393
     */
    public Header removeHeader(short id) {
        return getHeader(id);
    }

    public Header getHeader(short id) {
        if(id < 0)
            throw new IllegalArgumentException("An ID of " + id + " is invalid");
        return headers.getHeader(id);
    }
    /*---------------------------------------------------------------------*/


    public Message copy() {
        return copy(true);
    }

    /**
     * Create a copy of the message. If offset and length are used (to refer to another buffer), the copy will
     * contain only the subset offset and length point to, copying the subset into the new copy.
     * @param copy_buffer
     * @return Message with specified data
     */
    public Message copy(boolean copy_buffer) {
        Message retval=new Message(false);
        retval.dest_addr=dest_addr;
        retval.src_addr=src_addr;
        retval.flags=flags;

        if(copy_buffer && buf != null) {

            // change bela Feb 26 2004: we don't resolve the reference
            retval.setBuffer(buf, offset, length);
        }

        retval.headers=createHeaders(headers);
        return retval;
    }


    protected Object clone() throws CloneNotSupportedException {
        return copy();
    }

    public Message makeReply() {
        return new Message(src_addr);
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
     * @param out
     * @throws IOException
     */
    public void writeTo(DataOutputStream out) throws IOException {
        byte leading=0;

        if(dest_addr != null)
            leading=Util.setFlag(leading, DEST_SET);

        if(src_addr != null)
            leading=Util.setFlag(leading, SRC_SET);

        if(buf != null)
            leading=Util.setFlag(leading, BUF_SET);

        // 1. write the leading byte first
        out.write(leading);

        // 2. the flags (e.g. OOB, LOW_PRIO)
        out.write(flags);

        // 3. dest_addr
        if(dest_addr != null)
            Util.writeAddress(dest_addr, out);

        // 4. src_addr
        if(src_addr != null)
            Util.writeAddress(src_addr, out);

        // 5. buf
        if(buf != null) {
            out.writeInt(length);
            out.write(buf, offset, length);
        }

        // 6. headers
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
    }

    /**
     * Writes the message to the output stream, but excludes the dest and src addresses unless the src address given
     * as argument is different from the message's src address
     * @param src
     * @param out
     * @throws IOException
     */
    public void writeToNoAddrs(Address src, DataOutputStream out) throws IOException {
        byte leading=0;

        boolean write_src_addr=src == null || src_addr != null && !src_addr.equals(src);

        if(write_src_addr)
            leading=Util.setFlag(leading, SRC_SET);

        if(buf != null)
            leading=Util.setFlag(leading, BUF_SET);

        // 1. write the leading byte first
        out.write(leading);

        // 2. the flags (e.g. OOB, LOW_PRIO)
        out.write(flags);

        // 4. src_addr
        if(write_src_addr)
            Util.writeAddress(src_addr, out);

        // 5. buf
        if(buf != null) {
            out.writeInt(length);
            out.write(buf, offset, length);
        }

        // 6. headers
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
    }


    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {

        // 1. read the leading byte first
        byte leading=in.readByte();

        // 2. the flags
        flags=in.readByte();

        // 3. dest_addr
        if(Util.isFlagSet(leading, DEST_SET))
            dest_addr=Util.readAddress(in);

        // 4. src_addr
        if(Util.isFlagSet(leading, SRC_SET))
            src_addr=Util.readAddress(in);

        // 5. buf
        if(Util.isFlagSet(leading, BUF_SET)) {
            int len=in.readInt();
            buf=new byte[len];
            in.readFully(buf, 0, len);
            length=len;
        }

        // 6. headers
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
    }

    /* --------------------------------- End of Interface Streamable ----------------------------- */


    /**
     * Returns the exact size of the marshalled message. Uses method size() of each header to compute the size, so if
     * a Header subclass doesn't implement size() we will use an approximation. However, most relevant header subclasses
     * have size() implemented correctly. (See org.jgroups.tests.SizeTest).
     * @return The number of bytes for the marshalled message
     */
    public long size() {
        long retval=Global.BYTE_SIZE   // leading byte
                + Global.BYTE_SIZE;    // flags
        if(dest_addr != null)
            retval+=Util.size(dest_addr);
        if(src_addr != null)
            retval+=Util.size(src_addr);
        if(buf != null)
            retval+=Global.INT_SIZE // length (integer)
                    + length;       // number of bytes in the buffer

        retval+=Global.SHORT_SIZE;  // number of headers
        retval+=headers.marshalledSize();
        return retval;
    }



    /* ----------------------------------- Private methods ------------------------------- */

    public static String flagsToString(byte flags) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        if(isFlagSet(flags, OOB)) {
            first=false;
            sb.append("OOB");
        }
        if(isFlagSet(flags, LOOPBACK)) {
            if(!first)
                sb.append("|");
            else
                first=false;
            sb.append("LOOPBACK");
        }
        if(isFlagSet(flags, DONT_BUNDLE)) {
            if(!first)
                sb.append("|");
            else
                first=false;
            sb.append("DONT_BUNDLE");
        }
        if(isFlagSet(flags, NO_FC)) {
            if(!first)
                sb.append("|");
            else
                first=false;
            sb.append("NO_FC");
        }
        if(isFlagSet(flags, SCOPED)) {
            if(!first)
                sb.append("|");
            else
                first=false;
            sb.append("SCOPED");
        }
        return sb.toString();
    }


    public static String transientFlagsToString(byte flags) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        if(isFlagSet(flags, OOB_DELIVERED)) {
            if(!first)
                sb.append("|");
            else
                first=false;
            sb.append("OOB_DELIVERED");
        }
        return sb.toString();
    }

    private static void writeHeader(Header hdr, DataOutputStream out) throws IOException {
        short magic_number=ClassConfigurator.getMagicNumber(hdr.getClass());
        out.writeShort(magic_number);
        hdr.writeTo(out);
    }


    private static Header readHeader(DataInputStream in) throws IOException {
        try {
            short magic_number=in.readShort();
            Class clazz=ClassConfigurator.get(magic_number);
            if(clazz == null)
                throw new IllegalArgumentException("magic number " + magic_number + " is not available in magic map");

            Header hdr=(Header)clazz.newInstance();
            hdr.readFrom(in);
            return hdr;
        }
        catch(Exception ex) {
            IOException io_ex=new IOException("failed reading header");
            io_ex.initCause(ex);
            throw io_ex;
        }
    }

    private static Headers createHeaders(int size) {
        return size > 0? new Headers(size) : new Headers(3);
    }


    private static Headers createHeaders(Headers m) {
        return new Headers(m);
    }


    /* ------------------------------- End of Private methods ---------------------------- */


}
