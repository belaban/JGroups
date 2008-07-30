
package org.jgroups;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Buffer;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.jgroups.util.Headers;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A Message encapsulates data sent to members of a group. It contains among other things the
 * address of the sender, the destination address, a payload (byte buffer) and a list of
 * headers. Headers are added by protocols on the sender side and removed by protocols
 * on the receiver's side.
 * <p>
 * The byte buffer can point to a reference, and we can subset it using index and length. However,
 * when the message is serialized, we only write the bytes between index and length.
 * @author Bela Ban
 * @version $Id: Message.java,v 1.88 2008/07/30 08:56:39 belaban Exp $
 */
public class Message implements Streamable {
    protected Address dest_addr=null;
    protected Address src_addr=null;

    /** The payload */
    private byte[]    buf=null;

    /** The index into the payload (usually 0) */
    protected int     offset=0;

    /** The number of bytes in the buffer (usually buf.length is buf not equal to null). */
    protected int     length=0;

    /** Map<String,Header> */
    protected Headers headers;

    protected static final Log log=LogFactory.getLog(Message.class);


    static final byte DEST_SET      = 1;
    static final byte SRC_SET       = 2;
    static final byte BUF_SET       = 4;
    // static final byte HDRS_SET=8; // bela July 15 2005: not needed, we always create headers
    static final byte IPADDR_DEST   =16;
    static final byte IPADDR_SRC    =32;
    static final byte SRC_HOST_NULL =64;


    // =========================== Flags ==============================
    public static final byte OOB       = 1;
    public static final byte LOW_PRIO  = 2; // not yet sure if we want this flag...
    public static final byte HIGH_PRIO = 4; // not yet sure if we want this flag...

    private byte flags=0;

    static final Set<Class> nonStreamableHeaders=new HashSet<Class>();

    /** Map<Address,Address>. Maintains mappings to canonical addresses */
    private static final ConcurrentHashMap<Address,Address> canonicalAddresses=new ConcurrentHashMap<Address,Address>();
    private static final boolean DISABLE_CANONICALIZATION;

    static {
        boolean b;
        try {
            b=Boolean.getBoolean("disable_canonicalization");
        }
        catch (java.security.AccessControlException e) {
            // this will happen in an applet context
            b=false;
        }
        DISABLE_CANONICALIZATION=b;
    }


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
        if(DISABLE_CANONICALIZATION)
            dest_addr=new_dest;
        else
            dest_addr=canonicalAddress(new_dest);
    }

    public Address getSrc() {
        return src_addr;
    }

    public void setSrc(Address new_src) {
        if(DISABLE_CANONICALIZATION)
            src_addr=new_src;
        else
            src_addr=canonicalAddress(new_src);
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
    public Map<String,Header> getHeaders() {
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
        flags += flag;
    }

    public void clearFlag(byte flag) {
        if(flag > Byte.MAX_VALUE || flag < 0)
            throw new IllegalArgumentException("flag has to be >= 0 and <= " + Byte.MAX_VALUE);
        flags -= flag;
    }

    public boolean isFlagSet(byte flag) {
        return (flags & flag) == flag;
    }

    public byte getFlags() {
        return flags;
    }


    /*---------------------- Used by protocol layers ----------------------*/

    /** Puts a header given a key into the hashmap. Overwrites potential existing entry. */
    public void putHeader(String key, Header hdr) {
        headers.putHeader(key, hdr);
    }

    /**
     * Puts a header given a key into the map, only if the key doesn't exist yet
     * @param key
     * @param hdr
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with the key,
     *         if the implementation supports null values.)
     */
    public Header putHeaderIfAbsent(String key, Header hdr) {
        return headers.putHeaderIfAbsent(key, hdr);
    }

    /**
     *
     * @param key
     * @return the header assoaicted with key
     * @deprecated Use getHeader() instead. The issue with removing a header is described in
     * http://jira.jboss.com/jira/browse/JGRP-393
     */
    public Header removeHeader(String key) {
        return getHeader(key);
    }

    public Header getHeader(String key) {
        return headers.getHeader(key);
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
            ret.append(", flags=").append(flagsToString());
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


    /**
     * Returns size of buffer, plus some constant overhead for src and dest, plus number of headers time
     * some estimated size/header. The latter is needed because we don't want to marshal all headers just
     * to find out their size requirements. If a header implements Sizeable, the we can get the correct
     * size.<p> Size estimations don't have to be very accurate since this is mainly used by FRAG to
     * determine whether to fragment a message or not. Fragmentation will then serialize the message,
     * therefore getting the correct value.
     */


    /**
     * Returns the exact size of the marshalled message. Uses method size() of each header to compute the size, so if
     * a Header subclass doesn't implement size() we will use an approximation. However, most relevant header subclasses
     * have size() implemented correctly. (See org.jgroups.tests.SizeTest).
     * @return The number of bytes for the marshalled message
     */
    public long size() {
        long retval=Global.BYTE_SIZE                  // leading byte
                + Global.BYTE_SIZE                    // flags
                + length                              // buffer
                + (buf != null? Global.INT_SIZE : 0); // if buf != null 4 bytes for length

        // if(dest_addr != null)
        // retval+=dest_addr.size();
        if(src_addr != null)
            retval+=(src_addr).size();

        retval+=Global.SHORT_SIZE; // size (short)
        retval+=headers.marshalledSize();
        return retval;
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

        if(src_addr != null) {
            leading+=SRC_SET;
            if(src_addr instanceof IpAddress) {
                leading+=IPADDR_SRC;
                if(((IpAddress)src_addr).getIpAddress() == null) {
                    leading+=SRC_HOST_NULL;
                }
            }
        }
        if(buf != null)
            leading+=BUF_SET;

        // 1. write the leading byte first
        out.write(leading);

        // the flags (e.g. OOB, LOW_PRIO)
        out.write(flags);

        // 3. src_addr
        if(src_addr != null) {
            if(src_addr instanceof IpAddress) {
                src_addr.writeTo(out);
            }
            else {
                Util.writeAddress(src_addr, out);
            }
        }

        // 4. buf
        if(buf != null) {
            out.writeInt(length);
            out.write(buf, offset, length);
        }

        // 5. headers
        int size=headers.size();
        out.writeShort(size);
        final Object[] data=headers.getRawData();
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null) {
                out.writeUTF((String)data[i]);
                writeHeader((Header)data[i+1], out);
            }
        }
    }


    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        int len, leading;
        String hdr_name;
        Header hdr;


        // 1. read the leading byte first
        leading=in.readByte();

        flags=in.readByte();

        // 2. src_addr
        if((leading & SRC_SET) == SRC_SET) {
            if((leading & IPADDR_SRC) == IPADDR_SRC) {
                src_addr=new IpAddress();
                src_addr.readFrom(in);
            }
            else {
                src_addr=Util.readAddress(in);
            }
            if(!DISABLE_CANONICALIZATION)
                src_addr=canonicalAddress(src_addr);
        }

        // 3. buf
        if((leading & BUF_SET) == BUF_SET) {
            len=in.readInt();
            buf=new byte[len];
            in.read(buf, 0, len);
            length=len;
        }

        // 4. headers
        len=in.readShort();
        headers=createHeaders(len);
        Object[] data=headers.getRawData();
        int index=0;
        for(int i=0; i < len; i++) {
            hdr_name=in.readUTF();
            data[index++]=hdr_name;
            hdr=readHeader(in);
            data[index++]=hdr;
            // headers.putHeader(hdr_name, hdr);
        }
    }



    /* --------------------------------- End of Interface Streamable ----------------------------- */



    /* ----------------------------------- Private methods ------------------------------- */

    private String flagsToString() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        if(isFlagSet(OOB)) {
            first=false;
            sb.append("OOB");
        }
        if(isFlagSet(LOW_PRIO)) {
            if(!first)
                sb.append("|");
            else
                first=false;
            sb.append("LOW_PRIO");
        }
        if(isFlagSet(HIGH_PRIO)) {
            if(!first)
                sb.append("|");
            else
                first=false;
            sb.append("HIGH_PRIO");
        }
        return sb.toString();
    }

    private static void writeHeader(Header value, DataOutputStream out) throws IOException {
        short magic_number;
        String classname;
        ObjectOutputStream oos=null;
        int size=value.size();
        try {
            magic_number=ClassConfigurator.getMagicNumber(value.getClass());
            // write the magic number or the class name
            out.writeShort(magic_number);
            if(magic_number == -1) {
                classname=value.getClass().getName();
                out.writeUTF(classname);
                if(log.isWarnEnabled())
                    log.warn("magic number for " + classname + " not found, make sure you add your header to " +
                            "jg-magic-map.xml, or register it programmatically with the ClassConfigurator");
            }

            out.writeShort(size);

            // write the contents
            if(value instanceof Streamable) {
                ((Streamable)value).writeTo(out);
            }
            else {
                oos=new ObjectOutputStream(out);
                value.writeExternal(oos);
                if(!nonStreamableHeaders.contains(value.getClass())) {
                    nonStreamableHeaders.add(value.getClass());
                    if(log.isTraceEnabled())
                        log.trace("encountered non-Streamable header: " + value.getClass());
                }
            }
        }
        finally {
            if(oos != null)
                oos.close(); // this is a no-op on ByteArrayOutputStream
        }
    }


    private static Header readHeader(DataInputStream in) throws IOException {
        Header            hdr;
        short             magic_number;
        String            classname;
        Class             clazz;
        ObjectInputStream ois=null;

        try {
            magic_number=in.readShort();
            if(magic_number != -1) {
                clazz=ClassConfigurator.get(magic_number);
                if(clazz == null)
                    throw new IllegalArgumentException("magic number " + magic_number + " is not available in magic map");
            }
            else {
                classname=in.readUTF();
                clazz=ClassConfigurator.get(classname);
            }

            in.readShort(); // we discard the size since we don't use it

            hdr=(Header)clazz.newInstance();
            if(hdr instanceof Streamable) {
               ((Streamable)hdr).readFrom(in);
            }
            else {
                ois=new ObjectInputStream(in);
                hdr.readExternal(ois);
            }
        }
        catch(Exception ex) {
            IOException io_ex=new IOException("failed reading header");
            io_ex.initCause(ex);
            throw io_ex;
        }
        return hdr;
    }

    private static Headers createHeaders(int size) {
        return size > 0? new Headers(size) : new Headers(3);
    }


    private static Headers createHeaders(Headers m) {
        return new Headers(m);
    }

    /** canonicalize addresses to some extent.  There are race conditions
     * allowed in this method, so it may not fully canonicalize an address
     * @param nonCanonicalAddress
     * @return canonical representation of the address
     */
    private static Address canonicalAddress(Address nonCanonicalAddress) {
        Address result=null;
        if(nonCanonicalAddress == null) {
            return null;
        }
        // do not synchronize between get/put on the canonical map to avoid cost of contention
        // this can allow multiple equivalent addresses to leak out, but it's worth the cost savings
        try {
            result=canonicalAddresses.putIfAbsent(nonCanonicalAddress, nonCanonicalAddress);
            return result != null? result : nonCanonicalAddress;
        }
        catch(NullPointerException npe) {
            // no action needed
        }
        return result;
    }

    /* ------------------------------- End of Private methods ---------------------------- */



}
