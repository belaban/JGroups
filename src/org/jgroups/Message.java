// $Id: Message.java,v 1.47 2006/02/07 13:49:02 belaban Exp $

package org.jgroups;


import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ContextObjectInputStream;
import org.jgroups.util.Marshaller;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
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
 */
public class Message implements Externalizable, Streamable {
    protected Address dest_addr=null;
    protected Address src_addr=null;

    /** The payload */
    private byte[]    buf=null;

    /** The index into the payload (usually 0) */
    protected transient int     offset=0;

    /** The number of bytes in the buffer (usually buf.length is buf not equal to null). */
    protected transient int     length=0;

    /** Map<String,Header> */
    protected Map headers;

    protected static final Log log=LogFactory.getLog(Message.class);

    private static final long serialVersionUID=7966206671974139740L;

    static final byte DEST_SET=1;
    static final byte SRC_SET=2;
    static final byte BUF_SET=4;
    // static final byte HDRS_SET=8; // bela July 15 2005: not needed, we always create headers
    static final byte IPADDR_DEST=16;
    static final byte IPADDR_SRC=32;
    static final byte SRC_HOST_NULL=64;

    static final HashSet nonStreamableHeaders=new HashSet(); // todo: remove when all headers are streamable

    /** Map<Address,Address>. Maintains mappings to canonical addresses */
    private static final Map canonicalAddresses=new ConcurrentReaderHashMap();
    private static final boolean DISABLE_CANONICALIZATION=Boolean.getBoolean("disable_canonicalization");


    /** Public constructor
     *  @param dest Address of receiver. If it is <em>null</em> or a <em>string</em>, then
     *              it is sent to the group (either to current group or to the group as given
     *              in the string). If it is a Vector, then it contains a number of addresses
     *              to which it must be sent. Otherwise, it contains a single destination.<p>
     *              Addresses are generally untyped (all are of type <em>Object</em>. A channel
     *              instance must know what types of addresses it expects and downcast
     *              accordingly.
     */
    public Message(Address dest) {
        dest_addr=dest;
        headers=createHeaders(7);
    }

    /** Public constructor
     *  @param dest Address of receiver. If it is <em>null</em> or a <em>string</em>, then
     *              it is sent to the group (either to current group or to the group as given
     *              in the string). If it is a Vector, then it contains a number of addresses
     *              to which it must be sent. Otherwise, it contains a single destination.<p>
     *              Addresses are generally untyped (all are of type <em>Object</em>. A channel
     *              instance must know what types of addresses it expects and downcast
     *              accordingly.
     *  @param src  Address of sender
     *  @param buf  Message to be sent. Note that this buffer must not be modified (e.g. buf[0]=0 is
     *              not allowed), since we don't copy the contents on clopy() or clone().
     */
    public Message(Address dest, Address src, byte[] buf) {
        this(dest);
        src_addr=src;
        setBuffer(buf);
    }

    /**
     * Constructs a message. The index and length parameters allow to provide a <em>reference</em> to
     * a byte buffer, rather than a copy, and refer to a subset of the buffer. This is important when
     * we want to avoid copying. When the message is serialized, only the subset is serialized.
     * @param dest Address of receiver. If it is <em>null</em> or a <em>string</em>, then
     *              it is sent to the group (either to current group or to the group as given
     *              in the string). If it is a Vector, then it contains a number of addresses
     *              to which it must be sent. Otherwise, it contains a single destination.<p>
     *              Addresses are generally untyped (all are of type <em>Object</em>. A channel
     *              instance must know what types of addresses it expects and downcast
     *              accordingly.
     * @param src    Address of sender
     * @param buf    A reference to a byte buffer
     * @param offset The index into the byte buffer
     * @param length The number of bytes to be used from <tt>buf</tt>. Both index and length are checked for
     *               array index violations and an ArrayIndexOutOfBoundsException will be thrown if invalid
     */
    public Message(Address dest, Address src, byte[] buf, int offset, int length) {
        this(dest);
        src_addr=src;
        setBuffer(buf, offset, length);
    }


    /** Public constructor
     *  @param dest Address of receiver. If it is <em>null</em> or a <em>string</em>, then
     *              it is sent to the group (either to current group or to the group as given
     *              in the string). If it is a Vector, then it contains a number of addresses
     *              to which it must be sent. Otherwise, it contains a single destination.<p>
     *              Addresses are generally untyped (all are of type <em>Object</em>. A channel
     *              instance must know what types of addresses it expects and downcast
     *              accordingly.
     *  @param src  Address of sender
     *  @param obj  The object will be serialized into the byte buffer. <em>Object
     *              has to be serializable </em>! Note that the resulting buffer must not be modified
     *              (e.g. buf[0]=0 is not allowed), since we don't copy the contents on clopy() or clone().
     */
    public Message(Address dest, Address src, Serializable obj) {
        this(dest);
        src_addr=src;
        setObject(obj);
    }


    public Message() {
        headers=createHeaders(7);
    }


    public Message(boolean create_headers) {
        if(create_headers)
            headers=createHeaders(7);
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
            offset=length=0;
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

    public Map getHeaders() {
        return headers;
    }

    final public void setObject(Serializable obj) {
        if(obj == null) return;
        try {
            ByteArrayOutputStream out_stream=new ByteArrayOutputStream();
            ObjectOutputStream out=new ObjectOutputStream(out_stream);
            out.writeObject(obj);
            setBuffer(out_stream.toByteArray());
        }
        catch(IOException ex) {
            throw new IllegalArgumentException(ex.toString());
        }
    }

    final public Object getObject() {
        if(buf == null) return null;
        try {
            ByteArrayInputStream in_stream=new ByteArrayInputStream(buf, offset, length);
            // ObjectInputStream in=new ObjectInputStream(in_stream);
            ObjectInputStream in=new ContextObjectInputStream(in_stream); // put it back on norbert's request
            return in.readObject();
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex.toString());
        }
    }


    /**
     * Nulls all fields of this message so that the message can be reused. Removes all headers from the
     * hashmap, but keeps the hashmap
     */
    public void reset() {
        dest_addr=src_addr=null;
        setBuffer(null);
        headers.clear();
    }

    /*---------------------- Used by protocol layers ----------------------*/

    /** Puts a header given a key into the hashmap. Overwrites potential existing entry. */
    public void putHeader(String key, Header hdr) {
        headers.put(key, hdr);
    }

    public Header removeHeader(String key) {
        return (Header)headers.remove(key);
    }

    public void removeHeaders() {
        headers.clear();
    }

    public Header getHeader(String key) {
        return (Header)headers.get(key);
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
        StringBuffer ret=new StringBuffer(64);
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
        if(headers != null && (size=headers.size()) > 0)
            ret.append(" (").append(size).append(" headers)");

        ret.append(", size = ");
        if(buf != null && length > 0)
            ret.append(length);
        else
            ret.append('0');
        ret.append(" bytes");
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
    public long size() {
        long retval=Global.BYTE_SIZE                  // leading byte
                + length                              // buffer
                + (buf != null? Global.INT_SIZE : 0); // if buf != null 4 bytes for length

        // if(dest_addr != null)
           // retval+=dest_addr.size();
        if(src_addr != null)
            retval+=(src_addr).size();

            Map.Entry entry;
            String key;
            Header hdr;
            retval+=Global.SHORT_SIZE; // size (short)
            for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                key=(String)entry.getKey();
                retval+=key.length() +2; // not the same as writeUTF(), but almost
                hdr=(Header)entry.getValue();
                retval+=5; // 1 for presence of magic number, 4 for magic number
                retval+=hdr.size();
            }
        return retval;
    }


    public String printObjectHeaders() {
        StringBuffer sb=new StringBuffer();
        Map.Entry entry;

        if(headers != null) {
            for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
            }
        }
        return sb.toString();
    }



    /* ----------------------------------- Interface Externalizable ------------------------------- */

    public void writeExternal(ObjectOutput out) throws IOException {
        int             len;
        Externalizable  hdr;
        Map.Entry       entry;

        if(dest_addr != null) {
            out.writeBoolean(true);
            Marshaller.write(dest_addr, out);
        }
        else {
            out.writeBoolean(false);
        }

        if(src_addr != null) {
            out.writeBoolean(true);
            Marshaller.write(src_addr, out);
        }
        else {
            out.writeBoolean(false);
        }

        if(buf == null)
            out.writeInt(0);
        else {
            out.writeInt(length);
            out.write(buf, offset, length);
        }

        len=headers.size();
        out.writeInt(len);
        for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            out.writeUTF((String)entry.getKey());
            hdr=(Externalizable)entry.getValue();
            Marshaller.write(hdr, out);
        }
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean  destAddressExist=in.readBoolean();

        if(destAddressExist) {
            dest_addr=(Address)Marshaller.read(in);
            if(!DISABLE_CANONICALIZATION)
                dest_addr=canonicalAddress(dest_addr);
        }

        boolean srcAddressExist=in.readBoolean();
        if(srcAddressExist) {
            src_addr=(Address)Marshaller.read(in);
            if(!DISABLE_CANONICALIZATION)
                src_addr=canonicalAddress(src_addr);
        }

        int i=in.readInt();
        if(i != 0) {
            buf=new byte[i];
            in.readFully(buf);
            offset=0;
            length=buf.length;
        }

        int len=in.readInt();
        while(len-- > 0) {
            Object key=in.readUTF();
            Object value=Marshaller.read(in);
            headers.put(key, value);
        }
    }

    /* --------------------------------- End of Interface Externalizable ----------------------------- */


    /* ----------------------------------- Interface Streamable  ------------------------------- */

    /**
     * Streams all members (dest and src addresses, buffer and headers) to the output stream.
     * @param out
     * @throws IOException
     */
    public void writeTo(DataOutputStream out) throws IOException {
        byte leading=0;

//        if(dest_addr != null) {
//            leading+=DEST_SET;
//            if(dest_addr instanceof IpAddress)
//                leading+=IPADDR_DEST;
//        }

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

        // 2. dest_addr
//        if(dest_addr != null) {
//            if(dest_addr instanceof IpAddress)
//                dest_addr.writeTo(out);
//            else
//                Util.writeAddress(dest_addr, out);
//        }

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
        Map.Entry        entry;
        for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            out.writeUTF((String)entry.getKey());
            writeHeader((Header)entry.getValue(), out);
        }
    }


    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        int len, leading;
        String hdr_name;
        Header hdr;


        // 1. read the leading byte first
        leading=in.readByte();

        // 1. dest_addr
//        if((leading & DEST_SET) == DEST_SET) {
//            if((leading & IPADDR_DEST) == IPADDR_DEST) {
//                dest_addr=new IpAddress();
//                dest_addr.readFrom(in);
//            }
//            else {
//                dest_addr=Util.readAddress(in);
//            }
//        }

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
        for(int i=0; i < len; i++) {
            hdr_name=in.readUTF();
            hdr=readHeader(in);
            headers.put(hdr_name, hdr);
        }
    }



    /* --------------------------------- End of Interface Streamable ----------------------------- */



    /* ----------------------------------- Private methods ------------------------------- */

    private static void writeHeader(Header value, DataOutputStream out) throws IOException {
        int magic_number;
        String classname;
        ObjectOutputStream oos=null;
        try {
            magic_number=ClassConfigurator.getInstance(false).getMagicNumber(value.getClass());
            // write the magic number or the class name
            if(magic_number == -1) {
                out.writeBoolean(false);
                classname=value.getClass().getName();
                out.writeUTF(classname);
            }
            else {
                out.writeBoolean(true);
                out.writeInt(magic_number);
            }

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
        catch(ChannelException e) {
            log.error("failed writing the header", e);
        }
        finally {
            if(oos != null)
                oos.close(); // this is a no-op on ByteArrayOutputStream
        }
    }


    private static Header readHeader(DataInputStream in) throws IOException {
        Header            hdr;
        boolean           use_magic_number=in.readBoolean();
        int               magic_number;
        String            classname;
        Class             clazz;
        ObjectInputStream ois=null;

        try {
            if(use_magic_number) {
                magic_number=in.readInt();
                clazz=ClassConfigurator.getInstance(false).get(magic_number);
                if(clazz == null)
                    log.error("magic number " + magic_number + " is not available in magic map");
            }
            else {
                classname=in.readUTF();
                clazz=ClassConfigurator.getInstance(false).get(classname);
            }
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
            throw new IOException("failed read header: " + ex.toString());
        }
        return hdr;
    }

    private static Map createHeaders(int size) {
        return size > 0? new ConcurrentReaderHashMap(size) : new ConcurrentReaderHashMap();
    }


    private static Map createHeaders(Map m) {
        return new ConcurrentReaderHashMap(m);
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
            result=(Address)canonicalAddresses.get(nonCanonicalAddress);
        }
        catch(NullPointerException npe) {
            // no action needed
        }
        if(result == null) {
            result=nonCanonicalAddress;
            canonicalAddresses.put(nonCanonicalAddress, result);
        }
        return result;
    }

    /* ------------------------------- End of Private methods ---------------------------- */



}
