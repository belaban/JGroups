// $Id: Message.java,v 1.3 2004/01/16 07:45:37 belaban Exp $

package org.jgroups;


import org.jgroups.log.Trace;
import org.jgroups.util.Marshaller;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * A Message encapsulates data sent to members of a group. It contains among other things the
 * address of the sender, the destination address, a payload (byte buffer) and a list of
 * headers. Headers are added by protocols on the sender side and removed by protocols
 * on the receiver's side.
 * @author Bela Ban
 */
public class Message implements Externalizable {
    protected Address dest_addr=null;
    protected Address src_addr=null;
    protected byte[] buf=null;
    protected HashMap headers=null;
    static final long ADDRESS_OVERHEAD=200; // estimated size of Address (src and dest)
    static final long serialVersionUID=-1137364035832847034L;

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
        dest_addr=dest;
        src_addr=src;
        this.buf=buf;
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
    public Message(Address dest, Address src, Serializable obj) throws IOException {
        dest_addr=dest;
        src_addr=src;
        setObject(obj);
    }


    /** Only used for Externalization (creating an initial object) */
    public Message() {
    }  // should not be called as normal constructor

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
     * Returns the payload (byte buffer). Note that this buffer should not be modified as we do not
     * copy the buffer on copy() or clone(): the buffer of the copied message is simply a reference to
     * the old buffer.
     */
    public byte[] getBuffer() {
        return buf;
    }

    public void setBuffer(byte[] b) {
        buf=b;
    }

    public Map getHeaders() {
        return headers;
    }

    public void setObject(Serializable obj) throws IOException {
        if(obj == null) return;
        ByteArrayOutputStream out_stream=new ByteArrayOutputStream(256);
        ObjectOutputStream out=new ObjectOutputStream(out_stream);
        out.writeObject(obj);
        buf=out_stream.toByteArray();
    }

    public Object getObject() throws IOException, ClassNotFoundException {
        if(buf == null) return null;
        ByteArrayInputStream in_stream=new ByteArrayInputStream(buf);
        ObjectInputStream in=new ObjectInputStream(in_stream);
        return in.readObject();
    }


    /**
     * Nulls all fields of this message so that the message can be reused. Removes all headers from the
     * stack, but keeps the stack
     */
    public void reset() {
        dest_addr=src_addr=null;
        buf=null;
        if(headers != null)
            headers.clear();
    }

    /*---------------------- Used by protocol layers ----------------------*/

    /** Puts a header given a key into the hashmap. Overwrites potential existing entry. */
    public void putHeader(String key, Header hdr) {
        // the following code is compiled out of JGroups when Trace.debug==false
        if(Trace.debug) {
            if(headers().containsKey(key)) {
                Trace.debug("Message.putHeader()", "header for \"" + key  +
                                                   "\" is already present: old header=" +
                                                   headers().get(key) + ", new header=" + hdr);
            }
        }
        headers().put(key, hdr);
    }

    public Header removeHeader(String key) {
        return headers != null ? (Header)headers.remove(key) : null;
    }

    public void removeHeaders() {
        if(headers != null)
            headers.clear();
    }

    public Header getHeader(String key) {
        return headers != null ? (Header)headers.get(key) : null;
    }
    /*---------------------------------------------------------------------*/


    public Message copy() {
        Message retval=new Message();
        retval.dest_addr=dest_addr;
        retval.src_addr=src_addr;
        if(buf != null)
            retval.buf=buf;

        if(headers != null)
            retval.headers=(HashMap)headers.clone();
        return retval;
    }

    protected Object clone() throws CloneNotSupportedException {
        return copy();
    }

    public Message makeReply() {
        return new Message(src_addr, null, null);
    }


    public String toString() {
        StringBuffer ret=new StringBuffer();
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

        if(headers != null && headers.size() > 0)
            ret.append(" (" + headers.size() + " headers)");

//         {
//             ret.append(" (");
//             for(Iterator i=headers.keySet().iterator(); i.hasNext();) {
//                 Object key = i.next();
//                 Object value = headers.get(key);
//                 ret.append(key+"="+value);
//                 if (i.hasNext()) {
//                     ret.append(" ");
//                 }
//             }
//             ret.append(")");
//         }

        ret.append(", size = ");
        if(buf != null && buf.length > 0)
            ret.append(buf.length);
        else
            ret.append("0");
        ret.append(" bytes");
        ret.append("]");
        return ret.toString();
    }


    /** Tries to read an object from the message's buffer and prints it */
    public String toStringAsObject() {
        Object obj;

        if(buf == null) return null;
        try {
            obj=getObject();
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
        long retval=buf != null ? buf.length : 0;
        long hdr_size=0;
        Header hdr;

        if(dest_addr != null) retval+=ADDRESS_OVERHEAD;
        if(src_addr != null) retval+=ADDRESS_OVERHEAD;

        if(headers != null) {
            for(Iterator it=headers.values().iterator(); it.hasNext();) {
                hdr=(Header)it.next();
                if(hdr == null) continue;
                hdr_size=hdr.size();
                if(hdr_size <= 0)
                    hdr_size=Header.HDR_OVERHEAD;
                else
                    retval+=hdr_size;
            }
        }
        return retval;
    }


    public String printObjectHeaders() {
        StringBuffer sb=new StringBuffer();
        Map.Entry entry;

        if(headers != null) {
            for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
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
            out.writeInt(buf.length);
            out.write(buf);
        }

        if(headers == null)
            out.writeInt(0);
        else {
            len=headers.size();
            out.writeInt(len);
            for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                out.writeUTF((String)entry.getKey());
                hdr=(Externalizable)entry.getValue();
                Marshaller.write(hdr, out);
            }
        }
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int      len;
        boolean  destAddressExist=in.readBoolean();
        boolean  srcAddressExist;
        Object   key, value;

        if(destAddressExist) {
            dest_addr=(Address)Marshaller.read(in);
        }

        srcAddressExist=in.readBoolean();
        if(srcAddressExist) {
            src_addr=(Address)Marshaller.read(in);
        }

        int i=in.readInt();
        if(i != 0) {
            buf=new byte[i];
            in.readFully(buf);
        }

        len=in.readInt();
        if(len > 0) headers=new HashMap();
        while(len-- > 0) {
            key=in.readUTF();
            value=Marshaller.read(in);
            headers.put(key, value);
        }
    }

    /* --------------------------------- End of Interface Externalizable ----------------------------- */



    /* ----------------------------------- Private methods ------------------------------- */

    HashMap headers() {
        return headers != null ? headers : (headers=new HashMap());
    }
    /* ------------------------------- End of Private methods ---------------------------- */


}
