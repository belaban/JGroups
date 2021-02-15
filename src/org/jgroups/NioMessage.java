
package org.jgroups;


import org.jgroups.util.ByteArray;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * A {@link Message} with a (heap-based or direct) {@link java.nio.ByteBuffer} as payload.<br/>
 * <br/>
 * Note that the payload of an NioMessage must not be modified after sending it (ie. {@link JChannel#send(Message)};
 * serialization depends on position and limit to be correct.
 *
 * @since  5.0
 * @author Bela Ban
 */
public class NioMessage extends BaseMessage {

    /** The payload */
    protected ByteBuffer buf;

    /**
     * If true, use direct memory when creating {@link ByteBuffer} payloads, e.g. on {@link #setArray(byte[], int, int)},
     * {@link #setArray(ByteArray)}, {@link #setObject(Object)} or when getting read from the network
     * ({@link #readPayload(DataInput)}).<br/>
     * Unless this flag is set to true, a direct {@link ByteBuffer} payload will become a heap-based payload by a
     * receiver when sent over the network. This may be useful if we want to use off-heap (direct) memory only for
     * sending, but not receiving of messages.<br/>
     * If we want the receiver to create a direct-memory based message, set this flag to true before sending the message.
     */
    protected boolean use_direct_memory_for_allocations;


    public NioMessage() {
    }

    /**
     * Constructs a message given a destination address
     * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
     *            Otherwise, it is sent to a single member.
     */
    public NioMessage(Address dest) {
        super(dest);
    }

   /**
    * Constructs a message given a destination and source address and the payload byte buffer
    * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
    *             Otherwise, it is sent to a single member.
    * @param buf The payload. Note that this buffer must not be modified (e.g. buf[0]='x' is not
    *            allowed) since we don't copy the contents.
    */
    public NioMessage(Address dest, ByteBuffer buf) {
        super(dest);
        this.buf=buf;
    }


    /** Returns the byte buffer. Do not read from/write to it, or else retransmissions will fail!
        Use {@link ByteBuffer#duplicate()} before, to create a copy, if the buffer needs to be read from */
    public ByteBuffer        getBuf()                   {return buf;}
    public NioMessage        setBuf(ByteBuffer b)       {this.buf=b; return this;}
    public Supplier<Message> create()                   {return NioMessage::new;}
    public short             getType()                  {return Message.NIO_MSG;}
    public boolean           useDirectMemory()          {return use_direct_memory_for_allocations;}
    public NioMessage        useDirectMemory(boolean b) {use_direct_memory_for_allocations=b; return this;}
    public boolean           hasPayload()               {return buf != null;}
    public boolean           hasArray()                 {return buf != null && buf.hasArray();}
    public boolean           isDirect()                 {return buf != null && buf.isDirect();}
    public int               getOffset()                {return hasArray()? buf.arrayOffset()+buf.position() : 0;}
    public int               getLength()                {return buf != null? buf.remaining() : 0;}


    public byte[] getArray() {
        return buf != null? (isDirect()?Util.bufferToArray(buf) : buf.array()) : null;
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
    public NioMessage setArray(byte[] b, int offset, int length) {
        if(b != null)
            buf=createBuffer(b, offset, length);
        return this;
    }

    /**
     * Sets the buffer<p/>
     * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
     * retransmit a changed byte[] buffer !
     */
    public NioMessage setArray(ByteArray b) {
        if(b != null)
            this.buf=createBuffer(b.getArray(), b.getOffset(), b.getLength());
        return this;
    }


    public <T extends Object> T getObject() {
        return getObject(null);
    }

    /**
     * Tries to unmarshal the byte buffer payload into an object
     * @return The object
     */
    public <T extends Object> T getObject(ClassLoader loader) {
        if(buf == null)
            return null;
        try {
            return isFlagSet(Flag.SERIALIZED)? Util.objectFromByteBuffer(buf, loader) : (T)getArray();
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Takes an object and uses Java serialization to generate the byte[] buffer which is set in the
     * message. Parameter 'obj' has to be serializable (e.g. implementing Serializable,
     * Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).
     */
    public NioMessage setObject(Object obj) {
        clearFlag(Flag.SERIALIZED);
        if(obj == null) {
            buf=null;
            return this;
        }
        if(obj instanceof byte[])
            return setArray((byte[])obj, 0, ((byte[])obj).length);
        if(obj instanceof ByteArray)
            return setArray((ByteArray)obj);
        if(obj instanceof ByteBuffer)
            return setBuf((ByteBuffer)obj);
        try {
            ByteArray tmp=Util.objectToBuffer(obj);
            setFlag(Flag.SERIALIZED);
            return setArray(tmp);
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }


    /**
     * Create a copy of the message.<br/>
     * Note that for headers, only the arrays holding references to the headers are copied, not the headers themselves !
     * The consequence is that the headers array of the copy hold the *same* references as the original, so do *not*
     * modify the headers ! If you want to change a header, copy it and call {@link NioMessage#putHeader(short,Header)} again.
     * @param copy_payload Copy the buffer
     * @param copy_headers Copy the headers
     * @return Message with specified data
     */
    public NioMessage copy(boolean copy_payload, boolean copy_headers) {
        NioMessage retval=(NioMessage)super.copy(copy_payload, copy_headers);
        retval.useDirectMemory(use_direct_memory_for_allocations);
        return retval;
    }




    /* ----------------------------------- Interface Streamable  ------------------------------- */

    public int size() {return super.size() +sizeOfPayload();}

    public String toString() {
        return String.format("%s %s", super.toString(), use_direct_memory_for_allocations? "(direct)" : "");
    }

    @Override protected Message copyPayload(Message copy) {
        if(buf != null)
            ((NioMessage)copy).buf=buf.duplicate();
        return copy;
    }

    protected int sizeOfPayload() {
        return Global.INT_SIZE + getLength() + Global.BYTE_SIZE; // for use_direct_memory_for_allocations
    }

    public void writePayload(DataOutput out) throws IOException {
        out.writeBoolean(use_direct_memory_for_allocations);
        out.writeInt(buf != null? getLength() : -1);
        if(buf != null) {
            if(!isDirect()) {
                byte[] buffer=buf.array();
                int offset=buf.arrayOffset() + buf.position(), length=buf.remaining();
                out.write(buffer, offset, length);
            }
            else {
                // We need to duplicate the buffer, or else writing its contents to the output stream would modify
                // position; this would break potential retransmission.
                // We still need a transfer buffer as there is no way to transfer contents of a ByteBuffer directly to
                // an output stream; once we have a transport that directly supports ByteBuffers, we can change this
                ByteBuffer copy=buf.duplicate();
                byte[] transfer_buf=new byte[Math.max(copy.remaining()/10, 128)];
                while(copy.remaining() > 0) {
                    int bytes=Math.min(transfer_buf.length, copy.remaining());
                    copy.get(transfer_buf, 0, bytes);
                    out.write(transfer_buf, 0, bytes);
                }
            }
        }
    }

    public void readPayload(DataInput in) throws IOException {
        use_direct_memory_for_allocations=in.readBoolean();
        int len=in.readInt();
        if(len < 0)
            return;
        // unfortunately, we cannot create a ByteBuffer and read directly into it from an input stream (no such API)
        byte[] tmp=new byte[len];
        in.readFully(tmp, 0, tmp.length);
        buf=createBuffer(tmp, 0, tmp.length);
    }

    protected ByteBuffer createBuffer(byte[] array, int offset, int length) {
        return use_direct_memory_for_allocations? Util.wrapDirect(array, offset, length)
          : ByteBuffer.wrap(array, offset, length);
    }

}
