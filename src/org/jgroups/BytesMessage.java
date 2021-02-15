
package org.jgroups;


import org.jgroups.util.ByteArray;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * A {@link Message} containing a byte array as payload.
 * <br/>
 * The byte array can point to a reference, and we can subset it using index and length. When the message is serialized,
 * only the bytes between index and length are written.
 *
 * @since  5.0
 * @author Bela Ban
 */
public class BytesMessage extends BaseMessage {

    /** The payload */
    protected byte[]            array;

    /** The index into the payload */
    protected int               offset;

    /** The number of bytes in the array */
    protected int               length;


    public BytesMessage() {
    }

    /**
    * Constructs a message given a destination address
    * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
     *            Otherwise, it is sent to a single member.
    */
    public BytesMessage(Address dest) {
        super(dest);
    }

   /**
    * Constructs a message given a destination and source address and the payload byte array
    * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
    *             Otherwise, it is sent to a single member.
    * @param array The payload. Note that this array must not be modified (e.g. buf[0]='x' is not
    *              allowed) since we don't copy the contents.
    */
    public BytesMessage(Address dest, byte[] array) {
        this(dest, array, 0, array != null? array.length : 0);
    }


   /**
    * Constructs a message. The index and length parameters provide a reference to a byte array, rather than a copy,
    * and refer to a subset of the array. This is important when we want to avoid copying. When the message is
    * serialized, only the subset is serialized.</p>
    * <em>
    * Note that the byte array passed as argument must not be modified. Reason: if we retransmit the
    * message, it would still have a ref to the original byte array passed in as argument, and so we would
    * retransmit a changed byte array !
    * </em>
    *
    * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
    *             Otherwise, it is sent to a single member.
    * @param array A reference to a byte array
    * @param offset The index into the byte array
    * @param length The number of bytes to be used from <tt>buf</tt>. Both index and length are checked
    *               for array index violations and an ArrayIndexOutOfBoundsException will be thrown if invalid
    */
    public BytesMessage(Address dest, byte[] array, int offset, int length) {
        super(dest);
        setArray(array, offset, length);
    }


    public BytesMessage(Address dest, ByteArray array) {
        super(dest);
        setArray(array);
    }


   /**
    * Constructs a message given a destination and source address and the payload object
    * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
    *             Otherwise, it is sent to a single member.
    * @param obj The object that will be marshalled into the byte array. Has to be serializable (e.g. implementing
    *            Serializable, Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).
    */
    public BytesMessage(Address dest, Object obj) {
        super(dest);
        setObject(obj);
    }



    public BytesMessage setFlag(Flag... flags) {
        super.setFlag(flags);
        return this;
    }


    public Supplier<Message> create()      {return BytesMessage::new;}
    public short             getType()     {return Message.BYTES_MSG;}
    public boolean           hasPayload()  {return array != null;}
    public boolean           hasArray()    {return true;}
    public int               getOffset()   {return offset;}
    public int               getLength()   {return length;}


    /**
     * Returns a <em>reference</em> to the payload (byte array). Note that this array should not be
     * modified as we do not copy the array on copy() or clone(): the array of the copied message
     * is simply a reference to the old array.<br/>
     * Even if offset and length are used: we return the <em>entire</em> array, not a subset.
     */
    public byte[]            getArray()     {return array;}



    /**
     * Sets the internal array to point to a subset of a given array.<p/>
     * <em>
     * Note that the byte array passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte array passed in as argument, and so we would
     * retransmit a changed byte array !
     * </em>
     *
     * @param b The reference to a given array. If null, we'll reset the array to null
     * @param offset The initial position
     * @param length The number of bytes
     */
    public BytesMessage setArray(byte[] b, int offset, int length) {
        array=b;
        if(array != null) {
            if(offset < 0 || offset > array.length)
                throw new ArrayIndexOutOfBoundsException(offset);
            if((offset + length) > array.length)
                throw new ArrayIndexOutOfBoundsException((offset+length));
            this.offset=offset;
            this.length=length;
        }
        else
            this.offset=this.length=0;
        return this;
    }

    /**
     * Sets the array<p/>
     * Note that the byte array passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte array passed in as argument, and so we would
     * retransmit a changed byte array !
     */
    public BytesMessage setArray(ByteArray buf) {
        if(buf != null) {
            this.array=buf.getArray();
            this.offset=buf.getOffset();
            this.length=buf.getLength();
        }
        return this;
    }



    /**
     * Takes an object and uses Java serialization to generate the byte array which is set in the
     * message. Parameter 'obj' has to be serializable (e.g. implementing Serializable,
     * Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).
     */
    public BytesMessage setObject(Object obj) {
        clearFlag(Flag.SERIALIZED);
        if(obj == null) {
            array=null;
            offset=length=0;
            return this;
        }
        if(obj instanceof byte[])
            return setArray((byte[])obj, 0, ((byte[])obj).length);
        if(obj instanceof ByteArray)
            return setArray((ByteArray)obj);
        if(obj instanceof ByteBuffer) {
            ByteBuffer bb=(ByteBuffer)obj;
            if(bb.isDirect())
                return (BytesMessage)setArray(Util.bufferToArray(bb));
            else
                return setArray(bb.array(), bb.arrayOffset()+bb.position(), bb.remaining());
        }
        try {
            ByteArray tmp=Util.objectToBuffer(obj);
            setFlag(Flag.SERIALIZED);
            return setArray(tmp);
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }


    public <T extends Object> T getObject() {
        return getObject(null);
    }

    /**
     * Uses custom serialization to create an object from the array of the message. Note that this is dangerous when
     * using your own classloader, e.g. inside of an application server ! Most likely, JGroups will use the system
     * classloader to deserialize the array into an object, whereas (for example) a web application will want to use
     * the webapp's classloader, resulting in a ClassCastException. The recommended way is for the application to use
     * their own serialization and only pass byte array to JGroups.<p/>
     * As of 3.5, a classloader can be passed in. It will be used first to find a class, before contacting
     * the other classloaders in the list. If null, the default list of classloaders will be used.
     * @return the object
     */
    public <T extends Object> T getObject(ClassLoader loader) {
        if(array == null)
            return null;
        try {
            return isFlagSet(Flag.SERIALIZED)? Util.objectFromByteBuffer(array, offset, length, loader) : (T)getArray();
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }


    public int size() {
        return super.size() +sizeOfPayload();
    }


    /**
     * Copies the byte array. If offset and length are used (to refer to another array), the copy will contain only
     * the subset that offset and length point to, copying the subset into the new copy.<p/>
     * Note that for headers, only the arrays holding references to the headers are copied, not the headers themselves !
     * The consequence is that the headers array of the copy hold the *same* references as the original, so do *not*
     * modify the headers ! If you want to change a header, copy it and call {@link BytesMessage#putHeader(short,Header)} again.
     */
    @Override protected Message copyPayload(Message copy) {
        if(array != null)
            copy.setArray(array, offset, length);
        return copy;
    }

    protected int sizeOfPayload() {
        int retval=Global.INT_SIZE; // length
        if(array != null)
            retval+=length;         // number of bytes in the array
        return retval;
    }

    public void writePayload(DataOutput out) throws IOException {
        out.writeInt(array != null? length : -1);
        if(array != null)
            out.write(array, offset, length);
    }

    public void readPayload(DataInput in) throws IOException {
        int len=in.readInt();
        if(len >= 0) {
            array=new byte[len];
            in.readFully(array, 0, len);
            length=len;
        }
    }

    protected <T extends BytesMessage> T createMessage() {
        return (T)new BytesMessage();
    }



}
