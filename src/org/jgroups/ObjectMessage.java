
package org.jgroups;


import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * A {@link Message} with an object as payload. The object won't get serialized until sent by the transport.
 * <p>
 * Note that the object passed to the constructor (or set with {@link #setObject(Object)}) must not be changed after
 * the creation of an {@link ObjectMessage}.
 * @since  5.0
 * @author Bela Ban
 */
public class ObjectMessage extends BaseMessage {
    // can be null or a SizeStreamable or an ObjectWrapper (also SizeStreamable)
    protected SizeStreamable obj;


    public ObjectMessage() {
        this(null);
    }

    /**
    * Constructs a message given a destination address
    * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
    *             sent to a single member.
    */
    public ObjectMessage(Address dest) {
        this(dest, null);
    }



   /**
    * Constructs a message given a destination address and the payload object
    * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
    *             sent to a single member.
    * @param obj To be used as payload.
    */
    public ObjectMessage(Address dest, Object obj) {
        super(dest);
        setObject(obj);
    }

    /**
     * Constructs a message given a destination address and the payload object
     * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
     *             sent to a single member.
     * @param obj The {@link SizeStreamable} object to be used as payload. Note that this constructor has fewer
     *            checks (e.g. instanceof) than {@link ObjectMessage(Address, Object)}.
     */
    public ObjectMessage(Address dest, SizeStreamable obj) {
        super(dest);
        setObject(obj);
    }



    public Supplier<Message> create()                             {return ObjectMessage::new;}
    public short             getType()                            {return Message.OBJ_MSG;}
    public boolean           hasPayload()                         {return obj != null;}
    public boolean           hasArray()                           {return false;}
    public int               getOffset()                          {return 0;}
    public int               getLength()                          {return obj != null? objSize() : 0;}
    public byte[]            getArray()                           {throw new UnsupportedOperationException();}
    public ObjectMessage     setArray(byte[] b, int off, int len) {throw new UnsupportedOperationException();}
    public ObjectMessage     setArray(ByteArray buf)              {throw new UnsupportedOperationException();}
    public boolean           isWrapped()                          {return isFlagSet(Flag.SERIALIZED);}

    // reusing SERIALIZABLE
    public ObjectMessage setWrapped(boolean b) {
        if(b) setFlag(Flag.SERIALIZED);
        else  clearFlag(Flag.SERIALIZED);
        return this;
    }


    /** Sets the object. If the object doesn't implement {@link SizeStreamable}, or is a primitive type,
     * it will be wrapped into an {@link ObjectWrapperSerializable} (which does implement SizeStreamable)
     */
    public ObjectMessage setObject(Object obj) {
        if(obj == null) {
            this.obj=null;
            setWrapped(false);
            return this;
        }

        if(Util.isPrimitiveType(obj)) {
            this.obj=new ObjectWrapperPrimitive(obj);
            setWrapped(true); // primitive is the default
            return this;
        }
        if(obj instanceof SizeStreamable) {
            this.obj=(SizeStreamable)obj;
            setWrapped(false);
        }
        else {
            this.obj=new ObjectWrapperSerializable(obj); // ObjectWrapper implements SizeStreamable
            setWrapped(true);
        }
        return this;
    }

    /**
     * Sets the payload to a {@link SizeStreamable} (or null). This method has fewer checks (e.g. instanceof) than
     * {@link #setObject(Object)}.
     * @param obj The {@link SizeStreamable} to be used as payload (or null)
     */
    public ObjectMessage setObject(SizeStreamable obj) {
        setWrapped(false);
        if(obj == null) {
            this.obj=null;
            return this;
        }
        this.obj=obj;
        return this;
    }

    public ObjectMessage setSizeStreamable(SizeStreamable s) {
        return setObject(s);
    }

    public <T extends Object> T getObject() {
        if(obj == null)
            return null;
        return isWrapped() || obj instanceof ObjectWrapperPrimitive? ((ObjectWrapperPrimitive)obj).getObject() : (T)obj;
    }

    public int size() {
        return super.size() + objSize();
    }

    public void writePayload(DataOutput out) throws IOException {
        Util.writeGenericStreamable(obj, out);
    }

    public void readPayload(DataInput in) throws IOException, ClassNotFoundException {
        this.obj=Util.readSizeStreamable(in, null);
    }

    @Override protected Message copyPayload(Message copy) {
        if(obj != null)
            ((ObjectMessage)copy).setObject(obj);
        return copy;
    }

    public String toString() {
        return super.toString() + String.format(", obj: %s", obj);
    }

    protected int objSize() {
        return Util.size(obj);
    }
}
