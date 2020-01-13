
package org.jgroups;


import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * A {@link Message} containing an object as payload. The object needs to be serializable or externalizable, and it
 * will be serialized into a byte array when needed (e.g. when {@link #getLength()} or {@link #writeTo(DataOutput)}
 * is called.
 * <br/>
 * Note that objects that implement {@link SizeStreamable} should use the parent class {@link ObjectMessage}, as it
 * has a smaller memory footprint.
 * <br/>
 * Note that the object passed to the constructor (or set with {@link #setObject(Object)}) must not be changed after
 * the creation of an ObjectMessage, as length and serialized_obj will be cached.<br/>
 * There is simple synchronization for obj and serialized_obj.
 * @since  5.0
 * @author Bela Ban
 */
public class ObjectMessageSerializable extends ObjectMessage {
    protected byte[] serialized_obj; // contains the size of the serialized object (done just-in-time, if needed)


    public ObjectMessageSerializable(Address dest) {
        super(dest);
    }



   /**
    * Constructs a message given a destination and source address and the payload object
    * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
    *             sent to a single member.
    * @param obj The object that will be marshalled into the byte buffer. Has to be serializable (e.g. implementing
    *            Serializable, Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).
    */
    public ObjectMessageSerializable(Address dest, Object obj) {
        super(dest, obj);
    }


    public ObjectMessageSerializable() {
        super();
    }


    public ObjectMessageSerializable(boolean create_headers) {
        super(create_headers);
    }

    public Supplier<Message> create()  {return ObjectMessageSerializable::new;}
    public short             getType() {return Message.OBJ_MSG_SERIALIZABLE;}

    public synchronized <T> T getObject() {
        return super.getObject();
    }

    public synchronized ObjectMessageSerializable setObject(Object obj) {
        super.setObject(obj);
        serialized_obj=null; // in case an object was changed
        return this;
    }

    public synchronized int getLength() {
        if(obj == null)
            return 0;
        if(obj instanceof SizeStreamable)
            return Util.size((SizeStreamable)obj);

        if(serialized_obj != null)
            return serialized_obj.length;
        swizzle();
        return serialized_obj != null? serialized_obj.length : 0;
    }


    protected ObjectMessageSerializable swizzle() {
        if(serialized_obj != null || obj == null)
            return this;
        try {
            serialized_obj=Util.objectToByteBuffer(obj);
            return this;
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected synchronized int objSize() {
        int retval=Global.BYTE_SIZE; // is it a SizeStreamable?
        if(obj instanceof SizeStreamable)
            return retval + super.objSize();
        retval+=Global.INT_SIZE; // length (integer)
        if(obj == null)
            return retval;
        if(serialized_obj == null)
            swizzle();
        return retval + (serialized_obj != null? serialized_obj.length : 0); // number of bytes in the buffer
    }

    protected Object check(Object obj) {
        return obj;
    }

    /* ----------------------------------- Interface Streamable  ------------------------------- */



    @Override protected synchronized void writePayload(DataOutput out) throws IOException {
        out.writeBoolean(obj instanceof SizeStreamable);
        if(obj instanceof SizeStreamable) {
            Util.writeGenericStreamable((Streamable)obj, out);
            return;
        }
        if(obj != null) {
            if(serialized_obj == null)
                swizzle();
            out.writeInt(serialized_obj.length);
            out.write(serialized_obj, 0, serialized_obj.length);
        }
        else
            out.writeInt(-1);
    }

    @Override protected synchronized void readPayload(DataInput in) throws IOException, ClassNotFoundException {
        boolean streamable=in.readBoolean();
        if(streamable)
            obj=Util.readGenericStreamable(in);
        else {
            int len=in.readInt();
            if(len == -1)
                return;
            serialized_obj=new byte[len];
            in.readFully(serialized_obj, 0, len);
            obj=Util.objectFromByteBuffer(serialized_obj);
            serialized_obj=null;
        }
    }


    /* --------------------------------- End of Interface Streamable ----------------------------- */


    public String toString() {
        return super.toString()
          + String.format(", obj: %s, serialized size: %d", obj, serialized_obj != null? serialized_obj.length : 0);
    }
}
