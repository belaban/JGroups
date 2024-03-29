package org.jgroups.util;

import org.jgroups.Global;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps an object and its serialized form.
 * @author Bela Ban
 * @since  5.0.0
 */
public class ObjectWrapperSerializable extends ObjectWrapperPrimitive {
    protected ByteArray serialized; // serialized version of obj (cached)

    public ObjectWrapperSerializable() {
        // only used for deserialization
    }

    public ObjectWrapperSerializable(Object obj) {
        super(obj);
    }

    public synchronized ObjectWrapperSerializable setObject(Object obj) {
        super.setObject(obj);
        this.serialized=null;
        return this;
    }

    public synchronized ByteArray getSerialized() {
        if(serialized != null)
            return serialized;
        try {
            return serialized=Util.objectToBuffer(obj);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public int getLength() {
        return getSerialized().getLength();
    }

    public String toString() {
        return String.format("%s%s", obj, serialized != null? "( " + serialized.getLength() + " bytes)" : "");
    }

    public int serializedSize() {
        int retval=Global.INT_SIZE; // length (integer)
        if(obj == null)
            return retval;
        return retval + getLength();
    }

    public void writeTo(DataOutput out) throws IOException {
        if(obj != null) {
            ByteArray arr=getSerialized();
            out.writeInt(arr.getLength());
            out.write(arr.getArray(), 0, arr.getLength());
        }
        else
            out.writeInt(-1);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int len=in.readInt();
        if(len == -1)
            return;
        byte[] tmp=new byte[len];
        in.readFully(tmp, 0, len);
        serialized=new ByteArray(tmp);
        obj=Util.objectFromBuffer(serialized, null);
        // serialized=null;
    }


}
