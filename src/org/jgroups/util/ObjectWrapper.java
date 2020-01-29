package org.jgroups.util;

import org.jgroups.Global;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Wraps an object and its serialized form.
 * @author Bela Ban
 * @since  5.0.0
 */
public class ObjectWrapper implements SizeStreamable {
    protected Object obj;
    protected byte[] serialized; // serialized version of obj (cached)

    public ObjectWrapper() {
        // only used for deserialization
    }

    public ObjectWrapper(Object obj) {
        this.obj=Objects.requireNonNull(obj);
    }

    public <T extends Object> T getObject() {
        return (T)obj;
    }

    public synchronized ObjectWrapper setObject(Object obj) {
        this.obj=obj;
        this.serialized=null;
        return this;
    }

    public synchronized byte[] getSerialized() {
        if(serialized != null)
            return serialized;
        try {
            return serialized=Util.objectToByteBuffer(obj);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public int getLength() {
        return getSerialized().length;
    }

    public String toString() {
        return String.format("obj: %s %s", obj, serialized != null? serialized.length + " bytes" : "");
    }

    public int serializedSize() {
        int retval=Global.INT_SIZE; // length (integer)
        if(obj == null)
            return retval;
        return retval + getLength();
    }

    public void writeTo(DataOutput out) throws IOException {
        if(obj != null) {
            byte[] arr=getSerialized();
            out.writeInt(arr.length);
            out.write(arr, 0, arr.length);
        }
        else
            out.writeInt(-1);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int len=in.readInt();
        if(len == -1)
            return;
        serialized=new byte[len];
        in.readFully(serialized, 0, len);
        obj=Util.objectFromByteBuffer(serialized);
        // serialized=null;
    }


}
