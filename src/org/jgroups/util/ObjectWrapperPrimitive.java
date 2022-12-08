package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps a primitive object (e.g. Integer, Boolean, byte[], String etc)
 * @author Bela Ban
 * @since  5.0.0
 */
public class ObjectWrapperPrimitive implements SizeStreamable {
    protected Object obj;

    public ObjectWrapperPrimitive() {
    }

    public ObjectWrapperPrimitive(Object obj) {
        this.obj=obj;
    }

    public <T extends Object> T getObject() {
        return (T)obj;
    }

    public synchronized ObjectWrapperPrimitive setObject(Object obj) {
        this.obj=obj;
        return this;
    }

    public int getLength() {
        return Util.sizePrimitive(obj);
    }

    public String toString() {
        return String.format("%s", obj);
    }

    public int serializedSize() {
        return Util.sizePrimitive(obj);
    }

    public void writeTo(DataOutput out) throws IOException {
        Util.primitiveToStream(obj, out);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        obj=Util.primitiveFromStream(in);
    }


}
