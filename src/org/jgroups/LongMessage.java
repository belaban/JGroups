package org.jgroups;

import org.jgroups.util.Bits;
import org.jgroups.util.ByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Message with a long as payload. Can be used to send int- or long values, without having to resort to
 * {@link BytesMessage}.
 * @author Bela Ban
 * @since  5.0.0
 */
public class LongMessage extends BaseMessage {
    protected long value;

    public LongMessage() {
    }

    public LongMessage(Address dest, long v) {super(dest); this.value=v;}

    public long        getValue()                       {return value;}
    public LongMessage setValue(long v)                 {this.value=v; return this;}
    public short       getType()                        {return Message.LONG_MSG;}
    public boolean     hasPayload()                     {return true;}
    public boolean     hasArray()                       {return false;}
    public byte[]      getArray()                       {throw new UnsupportedOperationException();}
    public int         getOffset()                      {throw new UnsupportedOperationException();}
    public int         getLength()                      {return Bits.size(value);}
    public LongMessage setArray(byte[] b, int o, int l) {throw new UnsupportedOperationException();}
    public LongMessage setArray(ByteArray b)            {throw new UnsupportedOperationException();}
    public <T> T       getObject()                      {return (T)(Long)value;}

    /**
     * Sets the value
     * @param obj Has to be a Number (int or long)
     */
    public LongMessage setObject(Object obj) {
        Number v=(Number)obj;
        value=v.longValue(); return this;
    }

    public Supplier<? extends Message> create() {
        return LongMessage::new;
    }

    protected Message copyPayload(Message copy) {
        ((LongMessage)copy).setValue(value);
        return copy;
    }

    public void writePayload(DataOutput out) throws IOException {
        Bits.writeLongCompressed(value, out);
    }

    public void readPayload(DataInput in) throws IOException, ClassNotFoundException {
        value=Bits.readLongCompressed(in);
    }

    public int size() {
        return super.size() + Bits.size(value);
    }

    public String toString() {
        return super.toString() + " value=" + value;
    }
}
