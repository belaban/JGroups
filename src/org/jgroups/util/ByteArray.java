package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Byte array with an offset and length.<br/>
 * Note that the underlying byte array must <em>not</em> be changed as long as this instance is in use !
 * @author Bela Ban
 */
public class ByteArray implements SizeStreamable {
    private byte[] array;
    private int    offset;
    private int    length;

    public ByteArray(byte[] array, int offset, int length) {
        this.array=array;
        this.offset=offset;
        this.length=length;
    }

    public ByteArray(byte[] array) {
        this(array, 0, array.length);
    }

    public byte[] getArray()  {return array;}
    public int    getOffset() {return offset;}
    public int    getLength() {return length;}

    public ByteArray copy() {
        byte[] new_buf=array != null? new byte[length] : null;
        int new_length=new_buf != null? new_buf.length : 0;
        if(new_buf != null)
            System.arraycopy(array, offset, new_buf, 0, length);
        return new ByteArray(new_buf, 0, new_length);
    }

    public byte[] getBytes() {
        if(array == null) return null;
        if(offset == 0 && length == array.length)
            return array;
        byte[] tmp=new byte[length];
        System.arraycopy(array, offset, tmp, 0, length);
        return tmp;
    }

    @Override
    public int serializedSize() {
        return Bits.size(length) + length;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeIntCompressed(length, out);
        if(length > 0)
            out.write(array, offset, length);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        length=Bits.readIntCompressed(in);
        offset=0;
        if(length > 0) {
            array=new byte[length];
            in.readFully(array);
        }
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(length).append(" bytes");
        if(offset > 0)
            sb.append(" (offset=").append(offset).append(")");
        return sb.toString();
    }


}
