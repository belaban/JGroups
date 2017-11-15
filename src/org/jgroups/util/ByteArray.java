package org.jgroups.util;

/**
 * Byte array with an offset and length. This class is immutable.<br/>
 * Note that the underlying byte array must <em>not</em> be changed as long as this instance is in use !
 * @author Bela Ban
 */
public class ByteArray {
    private final byte[] array;
    private final int    offset;
    private final int    length;

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

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(length).append(" bytes");
        if(offset > 0)
            sb.append(" (offset=").append(offset).append(")");
        return sb.toString();
    }

}
