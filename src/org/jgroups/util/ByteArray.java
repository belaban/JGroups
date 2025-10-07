package org.jgroups.util;

/**
 * Byte array with an offset and length.<br/>
 * Note that the underlying byte array must <em>not</em> be changed as long as this instance is in use !
 * @author Bela Ban
 */
public record ByteArray(byte[] array, int offset, int length) {

    public ByteArray(byte[] array) {
        this(array, 0, array.length);
    }

    public ByteArray copy() {
        byte[] new_buf=array != null? new byte[length] : null;
        int new_length=new_buf != null? new_buf.length : 0;
        if(new_buf != null)
            System.arraycopy(array, offset, new_buf, 0, length);
        return new ByteArray(new_buf, 0, new_length);
    }

    public byte[] bytes() {
        if(array == null) return null;
        if(offset == 0 && length == array.length)
            return array;
        byte[] tmp=new byte[length];
        System.arraycopy(array, offset, tmp, 0, length);
        return tmp;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(length).append(" bytes");
        if(offset > 0)
            sb.append(" (offset=").append(offset).append(")");
        return sb.toString();
    }

}
