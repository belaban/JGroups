package org.jgroups.util;

import org.jgroups.Global;

import java.io.*;
import java.util.Arrays;

/**
 * Subclass of {@link UUID} accommodating additional data such as a flag and a small hashmap. There may be many instances
 * in memory, and as they are serialized a lot and sent across the wire, I tried to make this as compact as possible.
 * As a consequence, the hashmap can have a max size of 255 and a value can have a max length of 255 bytes.
 * @author Bela Ban
 * @since  3.5
 */
public class ExtendedUUID extends UUID {
    private static final long serialVersionUID=-2335117576152990301L;
    protected short           flags;
    protected byte[][]        keys;
    protected byte[][]        values;

    // reserved flags
    public static final short site_master            = 1 << 0;
    public static final short can_become_site_master = 1 << 1;

    public ExtendedUUID() {
        super();
    }

    protected ExtendedUUID(byte[] data) {
        super(data);
    }

    public ExtendedUUID(long mostSigBits, long leastSigBits) {
        super(mostSigBits,leastSigBits);
    }

    public ExtendedUUID(ExtendedUUID other) {
        super(other.mostSigBits, other.leastSigBits);
        flags=other.flags;
        if(other.keys != null) {
            keys=Arrays.copyOf(other.keys, other.keys.length);
            values=Arrays.copyOf(other.values, other.values.length);
        }
    }

    public static ExtendedUUID randomUUID() {
        return new ExtendedUUID(generateRandomBytes());
    }

    public static ExtendedUUID randomUUID(String name) {
        ExtendedUUID retval=new ExtendedUUID(generateRandomBytes());
        if(name != null)
            UUID.add(retval, name);
        return retval;
    }

    public ExtendedUUID setFlag(short flag) {
        flags |= flag; return this;
    }

    public ExtendedUUID clearFlag(short flag) {
        flags &= ~flag; return this;
    }

    public boolean isFlagSet(short flag) {
        return (flags & flag) == flag;
    }

    public byte[] get(byte[] key) {
        if(keys == null || key == null)
            return null;
        for(int i=0; i < keys.length; i++) {
            byte[] k=keys[i];
            if(k != null && Arrays.equals(k, key))
                return values[i];
        }
        return null;
    }

    public byte[] get(String key) {
        return get(Util.stringToBytes(key));
    }

    public ExtendedUUID put(byte[] key, byte[] val) {
        return put(0, key, val);
    }

    protected ExtendedUUID put(int start_index, byte[] key, byte[] val) {
        if(val != null && val.length > 0xff)
            throw new IllegalArgumentException("value has to be <= " + 0xff + " bytes");
        if(keys == null)
            resize(3);

        for(int i=start_index; i < keys.length; i++) {
            byte[] k=keys[i];
            if(k == null || Arrays.equals(key, k)) {
                keys[i]=key;
                values[i]=val;
                return this;
            }
        }

        int index=keys.length;
        resize(keys.length + 3);
        return put(index, key, val);
    }

    public ExtendedUUID put(String key, byte[] val) {
        return put(Util.stringToBytes(key), val);
    }

    public byte[] remove(byte[] key) {
        if(keys == null || key == null)
            return null;
        for(int i=0; i < keys.length; i++) {
            byte[] k=keys[i];
            if(k != null && Arrays.equals(k, key)) {
                byte[] old_val=values[i];
                keys[i]=values[i]=null;
                return old_val;
            }
        }
        return null;
    }

    public byte[] remove(String key) {
        return remove(Util.stringToBytes(key));
    }

    public boolean keyExists(byte[] key) {
        if(keys == null || key == null)
            return false;
        for(int i=0; i < keys.length; i++) {
            byte[] k=keys[i];
            if(k != null && Arrays.equals(k, key))
                return true;
        }
        return false;
    }

    public boolean keyExists(String key) {
        return keyExists(Util.stringToBytes(key));
    }

    public ExtendedUUID addContents(ExtendedUUID other) {
        flags|=other.flags;
        if(other.keys != null) {
            for(int i=0; i < other.keys.length; i++) {
                byte[] key=other.keys[i];
                byte[] val=other.values[i];
                if(!keyExists(key))
                    put(key, val); // overwrite
            }
        }
        return this;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        write(out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        read(in);
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        write(out);
    }



    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        read(in);
    }

    /** The number of bytes required to serialize this instance */
    public int size() {
        return super.size() + Global.SHORT_SIZE + Global.BYTE_SIZE + sizeofHashMap();
    }

    /** The number of non-null keys */
    public int length() {
        if(keys == null)
            return 0;
        int retval=0;
        for(byte[] key: keys)
            if(key != null)
                retval++;
        return retval;
    }

    public String toString() {return super.toString();}


    public String print() {
        StringBuilder sb=new StringBuilder(super.toString());
        if(flags != 0 || keys != null)
            sb.append(" (");
        if(flags != 0)
            sb.append("flags=" + flags + " (" + flagsToString() + ")");
        if(keys == null)
            return sb.toString();
        for(int i=0; i < keys.length; i++) {
            byte[] key=keys[i];
            if(key == null)
                continue;
            byte[] val=values[i];
            Object obj=val != null && val.length >= Util.MAX_LIST_PRINT_SIZE ? val.length + " bytes" : null;
            if(val != null && val.length <= Util.MAX_LIST_PRINT_SIZE) {
                if(obj == null) {
                    try {
                        obj=Util.bytesToString(val);
                    }
                    catch(Throwable t) {
                        obj=val != null? val.length + " bytes" : null;
                    }
                }
            }

            sb.append(", ").append(new AsciiString(key)).append("=").append(obj);
        }
        if(flags != 0 || keys != null)
            sb.append(")");
        return sb.toString();
    }

    protected void write(DataOutput out) throws IOException {
        out.writeShort(flags);
        int length=length();
        out.writeByte(length);
        if(keys == null)
            return;

        for(int i=0; i < keys.length; i++) {
            byte[] k=keys[i];
            if(k != null) {
                out.writeByte(k.length);
                out.write(k);
                byte[] v=values[i];
                out.writeByte(v != null? v.length : 0);
                if(v != null)
                    out.write(v);
            }
        }
    }

    protected void read(DataInput in) throws IOException {
        flags=in.readShort();
        int length=in.readUnsignedByte();
        if(length == 0)
            return;
        resize(length);

        for(int i=0; i < length; i++) {
            int len=in.readUnsignedByte();
            keys[i]=new byte[len];
            in.readFully(keys[i]);
            len=in.readUnsignedByte();
            if(len > 0) {
                values[i]=new byte[len];
                in.readFully(values[i]);
            }
        }
    }


    protected int sizeofHashMap() {
        if(keys == null) return 0;
        int retval=0;
        for(int i=0; i < keys.length; i++) {
            byte[] key=keys[i];
            if(key == null)
                continue;
            retval+=key.length + Global.BYTE_SIZE; // length

            byte[] val=values[i];
            retval+=Global.BYTE_SIZE + (val != null? val.length : 0);
        }
        return retval;
    }

    // Resizes the arrays
    protected void resize(int new_length) {
        if(keys == null) {
            keys=new byte[Math.min(new_length, 0xff)][];
            values=new byte[Math.min(new_length, 0xff)][];
            return;
        }
        if(new_length > 0xff) {
            if(keys.length < 0xff)
                new_length=0xff;
            else
                throw new ArrayIndexOutOfBoundsException("the hashmap cannot exceed " + 0xff + " entries");
        }

        keys=Arrays.copyOf(keys, new_length);
        values=Arrays.copyOf(values, new_length);
    }

    protected String flagsToString() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(Tuple<Short,String> flag: Arrays.asList(new Tuple<>(site_master, "sm"),
                                                    new Tuple<>(can_become_site_master, "can_be_sm"))) {
            if(isFlagSet(flag.getVal1())) {
                if(first)
                    first=false;
                else
                    sb.append("|");
                sb.append(flag.getVal2());
            }
        }
        return sb.toString();
    }
}
