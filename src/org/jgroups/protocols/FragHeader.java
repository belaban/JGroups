package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;


/**
 * @author Bela Ban
 */
public class FragHeader extends Header {
    public long    id;
    public int     frag_id;
    public int     num_frags;
    public boolean needs_deserialization; // true if byte[] array of a fragment needs to be de-serialized into a payload
    public short   original_type;


    public FragHeader() {
    } // used for externalization

    public FragHeader(long id, int frag_id, int num_frags) {
        this.id=id;
        this.frag_id=frag_id;
        this.num_frags=num_frags;
    }

    public short getMagicId() {return 52;}

    public Supplier<? extends Header> create() {
        return FragHeader::new;
    }
    public boolean    needsDeserialization()             {return needs_deserialization;}
    public FragHeader needsDeserialization(boolean flag) {needs_deserialization=flag; return this;}
    public short      getOriginalType()                  {return original_type;}
    public FragHeader setOriginalType(short type)        {this.original_type=type; return this;}


    public String toString() {
        return "[id=" + id + ", frag_id=" + frag_id + ", num_frags=" + num_frags + ']';
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeLongCompressed(id, out);
        Bits.writeIntCompressed(frag_id, out);
        Bits.writeIntCompressed(num_frags, out);
        out.writeBoolean(needs_deserialization);
        out.writeShort(original_type);
    }

    @Override
    public int serializedSize() {
        return Bits.size(id) + Bits.size(frag_id) + Bits.size(num_frags) + Global.BYTE_SIZE + Global.SHORT_SIZE;
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        id=Bits.readLongCompressed(in);
        frag_id=Bits.readIntCompressed(in);
        num_frags=Bits.readIntCompressed(in);
        needs_deserialization=in.readBoolean();
        original_type=in.readShort();
    }

}
