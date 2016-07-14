package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.Supplier;


/**
 * @author Bela Ban
 */
public class FragHeader extends Header {
    public long id;
    public int  frag_id;
    public int  num_frags;


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

    public String toString() {
        return "[id=" + id + ", frag_id=" + frag_id + ", num_frags=" + num_frags + ']';
    }


    public void writeTo(DataOutput out) throws Exception {
        Bits.writeLong(id,out);
        Bits.writeInt(frag_id, out);
        Bits.writeInt(num_frags, out);
    }

    public int size() {
        return Bits.size(id) + Bits.size(frag_id) + Bits.size(num_frags);
    }

    public void readFrom(DataInput in) throws Exception {
        id=Bits.readLong(in);
        frag_id=Bits.readInt(in);
        num_frags=Bits.readInt(in);
    }

}
