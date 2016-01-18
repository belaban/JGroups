package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.AbstractHeader;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;


/**
 * @author Bela Ban
 */
public class FragHeader extends AbstractHeader {
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

    public String toString() {
        return "[id=" + id + ", frag_id=" + frag_id + ", num_frags=" + num_frags + ']';
    }


    public void writeTo(DataOutput out) throws Exception {
        Bits.writeLong(id,out);
        out.writeInt(frag_id);
        out.writeInt(num_frags);
    }

    public int size() {
        return Bits.size(id) + 2*Global.INT_SIZE;
    }

    public void readFrom(DataInput in) throws Exception {
        id=Bits.readLong(in);
        frag_id=in.readInt();
        num_frags=in.readInt();
    }

}
