package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.*;

/**
 * @author Bela Ban
 */
public class FragHeader extends Header {
    public long id=0;
    public int  frag_id=0;
    public int  num_frags=0;


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
        Util.writeLong(id, out);
        out.writeInt(frag_id);
        out.writeInt(num_frags);
    }

    public int size() {
        return Util.size(id) + 2*Global.INT_SIZE;
    }

    public void readFrom(DataInput in) throws Exception {
        id=Util.readLong(in);
        frag_id=in.readInt();
        num_frags=in.readInt();
    }

}
