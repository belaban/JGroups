package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.Global;
import org.jgroups.util.Streamable;

import java.io.*;

/**
 * @author Bela Ban
 * @version $Id: FragHeader.java,v 1.2 2005/04/15 13:17:02 belaban Exp $
 */
public class FragHeader extends Header implements Streamable {
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

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(id);
        out.writeInt(frag_id);
        out.writeInt(num_frags);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id=in.readLong();
        frag_id=in.readInt();
        num_frags=in.readInt();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(id);
        out.writeInt(frag_id);
        out.writeInt(num_frags);
    }

    public long size() {
        return Global.LONG_SIZE + 2*Global.INT_SIZE;
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        id=in.readLong();
        frag_id=in.readInt();
        num_frags=in.readInt();
    }

}
