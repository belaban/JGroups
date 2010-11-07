package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;

import java.io.*;

/**
 * @author Bela Ban
 * @version $Id: FragHeader.java,v 1.5 2010/06/15 06:44:35 belaban Exp $
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


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(id);
        out.writeInt(frag_id);
        out.writeInt(num_frags);
    }

    public int size() {
        return Global.LONG_SIZE + 2*Global.INT_SIZE;
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        id=in.readLong();
        frag_id=in.readInt();
        num_frags=in.readInt();
    }

}
