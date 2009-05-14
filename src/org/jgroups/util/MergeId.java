package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;

/** ID to uniquely identify a merge
 * @author Bela Ban
 * @version $Id: MergeId.java,v 1.2 2009/05/14 15:20:34 belaban Exp $
 */
public class MergeId implements Streamable {
    private Address initiator; // must be non-null
    private int id;
    private static int LAST_ID=1;

    public MergeId() {}

    private MergeId(Address initiator, int id) {
        this.initiator=initiator;
        this.id=id;
    }

    public synchronized static MergeId create(Address addr) {
        int id=LAST_ID++;
        if(addr == null)
            throw new IllegalArgumentException("initiator has to be non null");
        return new MergeId(addr, id);
    }

    public boolean equals(Object obj) {
        return obj instanceof MergeId && initiator.equals(((MergeId)obj).initiator) && id == ((MergeId)obj).id;
    }

    public int hashCode() {
        return initiator.hashCode() + id;
    }

    public int size() {
        return Util.size(initiator) + Global.INT_SIZE;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeAddress(initiator, out);
        out.writeInt(id);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        initiator=Util.readAddress(in);
        id=in.readInt();
    }

    public String toString() {
        return initiator + "::" + id;
    }
}
