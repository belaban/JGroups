package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;

import java.io.DataInput;
import java.io.DataOutput;

/** ID to uniquely identify a merge
 * @author Bela Ban
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
        if(addr == null)
            throw new IllegalArgumentException("initiator has to be non null");

        int id=LAST_ID++;
        return new MergeId(addr, id);
    }

    public boolean equals(Object obj) {
        return this.getClass() == obj && initiator.equals(((MergeId)obj).initiator) && id == ((MergeId)obj).id;
    }

    public int hashCode() {
        return initiator.hashCode() + id;
    }

    public int size() {
        return Util.size(initiator) + Global.INT_SIZE;
    }

    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(initiator, out);
        out.writeInt(id);
    }

    public void readFrom(DataInput in) throws Exception {
        initiator=Util.readAddress(in);
        id=in.readInt();
    }

    public String toString() {
        return initiator + "::" + id;
    }
}
