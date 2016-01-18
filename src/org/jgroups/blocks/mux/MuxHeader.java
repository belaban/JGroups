package org.jgroups.blocks.mux;

import org.jgroups.Global;
import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Header that identifies the target handler for multiplexed dispatches.
 * @author Bela Ban
 * @author Paul Ferraro
 */
public class MuxHeader extends Header {

    private short id;

    public MuxHeader() {
    }
    
    public MuxHeader(short id) {
        this.id = id;
    }

    public short getId() {
        return id;
    }

    public int size() {
        return Global.SHORT_SIZE;
    }

    public void writeTo(DataOutput out) throws Exception {
        out.writeShort(id);
    }
    
    public void readFrom(DataInput in) throws Exception {
        id = in.readShort();
    }

    public String toString() {
        return "MuxHeader(" + id + ")";
    }
}
