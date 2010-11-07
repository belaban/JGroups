package org.jgroups.blocks.mux;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.jgroups.Global;
import org.jgroups.Header;

/**
 * Header that identifies the target handler for multiplexed dispatches.
 * @author Bela Ban
 * @author Paul Ferraro
 * @version $Id: MuxHeader.java,v 1.3 2010/06/15 06:44:40 belaban Exp $
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

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(id);
    }
    
    public void readFrom(DataInputStream in) throws IOException {
        id = in.readShort();
    }
}
