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
 * @version $Id: MuxHeader.java,v 1.1 2010/04/13 17:57:06 ferraro Exp $
 */
public class MuxHeader extends Header {

    public static final short ID=1500;
    
    private short id;

    public MuxHeader() {
    }
    
    public MuxHeader(short id) {
        this.id = id;
    }

    public short getId() {
        return id;
    }

    @Override
    public int size() {
        return Global.SHORT_SIZE;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(id);
    }
    
    @Override
    public void readFrom(DataInputStream in) throws IOException {
	id = in.readShort();
    }
}
