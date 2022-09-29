package org.jgroups.protocols;

import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  5.2.7
 */
public class ProtPerfHeader extends Header {
    public static final short ID=1505; // dummy protocol ID to get the ProtPerfHeader from a message

    protected long start_down; // us
    protected long start_up;   // us


    public ProtPerfHeader() {
    }

    public Supplier<? extends Header> create()          {return ProtPerfHeader::new;}
    public short                      getMagicId()      {return 98;}
    public int                        serializedSize()  {return 0;}
    public long                       startDown()       {return start_down;}
    public ProtPerfHeader             startDown(long t) {this.start_down=t; return this;}
    public long                       startUp()         {return start_up;}
    public ProtPerfHeader             startUp(long t)   {this.start_up=t; return this;}


    /** The contents of this header is not serialized */
    public void writeTo(DataOutput out) throws IOException {
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
    }
}
