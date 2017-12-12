package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;


/**
 * @author Bela Ban
 */
public class Frag3Header extends Header {
    protected int  id;              // unique sequence number - same for all fragments of a given original message
    protected int  frag_id;         // the ID of the frag, starting with 0. E.g. 4 fragments will have IDs from 0 to 3
    protected int  num_frags;       // the total number of fragments
    protected int  original_length; // the length of the original message
    protected int  offset;          // offset of this fragment in the original message; length is in Message.getLength()



    public Frag3Header() {
    } // used for externalization

    public Frag3Header(int id, int frag_id, int num_frags) {
        this.id=id;
        this.frag_id=frag_id;
        this.num_frags=num_frags;
    }

    public Frag3Header(int id, int frag_id, int num_frags, int original_length, int offset) {
        this.id=id;
        this.frag_id=frag_id;
        this.num_frags=num_frags;
        this.original_length=original_length;
        this.offset=offset;
    }

    public short getMagicId() {return 91;}

    public Supplier<? extends Header> create() {
        return Frag3Header::new;
    }

    public String toString() {
        return String.format("[id=%d, frag-id=%d, num_frags=%d orig-length=%d, offset=%d]",
                             id, frag_id, num_frags, original_length, offset);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeInt(id,out);
        Bits.writeInt(frag_id, out);
        Bits.writeInt(num_frags, out);
        Bits.writeInt(original_length, out);
        Bits.writeInt(offset, out);
    }

    @Override
    public int serializedSize() {
        return Bits.size(id) + Bits.size(frag_id) + Bits.size(num_frags) + Bits.size(original_length) + Bits.size(offset);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        id=Bits.readInt(in);
        frag_id=Bits.readInt(in);
        num_frags=Bits.readInt(in);
        original_length=Bits.readInt(in);
        offset=Bits.readInt(in);
    }

}
