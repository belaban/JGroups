package org.jgroups.protocols;

import org.jgroups.Global;
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
    protected int     id;              // unique sequence number - same for all fragments of a given original message
    protected int     frag_id;         // the ID of the frag, starting with 0. E.g. 4 fragments will have IDs from 0 to 3
    protected int     num_frags;       // the total number of fragments
    protected int     original_length; // the length of the original message
    protected int     offset;          // offset of this fragment in the original message; length is in Message.getLength()
    protected boolean needs_deserialization; // true if byte[] array of a fragment needs to be de-serialized into a payload


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

    public short                      getMagicId()                       {return 91;}
    public Supplier<? extends Header> create()                           {return Frag3Header::new;}
    public boolean                    needsDeserialization()             {return needs_deserialization;}
    public Frag3Header                needsDeserialization(boolean flag) {needs_deserialization=flag; return this;}

    public String toString() {
        return String.format("[id=%d, frag-id=%d, num_frags=%d orig-length=%d, offset=%d]",
                             id, frag_id, num_frags, original_length, offset);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeIntCompressed(id, out);
        Bits.writeIntCompressed(frag_id, out);
        Bits.writeIntCompressed(num_frags, out);
        Bits.writeIntCompressed(original_length, out);
        Bits.writeIntCompressed(offset, out);
        out.writeBoolean(needs_deserialization);
    }

    @Override
    public int serializedSize() {
        return Bits.size(id) + Bits.size(frag_id) + Bits.size(num_frags) + Bits.size(original_length)
          + Bits.size(offset) + Global.BYTE_SIZE;
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        id=Bits.readIntCompressed(in);
        frag_id=Bits.readIntCompressed(in);
        num_frags=Bits.readIntCompressed(in);
        original_length=Bits.readIntCompressed(in);
        offset=Bits.readIntCompressed(in);
        needs_deserialization=in.readBoolean();
    }

}
