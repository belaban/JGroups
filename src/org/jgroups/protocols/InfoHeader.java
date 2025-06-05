package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Carries random bits of information
 * @author Bela Ban
 * @since  5.4.9
 */
public class InfoHeader extends Header {
    protected Map<String,String> info=new HashMap<>();
    public static short ID=1234;

    public InfoHeader() {
    }

    @Override
    public short getMagicId() {return 101;}

    @Override
    public Supplier<? extends Header> create() {return InfoHeader::new;}

    public String     get(String key)               {return info.get(key);}
    public InfoHeader put(String key, String value) {info.put(key, value); return this;}
    public InfoHeader clear()                       {info.clear(); return this;}

    @Override
    public int serializedSize() {
        int retval=Integer.BYTES;
        if(!info.isEmpty()) {
            for(Map.Entry<String,String> e: info.entrySet())
                retval+=Util.size(e.getKey()) + Util.size(e.getValue());
        }
        return retval;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(info.size());
        for(Map.Entry<String,String> e: info.entrySet()) {
            Bits.writeString(e.getKey(), out);
            Bits.writeString(e.getValue(), out);
        }
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int size=in.readInt();
        for(int i=0; i < size; i++) {
            String key=Bits.readString(in),
              value=Bits.readString(in);
            info.put(key, value);
        }
    }

    @Override
    public String toString() {
        return info.toString();
    }
}
