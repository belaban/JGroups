package org.jgroups.admin;

import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Header for admin messages
 * @author Bela Ban
 * @since  5.6.0
 */
public class AdminHeader extends Header {
    public enum Type {request, response}

    protected String key;
    protected Type   type;
    protected String command;

    public AdminHeader() {
    }

    public AdminHeader(String k, Type type) {
        this.key=k; this.type=type;
    }

    public AdminHeader(String k, Type t, String cmd) {
        this(k, t);
        command=cmd;
    }

    public String      key()             {return key;}
    public AdminHeader key(String k)     {this.key=k; return this;}
    public Type        type()            {return type;}
    public AdminHeader type(Type t)      {this.type=t; return this;}
    public String      command()         {return command;}
    public void        command(String c) {this.command=c;}

    @Override
    public short getMagicId() {
        return 103;
    }

    @Override
    public Supplier<? extends Header> create() {
        return AdminHeader::new;
    }

    @Override
    public int serializedSize() {
        return Bits.size(key) + Integer.BYTES + Bits.size(command);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeString(key, out);
        out.writeInt(type.ordinal());
        Bits.writeString(command, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        key=Bits.readString(in);
        type=Type.values()[in.readInt()];
        command=Bits.readString(in);
    }

    @Override
    public String toString() {
        return String.format("%s key=%s%s", type, key, command != null? " cmd=" + command : "");
    }
}
