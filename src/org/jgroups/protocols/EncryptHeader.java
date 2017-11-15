package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class EncryptHeader extends Header {
    public static final byte INSTALL_KEYS       = 1; // body of the message contains public and/or shared keys
    public static final byte FETCH_SHARED_KEY   = 2; // the receiver fetches the shared key via an external key exchange

    protected byte    type;
    protected byte[]  version;
    protected Address server; // used with FETCH_SHARED_KEY
    protected byte[]  iv;
    protected boolean needs_deserialization;

    public EncryptHeader() {}


    public EncryptHeader(byte type, byte[] version, byte[] iv) {
        this.type=type;
        this.version=version;
        this.iv = iv;
    }

    public EncryptHeader(byte type, byte[] version) {
        this.type=type;
        this.version=version;
    }

    public EncryptHeader(byte[] version) {
        this.version=version;
    }

    public byte                       type()                             {return type;}
    public byte[]                     version()                          {return version;}
    public Address                    server()                           {return server;}
    public byte[]                     iv()                               {return iv;}
    public EncryptHeader              server(Address s)                  {this.server=s; return this;}
    public boolean                    needsDeserialization()             {return needs_deserialization;}
    public EncryptHeader              needsDeserialization(boolean flag) {needs_deserialization=flag; return this;}
    public short                      getMagicId()                       {return 88;}
    public Supplier<? extends Header> create()                           {return EncryptHeader::new;}

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
        out.writeBoolean(needs_deserialization);
        Util.writeByteBuffer(version, 0, version != null? version.length : 0, out);
        Util.writeAddress(server, out);
        Util.writeByteBuffer(iv, 0, iv != null? iv.length : 0, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
        needs_deserialization=in.readBoolean();
        version=Util.readByteBuffer(in);
        server=Util.readAddress(in);
        iv = Util.readByteBuffer(in);
    }

    @Override
    public String toString() {
        return String.format("%s [version=%s]",
                             typeToString(type), (version != null? Util.byteArrayToHexString(version) : "null"))
          + (server == null? "" : " [server=" + server + "]")
          + (iv == null? "" : " [iv=" + Util.byteArrayToHexString(iv) + "]");
    }

    public int serializedSize() {
        return Global.BYTE_SIZE + Util.size(version) + Util.size(server) + Util.size(iv) + Global.BYTE_SIZE;
    }

    protected static String typeToString(byte type) {
        switch(type) {
            case INSTALL_KEYS:       return "INSTALL_KEYS";
            case FETCH_SHARED_KEY:   return "FETCH_SHARED_KEY";
            default:                 return EncryptHeader.class.getSimpleName();
        }
    }
}
