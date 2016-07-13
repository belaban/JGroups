package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.Supplier;

public class SaslHeader extends Header {
    public enum Type {
        CHALLENGE, RESPONSE
    };

    private Type type;
    private byte[] payload;

    public SaslHeader() {
    }

    public SaslHeader(Type type, byte[] payload) {
        this.type = type;
        this.payload = payload;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public SaslHeader payload(byte[] payload) {
        this.payload = payload;
        return this;
    }

    public byte[] token() {
        return payload;
    }

    public SaslHeader type(Type type) {
        this.type = type;
        return this;
    }

    public Supplier<? extends Header> create() {
        return SaslHeader::new;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type.ordinal());
        Util.writeByteBuffer(payload, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = Type.values()[in.readByte()];
        payload = Util.readByteBuffer(in);
    }

    @Override
    public int size() {
        return Util.size(payload);
    }

    @Override
    public String toString() {
        return "payload=" + payload;
    }
}
