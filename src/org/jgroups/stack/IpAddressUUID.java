package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.NameCache;
import org.jgroups.util.UUID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.function.Supplier;

/**
 * IpAddress with a 'semi'-UUID to prevent reincarnation when the port is fixed. The randomness is given through a
 * long and an int. On 64-bit architectures, this increases the memory size from 24 to 32 bytes per instance.<p>
 * See https://issues.jboss.org/browse/JGRP-2080 for details
 * @author Bela Ban
 * @since  4.0
 */
public class IpAddressUUID extends IpAddress {
    protected long low;
    protected int  high;

    public IpAddressUUID() {
    }

    public IpAddressUUID(String addr_port) throws Exception {
        super(addr_port);
        long[] tmp=createUUID();
        low=tmp[0]; high=(int)tmp[1];
    }

    public IpAddressUUID(InetAddress i, int p) {
        super(i, p);
        long[] tmp=createUUID();
        low=tmp[0]; high=(int)tmp[1];
    }

    public IpAddressUUID(String i, int p) throws UnknownHostException {
        super(i, p);
        long[] tmp=createUUID();
        low=tmp[0]; high=(int)tmp[1];
    }

    public IpAddressUUID(int port) {
        super(port);
        long[] tmp=createUUID();
        low=tmp[0]; high=(int)tmp[1];
    }

    public IpAddressUUID(int port, boolean set_default_host) {
        super(port, set_default_host);
        long[] tmp=createUUID();
        low=tmp[0]; high=(int)tmp[1];
    }

    public IpAddressUUID(InetSocketAddress sock_addr) {
        super(sock_addr);
        long[] tmp=createUUID();
        low=tmp[0]; high=(int)tmp[1];
    }


    protected IpAddressUUID(InetAddress i, int p, long low, int high) {
        this.ip_addr=i;
        this.port=p;
        this.low=low;
        this.high=high;
    }

    public int compareTo(Address o) {
        if(this == o) return 0;
        int retval=super.compareTo(o);
        if(retval != 0) return retval;
        // at this point o is an IpAddress but perhaps it is also an IpAddressUUID?
        if(o instanceof IpAddressUUID) {
            IpAddressUUID other=(IpAddressUUID)o;
            retval=this.low < other.low? -1 : this.low > other.low? 1 : 0;
            if(retval != 0) return retval;
            return this.high < other.high? -1 : this.high > other.high? 1 : 0;
        }
        return retval;
    }

    public IpAddress copy() {
        return new IpAddressUUID(ip_addr, port, low, high);
    }

    public Supplier<? extends IpAddress> create() {
        return IpAddressUUID::new;
    }

    public boolean equals(Object obj) {
        if(this == obj) return true;
        boolean retval=super.equals(obj);
        if(!retval) return retval;
        if(obj instanceof IpAddressUUID) {
            IpAddressUUID other=(IpAddressUUID)obj;
            return this == other || (this.low == other.low && this.high == other.high);
        }
        return retval;
    }

    // No separate hashCode() impl as IpAddressUUIDs and IpAddresses should get assigned to the *same* bucket in a hashmap!
   /* public int hashCode() {
        return super.hashCode()+
          (int)((low >> 32) ^ low ^
            (high >> 16) ^ high);
    }*/

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(low);
        out.writeInt(high);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        super.readFrom(in);
        low=in.readLong();
        high=in.readInt();
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Global.LONG_SIZE + Global.INT_SIZE;
    }

    public String toString() {
        String logical_name=NameCache.get(this);
        return logical_name != null? logical_name : super.toString();
    }

    public String toString(boolean detailed) {
        String logical_name=NameCache.get(this);
        if(logical_name != null)
            return detailed? String.format("%s (%s)", logical_name, super.toString()) : logical_name;
        return super.toString();
    }

    protected static long[] createUUID() {
        byte[] data=UUID.generateRandomBytes(12);
        long msb = 0;
        int lsb = 0;
        for (int i=0; i<8; i++)
            msb = (msb << 8) | (data[i] & 0xff);
        for (int i=8; i<12; i++)
            lsb = (lsb << 8) | (data[i] & 0xff);
        return new long[]{msb,lsb};
    }
}
