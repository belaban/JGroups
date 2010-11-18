package org.jgroups.util;

import org.jgroups.Address;

import java.io.*;

/**
 * Used by RELAY to ship original sender (proxy_addr) of a message. Behaves like an Addess, but forwards all methods
 * to addr. Both addresses have to be non-null !
 * @author Bela Ban
 */
public class ProxyAddress implements Address {
    private static final long serialVersionUID=1571765837965342563L;

    protected Address addr;
    protected Address proxy_addr=null;


    public ProxyAddress() {
    }

    public ProxyAddress(Address addr, Address proxy_addr) {
        this.addr=addr;
        this.proxy_addr=proxy_addr;
    }



    public boolean isMulticastAddress() {
        return addr.isMulticastAddress();
    }

    public int size() {
        return Util.size(addr) + Util.size(proxy_addr);
    }

    public int compareTo(Address o) {
        ProxyAddress other=(ProxyAddress)o;
        return addr.compareTo(other.addr);
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeAddress(addr, out);
        Util.writeAddress(proxy_addr, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        addr=Util.readAddress(in);
        proxy_addr=Util.readAddress(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(addr);
        out.writeObject(proxy_addr);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        addr=(Address)in.readObject();
        proxy_addr=(Address)in.readObject();
    }

    public String toString() {
        return addr + " | " + proxy_addr;
    }

}
