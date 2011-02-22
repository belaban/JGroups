package org.jgroups.util;

import org.jgroups.Address;

import java.io.*;

/**
 * Used by RELAY to ship original sender (original_addr) of a message. Behaves like an Addess, but forwards all methods
 * to addr. Both addresses have to be non-null !
 * @author Bela Ban
 */
public class ProxyAddress implements Address {
    private static final long serialVersionUID=1571765837965342563L;

    protected Address proxy_addr;
    protected Address original_addr=null;


    public ProxyAddress() {
    }

    public ProxyAddress(Address proxy_addr, Address original_addr) {
        this.proxy_addr=proxy_addr;
        this.original_addr=original_addr;
    }


    public Address getProxyAddress() {
        return proxy_addr;
    }

    public Address getOriginalAddress() {
        return original_addr;
    }

    public boolean isMulticastAddress() {
        return false; // ProxyAddresses always only proxy unicast destinations
    }

    public int size() {
        return Util.size(proxy_addr) + Util.size(original_addr);
    }

    public int compareTo(Address o) {
        if(o instanceof ProxyAddress)
            return original_addr.compareTo(((ProxyAddress)o).original_addr);
        return original_addr.compareTo(o);
    }


    protected Object clone() throws CloneNotSupportedException {
        return new ProxyAddress(proxy_addr, original_addr);
    }

    public boolean equals(Object obj) {
        return compareTo((Address)obj) == 0;
    }

    public int hashCode() {
        return original_addr.hashCode();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeAddress(proxy_addr, out);
        Util.writeAddress(original_addr, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        proxy_addr=Util.readAddress(in);
        original_addr=Util.readAddress(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(proxy_addr);
        out.writeObject(original_addr);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        proxy_addr=(Address)in.readObject();
        original_addr=(Address)in.readObject();
    }

    public String toString() {
        return original_addr.toString() + "-r";
    }

    public String toStringDetailed() {
        return original_addr + " (proxy=" + proxy_addr + ")";
    }

}
