package org.jgroups.protocols.jzookeeper;

import org.jgroups.Address;
import org.jgroups.util.Bits;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;


/**
 * The represents an unique identifier for the messages processed by the Total Order Anycast protocol
 * <p/>
 * Note: it is similar to the ViewId (address + counter)
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class MessageId implements Externalizable, Comparable<MessageId>, Cloneable, Streamable {
    private static final long serialVersionUID = 878801547232534461L;
    private Address address = null;
    private long id = -1;
    private long startSend = 0;
    private long startFToLF = 0;
    private long startLToFP = 0;
    private long startFToLA = 0;
    private long zxid = -1;

    public MessageId() {}

    public MessageId(Address address, long id) {
        this.address = address;
        this.id = id;
    }
    
    public MessageId(Address address, long id, long startSend) {
        this.address = address;
        this.id = id;
        this.startSend = startSend;
    }
    
    public MessageId(long zxid) {
        this.zxid = zxid;
    }

    @Override
    public int compareTo(MessageId other) {
        if (other == null) {
        	System.out.println("Before NullPointerException MessageId");
            throw new NullPointerException();
        }

        return id == other.id ? this.address.compareTo(other.address) :
                id < other.id ? -1 : 1;
    }


    public Address getAddress() {
        return address;
    }
    
    public long getStartTime(){
    	return startSend;
    }

    public void setStartTime(long startSend){
    	this.startSend = startSend;
    }
    

	public long getStartFToLF() {
		return startFToLF;
	}

	public void setStartFToLF(long startFToLF) {
		this.startFToLF = startFToLF;
	}

	public long getStartLToFP() {
		return startLToFP;
	}

	public void setStartLToFP(long startLToFP) {
		this.startLToFP = startLToFP;
	}

	public long getStartFToLA() {
		return startFToLA;
	}

	public void setStartFToLA(long startFToLA) {
		this.startFToLA = startFToLA;
	}

	public long getZxid(){
    	return zxid;
    }
    
    public long getId(){
    	return id;
    }

    public void setZxid(long zxid){
    	this.zxid = zxid;
    }
    
    @Override
    public String toString() {
        return "MessageId{" + address + ":" + id + ":" + zxid +"}";
    }

    public Object clone() {
        try {
            MessageId dolly = (MessageId) super.clone();
            dolly.address = address;
            return dolly;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageId messageId = (MessageId) o;

        return  id == messageId.id &&
                !(address != null ? !address.equals(messageId.address) : messageId.address != null);

    }

    @Override
    public int hashCode() {
        int result = address != null ? address.hashCode() : 0;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    public int serializedSize() {
        return Bits.size(id) + Util.size(address) + Bits.size(startSend) + Bits.size(zxid);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(address, out);
        Bits.writeLong(id, out);
        Bits.writeLong(startSend, out);
        Bits.writeLong(startFToLF, out);
        Bits.writeLong(startLToFP, out);
        Bits.writeLong(startFToLA, out);
        Bits.writeLong(zxid, out);

    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        address = Util.readAddress(in);
        id = Bits.readLong(in);
        startSend = Bits.readLong(in);
        startFToLF = Bits.readLong(in);
        startLToFP = Bits.readLong(in);
        startFToLA = Bits.readLong(in);
        zxid = Bits.readLong(in);
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        try {
            writeTo(objectOutput);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        try {
            readFrom(objectInput);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}