package org.jgroups.protocols.zWithInfinspan;

import org.jgroups.Address;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
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
public class MessageId implements Comparable<MessageId>, SizeStreamable {
    private static final long serialVersionUID = 878801547232534461L;
    private Address originator = null;
    private long id = -1;
    private long startSend = 0;
    private long startFToLF = 0;
    private long startLToFP = 0;
    private long startFToLA = 0;
    private long zxid = -1;

    public MessageId() {}

    public MessageId(Address originator, long id) {
        this.originator = originator;
        this.id = id;
    }
    
    public MessageId(Address address, long id, long startSend) {
        this.originator = address;
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

        return id == other.id ? this.originator.compareTo(other.originator) :
                id < other.id ? -1 : 1;
    }


    public Address getOriginator() {
        return originator;
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
        return "MessageId{" + originator + ":" + id + ":" + zxid +"}";
    }

    public Object clone() {
        try {
            MessageId dolly = (MessageId) super.clone();
            dolly.originator = originator;
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
                !(originator != null ? !originator.equals(messageId.originator) : messageId.originator != null);

    }

    @Override
    public int hashCode() {
        int result = originator != null ? originator.hashCode() : 0;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(originator, out);
        Bits.writeLong(id, out);
        Bits.writeLong(startSend, out);
        Bits.writeLong(startFToLF, out);
        Bits.writeLong(startLToFP, out);
        Bits.writeLong(startFToLA, out);
        Bits.writeLong(zxid, out);

    }

    @Override
    public void readFrom(DataInput in) throws Exception {
    	originator = Util.readAddress(in);
        id = Bits.readLong(in);
        startSend = Bits.readLong(in);
        startFToLF = Bits.readLong(in);
        startLToFP = Bits.readLong(in);
        startFToLA = Bits.readLong(in);
        zxid = Bits.readLong(in);
    }

    
	@Override
	public int size() {
        return Bits.size(id) + Util.size(originator) + Bits.size(startSend) + Bits.size(zxid);

	}
}
