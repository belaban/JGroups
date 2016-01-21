package org.jgroups.protocols.zWithInfinspan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Message;

public class Proposal implements Comparable<Proposal>{
	
	public int AckCount;

	private HashSet<Long> ackSet = new HashSet<Long>();

	private long zxid = -1;
	
	private MessageId messageId=null;
	
	private MessageOrderInfo messageOrderInfo=null;

    
	private Address messageSrc;
	
	private long requestCreated ;

    public Proposal(){
    	requestCreated = System.currentTimeMillis();
    }
	public Proposal(int count, HashSet<Long> ackSet) {

		this.AckCount = count;
		this.ackSet = ackSet;
		//.message = message;
	}

	public int getAckCount() {
		return AckCount;
	}

	public void setAckCount(int count) {
		this.AckCount = count;
	}

	public HashSet<Long> getAckSet() {
		return ackSet;
	}

	public void setAckSet(HashSet<Long> ackSet) {
		this.ackSet = ackSet;
	}

	public MessageId getMessageId() {
		return messageId;
	}

	public void setMessageId(MessageId messageId) {
		this.messageId = messageId;
	}
	
	public MessageOrderInfo getMessageOrderInfo() {
		return messageOrderInfo;
	}
	public void setMessageOrderInfo(MessageOrderInfo messageOrderInfo) {
		this.messageOrderInfo = messageOrderInfo;
	}
	public Address getMessageSrc() {
		return messageSrc;
	}
	public void setMessageSrc(Address messageSrc) {
		this.messageSrc = messageSrc;
	}
	public long getRequestCreated() {
		return requestCreated;
	}
	public void setRequestCreated(long requestCreated) {
		this.requestCreated = requestCreated;
	}
	public long getZxid() {
		return zxid;
	}
	public void setZxid(long zxid) {
		this.zxid = zxid;
	}
	@Override
	public int compareTo(Proposal o) {
		if (this.zxid > o.zxid)
			return 1;
		else if (this.zxid < o.zxid)
			return -1;
		else
			return 0;
	}
	@Override
	public String toString() {
		return "Proposal [zxid=" + zxid + "]";
	}
 

}
