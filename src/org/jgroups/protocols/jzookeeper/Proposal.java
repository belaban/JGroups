package org.jgroups.protocols.jzookeeper;

import java.util.HashSet;

import org.jgroups.Address;
import org.jgroups.Message;

public class Proposal {
	
	public int AckCount;

	private HashSet<Long> ackSet = new HashSet<Long>();

	private MessageId messageId=null;
    
	private Address messageSrc;

    public Proposal(){
    	
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

	public MessageId getMessageInfo() {
		return messageId;
	}

	public void setMessageId(MessageId messageId) {
		this.messageId = messageId;
	}
	public Address getMessageSrc() {
		return messageSrc;
	}
	public void setMessageSrc(Address messageSrc) {
		this.messageSrc = messageSrc;
	}
    

}
