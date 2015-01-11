package org.jgroups.protocols.jzookeeper;

import java.util.HashSet;

import org.jgroups.Address;
import org.jgroups.Message;

public class Proposal {
	
	public int AckCount;

	private HashSet<Long> ackSet = new HashSet<Long>();

	private MessageInfo messageInfo=null;
    
	private Address messageSrc;

    public Proposal(){
    	
    }
	public Proposal(int count, HashSet<Long> ackSet, MessageInfo messageInfo) {

		this.AckCount = count;
		this.ackSet = ackSet;
		this.messageInfo = messageInfo;
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

	public MessageInfo getMessageInfo() {
		return messageInfo;
	}

	public void setMessageInfo(MessageInfo messageInfo) {
		this.messageInfo = messageInfo;
	}
	public Address getMessageSrc() {
		return messageSrc;
	}
	public void setMessageSrc(Address messageSrc) {
		this.messageSrc = messageSrc;
	}
    

}
