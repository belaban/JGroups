package org.jgroups.protocols.jzookeeper;

import java.util.HashSet;

import org.jgroups.Message;

public class Proposal {
	
	int AckCount;

    public HashSet<Long> ackSet = new HashSet<Long>();

    public Message message;

    public Proposal(){
    	
    }
	public Proposal(int count, HashSet<Long> ackSet, Message message) {

		this.AckCount = count;
		this.ackSet = ackSet;
		this.message = message;
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

	public Message getMessage() {
		return message;
	}

	public void setMessage(Message message) {
		this.message = message;
	}
    
    

}
