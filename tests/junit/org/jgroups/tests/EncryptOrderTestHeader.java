// $Id: EncryptOrderTestHeader.java,v 1.1 2005/04/08 08:11:51 steview Exp $

package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.ViewId;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;



public class EncryptOrderTestHeader extends Header {
   

    long    seqno=-1;          // either reg. NAK_ACK_MSG or first_seqno in retransmissions
  
  

    public EncryptOrderTestHeader() {}
    


    public EncryptOrderTestHeader(long seqno) {

	this.seqno=seqno;
    }


    public long size() {
	return 512;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	
	out.writeLong(seqno);
    }



    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

	seqno=in.readLong();

    }


    public EncryptOrderTestHeader copy() {
	EncryptOrderTestHeader ret=new EncryptOrderTestHeader(seqno);
	return ret;
    }


   

    public String toString() {
	StringBuffer ret=new StringBuffer();
	ret.append("[ENCRYPT_ORDER_TEST: seqno=" + seqno);	
	ret.append(']');

	return ret.toString();
    }

}
