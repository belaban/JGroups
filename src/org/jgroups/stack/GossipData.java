// $Id: GossipData.java,v 1.2 2004/03/30 06:47:27 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Address;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;



/**
 * Encapsulates data sent between GossipServer and GossipClient
 * @author Bela Ban Oct 4 2001
 */
public class GossipData implements Externalizable {
    public static final int REGISTER_REQ = 1;
    public static final int GET_REQ      = 2;
    public static final int GET_RSP      = 3;

    int     type=0;
    String  group=null;  // REGISTER, GET_REQ and GET_RSP
    Address mbr=null;    // REGISTER
    Vector  mbrs=null;   // GET_RSP


    public GossipData() {
	; // used for externalization
    }

    public GossipData(int type, String group, Address mbr, Vector mbrs) {
	this.type=type;
	this.group=group;
	this.mbr=mbr;
	this.mbrs=mbrs;
    }


    public int     getType()  {return type;}
    public String  getGroup() {return group;}
    public Address getMbr()   {return mbr;}
    public Vector  getMbrs()  {return mbrs;}
    


    public String toString() {
	StringBuffer sb=new StringBuffer();
	sb.append(type2String(type));
	switch(type) {
	case REGISTER_REQ:
	    sb.append(" group=" + group + ", mbr=" + mbr);
	    break;

	case GET_REQ:
	    sb.append(" group=" + group);
	    break;

	case GET_RSP:
	    sb.append(" group=" + group + ", mbrs=" + mbrs);
	    break;
	}
	return sb.toString();
    }


    public static String type2String(int t) {
	switch(t) {
	case REGISTER_REQ: return "REGISTER_REQ";
	case GET_REQ:      return "GET_REQ";
	case GET_RSP:      return "GET_RSP";
	default:           return "<unknown>";
	}
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeInt(type);
	out.writeUTF(group);
	out.writeObject(mbr);
	out.writeObject(mbrs);
    }
    
    
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
 	type=in.readInt();
	group=in.readUTF();
	mbr=(Address)in.readObject();
	mbrs=(Vector)in.readObject();
    }

}
