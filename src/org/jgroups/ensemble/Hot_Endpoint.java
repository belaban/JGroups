// $Id: Hot_Endpoint.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.jgroups.Address;


public class Hot_Endpoint implements Address {
    public String name;

    public String toString() {return name;}

    public boolean isMulticastAddress() {return false;}

    public int compareTo(Object other) throws ClassCastException {
	Hot_Endpoint o=(Hot_Endpoint)other;
	if(o.name != null)
	    return name.compareTo(o.name);
	return -1;
    }

    public boolean equals(Object obj) {
	String       other_name;
	Hot_Endpoint other=(Hot_Endpoint)obj;
	if(other == null)
	    return false;
	if(other == this)
	    return true;
	other_name=other.name;
	if(name.equals(other_name))
	    return true;
	return false;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeObject(name);
    }
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	name=(String)in.readObject();
    }
    
}
