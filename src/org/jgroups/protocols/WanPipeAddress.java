// $Id: WanPipeAddress.java,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.jgroups.*;


/**
   Logical address for a WAN pipe (logical link)
 */
public class WanPipeAddress implements Address {
    String logical_name=null;


    // Used only by Externalization
    public WanPipeAddress() {}



    public WanPipeAddress(String logical_name) {
	this.logical_name=logical_name;
    }


    public boolean isMulticastAddress() {
	return true;
    }


    /**
       Establishes an order between 2 addresses. Assumes other contains non-null WanPipeAddress.
       @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
    */
    public int compareTo(Object other) throws ClassCastException {
	if(other == null) {
	    System.err.println("WanPipeAddress.compareTo(): other address is null !");
	    return -1;
	}
	
	if(!(other instanceof WanPipeAddress)) {
	    System.err.println("WanPipeAddress.compareTo(): other address is not of type WanPipeAddress !");
	    return -1;
	}
	   
	if(((WanPipeAddress)other).logical_name == null) {
	    System.err.println("WanPipeAddress.compareTo(): other address is null !");
	    return -1;
	}
	
	return logical_name.compareTo(((WanPipeAddress)other).logical_name);
    }
    
    

    public boolean equals(Object obj) {
	return compareTo(obj) == 0 ? true : false;
    }




    public int hashCode() {
	return logical_name.hashCode();
    }



    public String toString() {
	return logical_name;
    }




   public void writeExternal(ObjectOutput out) throws IOException {
       out.writeObject(logical_name);
   }



   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
       logical_name=(String)in.readObject();
   }




    public static void main(String args[]) {

	WanPipeAddress a=new WanPipeAddress("daddy");
	System.out.println(a);

	WanPipeAddress b=new WanPipeAddress("daddy.nms.fnc.fujitsu.com");
	System.out.println(b);


	if(a.equals(b))
	    System.out.println("equals");
	else
	    System.out.println("does not equal");
    }


}
