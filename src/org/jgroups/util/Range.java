// $Id: Range.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.util;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;



public class Range implements Externalizable {
    public long low=-1;  // first msg to be retransmitted
    public long high=-1; // last msg to be retransmitted



    /** For externalization */
    public Range() {
    }

    public Range(long low, long high) {
	this.low=low; this.high=high;
    }
    


    public String toString() {
	return "[" + low + " : " + high + "]";
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeLong(low);
	out.writeLong(high);
    }
    
    
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	low=in.readLong();
	high=in.readLong();
    }


}
