
package org.jgroups.demos.wb;
import org.jgroups.Address;



public class Node implements java.io.Serializable {
    public double    x, y, dx, dy;
    public boolean   fixed;
    public String    lbl=null;
    public Address   addr=null;
    public int       xloc=0, yloc=0;
    public int       width=0;
    public int       height=0;

    
    public String toString() {
	StringBuilder ret=new StringBuilder();
	ret.append("name=" + lbl + ", addr=" + addr + " at " + x + ',' + y);
	return ret.toString();
    }
}

