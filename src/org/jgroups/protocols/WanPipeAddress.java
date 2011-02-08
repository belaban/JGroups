// $Id: WanPipeAddress.java,v 1.9 2005/08/08 12:45:46 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;


/**
 * Logical address for a WAN pipe (logical link)
 */
public class WanPipeAddress implements Address {
    String logical_name=null;
    static final Log log=LogFactory.getLog(WanPipeAddress.class);


    // Used only by Externalization
    public WanPipeAddress() {
    }


    public WanPipeAddress(String logical_name) {
        this.logical_name=logical_name;
    }


    public boolean isMulticastAddress() {
        return true;
    }

    public int size() {
        return logical_name != null? logical_name.length()+2 : 22;
    }


    /**
     * Establishes an order between 2 addresses. Assumes other contains non-null WanPipeAddress.
     *
     * @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
     */
    public int compareTo(Object other) throws ClassCastException {
        if(other == null) {
            log.error("WanPipeAddress.compareTo(): other address is null !");
            return -1;
        }

        if(!(other instanceof WanPipeAddress)) {
            log.error("WanPipeAddress.compareTo(): other address is not of type WanPipeAddress !");
            return -1;
        }

        if(((WanPipeAddress)other).logical_name == null) {
            log.error("WanPipeAddress.compareTo(): other address is null !");
            return -1;
        }

        return logical_name.compareTo(((WanPipeAddress)other).logical_name);
    }


    public boolean equals(Object obj) {
        return compareTo(obj) == 0;
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


    public void writeTo(DataOutputStream outstream) throws IOException {
        Util.writeString(logical_name, outstream);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        logical_name=Util.readString(instream);
    }
}
