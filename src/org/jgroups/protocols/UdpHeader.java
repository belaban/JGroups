// $Id: UdpHeader.java,v 1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Header;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;




public class UdpHeader extends Header {
    public String group_addr=null;
    transient byte[] data=null;

    public UdpHeader() {
    }  // used for externalization

    public UdpHeader(String n) {
        group_addr=n;
        data=group_addr.getBytes();
    }

    public String toString() {
        return "[UDP:group_addr=" + group_addr + "]";
    }


    public long size() {
        return 100;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if(data != null) {
            out.writeInt(data.length);
            out.write(data, 0, data.length);
        }
        else
            out.writeInt(0);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int len=in.readInt();
        if(len > 0) {
            data=new byte[len];
            in.readFully(data, 0, len);
            group_addr=new String(data);
        }
    }


}
