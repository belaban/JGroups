// $Id: UdpHeader.java,v 1.4 2004/07/05 14:17:16 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Header;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;




public class UdpHeader extends Header {
    public String group_addr=null;

    public UdpHeader() {
    }  // used for externalization

    public UdpHeader(String n) {
        group_addr=n;
    }

    public String toString() {
        return "[UDP:group_addr=" + group_addr + ']';
    }


    public long size() {
        return 100;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(group_addr);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        group_addr=in.readUTF();
    }


}
