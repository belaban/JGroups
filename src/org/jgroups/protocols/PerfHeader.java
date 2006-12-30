// $Id: PerfHeader.java,v 1.11 2006/12/30 10:32:49 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;


/**
 * Entry specific for 1 protocol layer. Records time message was received by that layer and when message was passed on
 */
class PerfEntry implements Externalizable {
    long received=0;
    long done=0;
    long total=-1;


    // Needed for externalization
    public PerfEntry() {

    }


    public long getReceived() {
        return received;
    }

    public long getDone() {
        return done;
    }

    public long getTotal() {
        return total;
    }

    public void setReceived(long r) {
        received=r;
    }

    public void setDone(long d) {
        done=d;
        if(received > 0 && done > 0 && done >= received)
            total=done - received;
    }

    public String toString() {
        if(total >= 0)
            return "time: " + total;
        else
            return "time: n/a";
    }


    public String printContents(boolean detailed) {
        StringBuffer sb=new StringBuffer();
        if(detailed) {
            if(received > 0) sb.append("received=").append(received);
            if(done > 0) {
                if(received > 0) sb.append(", ");
                sb.append("done=").append(done);
            }
        }
        if(detailed && (received > 0 || done > 0)) sb.append(", ");
        sb.append(toString());
        return sb.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(received);
        out.writeLong(done);
        out.writeLong(total);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        received=in.readLong();
        done=in.readLong();
        total=in.readLong();
    }


}
