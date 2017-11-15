
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;


/**
 * Protocol which prints out the real size of a message. Don't use this layer in
 * a production stack since the costs are high (just for debugging).
 * 
 * @author Bela Ban June 13 2001
 */
public class SIZE extends Protocol {
    protected final List<Address> members=new ArrayList<>();
    @Property protected boolean   print_msg=false;
    @Property protected boolean   raw_buffer=false; // just print the payload size of the message

    /** Min size in bytes above which msgs should be printed */
    protected @Property long      min_size;

    protected Address             local_addr;


    public Object up(Message msg) {
        if(log.isTraceEnabled()) {
            int size=raw_buffer? msg.getLength() : msg.size();
            if(size >= min_size) {
                StringBuilder sb=new StringBuilder(local_addr + ".up(): size of message buffer=");
                sb.append(Util.printBytes(size)).append(", " + numHeaders(msg) + " headers");
                if(print_msg)
                    sb.append(", headers=" + msg.printHeaders());
                log.trace(sb);
            }
        }
        return up_prot.up(msg);
    }


    public void up(MessageBatch batch) {
        if(log.isTraceEnabled()) {
            long size=raw_buffer? batch.length() : batch.totalSize();
            if(size >= min_size) {
                StringBuilder sb=new StringBuilder(local_addr + ".up(): size of message batch=");
                sb.append(Util.printBytes(size)).append(", " + batch.size() + " messages, " + numHeaders(batch) + " headers");
                log.trace(sb);
            }
        }
        up_prot.up(batch);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);          // Pass on to the layer below us
    }

    public Object down(Message msg) {
        if(log.isTraceEnabled()) {
            int size=raw_buffer? msg.getLength() : msg.size();
            if(size >= min_size) {
                StringBuilder sb=new StringBuilder(local_addr + ".down(): size of message buffer=");
                sb.append(Util.printBytes(size)).append(", " + numHeaders(msg) + " headers");
                if(print_msg)
                    sb.append(", headers=" + msg.printHeaders());
                log.trace(sb);
            }
        }
        return down_prot.down(msg);
    }

    protected static int numHeaders(Message msg) {
        return msg == null? 0 : msg.getNumHeaders();
    }

    protected static int numHeaders(MessageBatch batch) {
        int retval=0;
        for(Message msg: batch)
            retval+=numHeaders(msg);
        return retval;
    }

}
