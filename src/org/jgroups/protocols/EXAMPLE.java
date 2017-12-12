
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;


/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */
@Unsupported
@MBean(description="Sample protocol")
public class EXAMPLE extends Protocol {
    final List<Address> members=new ArrayList<>();


    /**
     * Just remove if you don't need to reset any state
     */
    public static void reset() {
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                List<Address> new_members=((View)evt.getArg()).getMembers();
                synchronized(members) {
                    members.clear();
                    if(new_members != null && !new_members.isEmpty())
                        members.addAll(new_members);
                }
                return down_prot.down(evt);
        }
        return down_prot.down(evt);          // Pass on to the layer below us
    }

    public Object down(Message msg) {
        // Do something with the event, e.g. add a header to the message
        // Optionally pass down
        return down_prot.down(msg);
    }

    public Object up(Message msg) {
        // Do something with the event, e.g. extract the message and remove a header.
        // Optionally pass up
        return up_prot.up(msg);            // Pass up to the layer above us
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            // do something; perhaps check for the presence of a header
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }



    public static class ExampleHeader extends Header {
        // your variables


        public Supplier<? extends Header> create() {return ExampleHeader::new;}
        public short getMagicId() {return 21000;}
        public int serializedSize() {
            return 0; // return serialized size of all variables sent across the wire
        }

        public String toString() {
            return "[EXAMPLE: <variables> ]";
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            // write variables to stream
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            // initialize variables from stream
        }
    }


}
