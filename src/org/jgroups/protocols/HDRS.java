
package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;


/**
 * Prints the headers of all sent or received messages
 */
@MBean(description="Prints the headers of all sent and/or received messages")
public class HDRS extends Protocol {
    @Property(description="Enables printing of down messages")
    protected volatile boolean print_down=true;

    @Property(description="Enables printing of up (received) messages")
    protected volatile boolean print_up=true;


    public Object down(Message msg) {
        if(print_down)
            System.out.printf("-- to %s (%d bytes): %s\n", msg.getDest(), msg.getLength(), msg.printHeaders());
        return down_prot.down(msg);  // Pass on to the layer below us
    }

    public Object up(Message msg) {
        if(print_up)
            System.out.printf("-- [s] from %s (%d bytes): %s\n", msg.getSrc(), msg.getLength(), msg.printHeaders());
        return up_prot.up(msg); // Pass up to the layer above us
    }

    public void up(MessageBatch batch) {
        if(print_up) {
            for(Message msg : batch)
                System.out.printf("-- [b] from %s (%d bytes): %s\n", msg.getSrc(), msg.getLength(), msg.printHeaders());
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }




}
