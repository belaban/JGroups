
package org.jgroups.protocols;

import org.jgroups.Event;
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


    public Object up(Event evt) {
        if(print_up && evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            System.out.printf("-- [s] from %s (%d bytes): %s\n", msg.src(), msg.getLength(), msg.printHeaders());
        }
        return up_prot.up(evt); // Pass up to the layer above us
    }

    public void up(MessageBatch batch) {
        if(print_up) {
            for(Message msg : batch)
                System.out.printf("-- [b] from %s (%d bytes): %s\n", msg.src(), msg.getLength(), msg.printHeaders());
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    public Object down(Event evt) {
        if(print_down && evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            System.out.printf("-- to %s (%d bytes): %s\n", msg.dest(), msg.getLength(), msg.printHeaders());
        }

        return down_prot.down(evt);  // Pass on to the layer below us
    }


}
