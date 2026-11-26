
package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.function.Predicate;


/**
 * Prints the headers of all sent or received messages
 */
@MBean(description="Prints the headers of all sent and/or received messages")
public class HDRS extends Protocol {
    @Property(description="Enables printing of down messages")
    protected volatile boolean print_down=true;

    @Property(description="Enables printing of up (received) messages")
    protected volatile boolean print_up=true;

    // Filter to print messages on given conditions. If null, the filter is not active
    protected Predicate<Message> filter;

    public boolean printUp()                    {return print_up;}
    public HDRS    printUp(boolean b)           {print_up=b; return this;}
    public boolean printDown()                  {return print_down;}
    public HDRS    printDown(boolean b)         {print_down=b; return this;}
    public HDRS    filter(Predicate<Message> f) {filter=f; return this;}


    public Object down(Message msg) {
        if(print_down) {
            if(filter != null && filter.test(msg) == false)
                return down_prot.down(msg);
            System.out.printf("-- to %s (%d bytes): %s\n", msg.getDest(), msg.getLength(), msg.printHeaders());
        }
        return down_prot.down(msg);  // Pass on to the layer below us
    }

    public Object up(Message msg) {
        if(print_up) {
            if(filter != null && !filter.test(msg))
                return up_prot.up(msg);
            System.out.printf("-- from %s (%d bytes): %s\n", msg.getSrc(), msg.getLength(), msg.printHeaders());
        }
        return up_prot.up(msg); // Pass up to the layer above us
    }

    public void up(MessageBatch batch) {
        if(print_up) {
            if(filter != null && batch.anyMatch(filter) == false) {
                up_prot.up(batch);
                return;
            }
            for(Message msg: batch)
                System.out.printf("-- from %s (%d bytes): %s\n", msg.getSrc(), msg.getLength(), msg.printHeaders());
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }




}
