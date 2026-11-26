
package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;


public class TRACE extends Protocol {

    public TRACE() {}

    public Object up(Event evt) {
        System.out.println("---------------- TRACE (received) ----------------------");
        System.out.println(evt);
        System.out.println("--------------------------------------------------------");
        return up_prot.up(evt);
    }


    public void up(MessageBatch batch) {
        System.out.println("---------------- TRACE (received) ----------------------");
        System.out.println("message batch (" + batch.size() + " messages");
        System.out.println("--------------------------------------------------------");
        up_prot.up(batch);
    }

    public Object down(Event evt) {
        System.out.println("------------------- TRACE (sent) -----------------------");
        System.out.println(evt);
        System.out.println("--------------------------------------------------------");
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
         System.out.println("------------------- TRACE (sent) -----------------------");
         System.out.printf("msg from %s to %s (%d bytes): hdrs=%s\n", msg.getSrc(), msg.getDest(), msg.getLength(), msg.printHeaders());
         System.out.println("--------------------------------------------------------");
         return down_prot.down(msg);
     }

    public String toString() {
        return "TRACE";
    }


}
