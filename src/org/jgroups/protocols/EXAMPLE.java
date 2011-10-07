
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


class ExampleHeader implements Serializable {
    private static final long serialVersionUID=-8802317525466899597L;
    // your variables

    public String toString() {
        return "[EXAMPLE: <variables> ]";
    }
}


/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */
@Unsupported
@MBean(description="Sample protocol")
public class EXAMPLE extends Protocol {
    final List<Address> members=new ArrayList<Address>();


    /**
     * Just remove if you don't need to reset any state
     */
    public static void reset() {
    }


    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                // Do something with the event, e.g. extract the message and remove a header.
                // Optionally pass up
                break;
        }

        return up_prot.up(evt);            // Pass up to the layer above us
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

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                // Do something with the event, e.g. add a header to the message
                // Optionally pass down
                break;
        }

        return down_prot.down(evt);          // Pass on to the layer below us
    }


}
