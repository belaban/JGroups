package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.Event;

/** Duplicates outgoing or incoming messages by copying them
 * @author Bela Ban
 * @version $Id: DUPL.java,v 1.1 2008/06/05 18:49:34 belaban Exp $
 */
public class DUPL extends Protocol {

    @Property @ManagedAttribute(description="Number of copies of each incoming message (0=no copies)",writable=true)
    protected short incoming_copies=1;

    @Property @ManagedAttribute(description="Number of copies of each outgoing message (0=no copies)",writable=true)
    protected short outgoing_copies=1;

    @Property @ManagedAttribute(description="Whether or not to copy unicast messages",writable=true)
    protected boolean copy_unicast_msgs=true;

    @Property @ManagedAttribute(description="Whether or not to copy multicast messages",writable=true)
    protected boolean copy_multicast_msgs=true;

    public String getName() {
        return "DUPL";
    }


    public Object down(Event evt) {
        boolean copy=(copy_multicast_msgs || copy_unicast_msgs) && (incoming_copies > 0 || outgoing_copies > 0);


        return down_prot.down(evt);
    }
}
