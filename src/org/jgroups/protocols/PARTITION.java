
package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.stack.Protocol;


/**
 * Protocol to simulate a partition. This can be done in 2 ways: send down a PARTITION event (with a boolean flag on or
 * off, to start or end a partition), or by grabbing a reference to the protocol via the ProtocolStack and calling the
 * methods startPartition() or stopPartition() directly. This can also be done via JMX.<p/>
 * A partition simply discards all messages, but let's other events pass.
 * @author Bela Ban
 * @version $Id: PARTITION.java,v 1.1 2007/08/27 10:25:34 belaban Exp $
 */
public class PARTITION extends Protocol {
    protected boolean partition_on=false;

    public String getName() {
        return "PARTITION";
    }

    public boolean isPartitionOn() {
        return partition_on;
    }

    public void startPartition() {
        partition_on=true;
    }

    public void stopPartition() {
        partition_on=false;
    }


    public Object up(Event evt) {
        if(partition_on == false)
            return up_prot.up(evt);
        else
            return null;
    }


    public Object down(Event evt) {
        if(partition_on == false)
            return down_prot.down(evt);
        else
            return null;
    }


}