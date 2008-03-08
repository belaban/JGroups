
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;


/**
 * Protocol to simulate a partition. This can be done in 2 ways: send down a PARTITION event (with a boolean flag on or
 * off, to start or end a partition), or by grabbing a reference to the protocol via the ProtocolStack and calling the
 * methods startPartition() or stopPartition() directly. This can also be done via JMX.<p/>
 * A partition simply discards all messages, but let's other events pass.
 * @author Bela Ban
 * @version $Id: PARTITION.java,v 1.4 2008/03/08 09:46:46 vlada Exp $
 */
@MBean(description="Protocol to simulate a partition")
public class PARTITION extends Protocol {
    @ManagedAttribute
    protected boolean partition_on=false;
    protected Address local_address=null;

    public String getName() {
        return "PARTITION";
    }

    public boolean isPartitionOn() {
        return partition_on;
    }

    @ManagedOperation
    public void startPartition() {
        partition_on=true;
    }

    @ManagedOperation
    public void stopPartition() {
        partition_on=false;
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.START_PARTITION:
                startPartition();
                return null;
            case Event.STOP_PARTITION:
                stopPartition();
                return null;
            default:
                return down_prot.down(evt);
        }
    }

    public Object up(Event evt) {
        if(evt.getType() == Event.SET_LOCAL_ADDRESS)
            local_address=(Address)evt.getArg();
        if(partition_on == false)
            return up_prot.up(evt);
        if(evt.getType() != Event.MSG)
            return up_prot.up(evt);
        Message msg=(Message)evt.getArg();
        if(msg.getSrc().equals(local_address))
            return up_prot.up(evt);
        return null;
    }
}