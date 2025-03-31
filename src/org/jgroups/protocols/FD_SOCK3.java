package org.jgroups.protocols;

import java.util.Collection;
import java.util.List;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Membership;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Util;

/**
 * Failure detection protocol based on disconnection events from TCP based transports.
 * @author Christian Fredriksson
 * @since 5.4.6
 */
@MBean(description="Failure detection protocol based on sockets connecting members")
public class FD_SOCK3 extends Protocol {

    @ManagedAttribute(description="Number of suspect events emitted",type=AttributeType.SCALAR)
    protected int                            num_suspect_events;

    @ManagedAttribute(description="True when this member is leaving the cluster, set to false when joining")
    protected volatile boolean               shutting_down;

    @ManagedAttribute(description="List of the current cluster members")
    protected final Membership               members=new Membership();

    @ManagedAttribute(description="List of currently suspected members")
    protected final Membership               suspected_mbrs=new Membership();


    protected final BoundedList<String>      suspect_history=new BoundedList<>(20);


    public FD_SOCK3() {
    }


    @ManagedAttribute(description="The number of currently suspected members")
    public int         getNumSuspectedMembers()          {return suspected_mbrs.size();}

    @ManagedOperation(description="Print suspect history")
    public String printSuspectHistory() {return String.join("\n", suspect_history);}


    @Override
    public void init() throws Exception {
        TP tp=getTransport();
        if(!(tp instanceof BasicTCP))
            throw new IllegalStateException("FD_SOCK3 can only be used with TCP or TCP_NIO2");
        if(((BasicTCP)tp).getConnExpireTime() > 0)
            throw new IllegalStateException("FD_SOCK3 cannot be used with connection reaping");
    }

    @Override
    public void stop() {
        suspected_mbrs.clear();
    }

    public void resetStats() {
        super.resetStats();
        num_suspect_events=0;
        suspect_history.clear();
    }

    @Override
    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.UNSUSPECT:
                unsuspect(evt.getArg());
                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
                shutting_down=false;
                break;

            case Event.DISCONNECT:
                shutting_down=true;
                break;

            case Event.VIEW_CHANGE:
                Object ret=down_prot.down(evt);
                handleView(evt.arg());
                return ret;
        }
        return down_prot.down(evt);
    }

    @Override
    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MBR_DISCONNECTED:
                handleMbrDisconnected(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    protected void handleView(View v) {
        final List<Address> new_mbrs=v.getMembers();
        members.set(new_mbrs);
        suspected_mbrs.retainAll(new_mbrs);
    }

    protected void handleMbrDisconnected(Address member) {
        if (!shutting_down)
            suspect(member);
    }

    protected void suspect(Address suspect) {

        suspect_history.add(String.format("%s: %s", Util.utcNow(), suspect));
        suspected_mbrs.add(suspect);
        Collection<Address> suspects_copy=suspected_mbrs.getMembers(); // returns a copy
        if(suspects_copy.isEmpty())
            return;

        // Check if we're coord, then send up the stack, make a copy (https://issues.redhat.com/browse/JGRP-2552)
        Membership eligible_mbrs=this.members.copy().remove(suspected_mbrs.getMembers());
        if(eligible_mbrs.isCoord(local_addr)) {
            log.debug("%s: suspecting %s", local_addr, suspects_copy);
            up_prot.up(new Event(Event.SUSPECT, suspects_copy));
            down_prot.down(new Event(Event.SUSPECT, suspects_copy));
            if (stats)
                num_suspect_events++;
        }
    }

    protected void unsuspect(Address mbr) {
        if(mbr == null)
            return;
        suspected_mbrs.remove(mbr);
    }
}
