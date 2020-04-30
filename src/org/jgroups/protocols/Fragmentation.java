package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;

import java.util.concurrent.atomic.LongAdder;

/**
 * Base class for all fragmentation protocols.
 * @author Bela Ban
 * @since  5.0.0
 */
@MBean(description="Fragments messages larger than fragmentation size into smaller messages")
public class Fragmentation extends Protocol {

    @Property(description="The max number of bytes in a message. Larger messages will be fragmented",
      type=AttributeType.BYTES)
    protected int                 frag_size=60000;

    @ManagedAttribute(description="Number of sent fragments",type=AttributeType.SCALAR)
    protected LongAdder num_frags_sent=new LongAdder();
    @ManagedAttribute(description="Number of received fragments",type=AttributeType.SCALAR)
    protected LongAdder num_frags_received=new LongAdder();

    protected Address   local_addr;


    public int                         getFragSize()      {return frag_size;}
    public <T extends Fragmentation> T setFragSize(int f) {this.frag_size=f; return (T)this;}
    public long                        getNumberOfSentFragments()     {return num_frags_sent.sum();}
    public long                        getNumberOfReceivedFragments() {return num_frags_received.sum();}

    public void resetStats() {
        super.resetStats();
        num_frags_sent.reset();
        num_frags_received.reset();
    }


    @Override public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);  // Pass on to the layer below us
    }
}
