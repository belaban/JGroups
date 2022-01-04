package org.jgroups.protocols;

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

    protected LongAdder num_frags_sent=new LongAdder();
    protected LongAdder num_frags_received=new LongAdder();


    public int                         getFragSize()      {return frag_size;}
    public <T extends Fragmentation> T setFragSize(int f) {this.frag_size=f; return (T)this;}

    @ManagedAttribute(description="Number of sent fragments",type=AttributeType.SCALAR)
    public long                        getNumberOfSentFragments()     {return num_frags_sent.sum();}

    @ManagedAttribute(description="Number of received fragments",type=AttributeType.SCALAR)
    public long                        getNumberOfReceivedFragments() {return num_frags_received.sum();}

    public void resetStats() {
        super.resetStats();
        num_frags_sent.reset();
        num_frags_received.reset();
    }
}
