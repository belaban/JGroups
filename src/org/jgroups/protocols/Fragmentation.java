package org.jgroups.protocols;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

/**
 * Base class for all fragmentation protocols.
 * @author Bela Ban
 * @since  5.0.0
 */
@MBean(description="Fragments messages larger than fragmentation size into smaller messages")
public class Fragmentation extends Protocol {

    @Property(description="The max number of bytes in a message. Larger messages will be fragmented")
    protected int                 frag_size=60000;


    public int           getFragSize()      {return frag_size;}
    public <T extends Fragmentation> T setFragSize(int f) {this.frag_size=f; return (T)this;}
}
