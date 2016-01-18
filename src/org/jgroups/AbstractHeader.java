
package org.jgroups;

import org.jgroups.util.Streamable;

/**
 * Header is a JGroups internal base class for all JGroups headers. Client normally do not need to
 * interact with headers unless they are developing their custom protocols.
 * 
 * @author Bela Ban
 * @since 2.0
 */
public abstract class AbstractHeader implements Streamable {
    /** The ID of the protocol which added a header to a message. Set externally, e.g. by {@link Message#putHeader(short, AbstractHeader)} */
    protected short prot_id;

    public short  getProtId()         {return prot_id;}
    public AbstractHeader setProtId(short id) {this.prot_id=id; return this;}


    /**
     * To be implemented by subclasses. Return the size of this object for the serialized version of it.
     * I.e. how many bytes this object takes when flattened into a buffer. This may be different for each instance,
     * or can be the same. This may also just be an estimation. E.g. FRAG uses it on Message to determine whether
     * or not to fragment the message. Fragmentation itself will be accurate, because the entire message will actually
     * be serialized into a byte buffer, so we can determine the exact size.
     */
    public abstract int size();



    public String toString() {
        return '[' + getClass().getSimpleName() + "]";
    }

}
