// $Id: Header.java,v 1.12 2010/06/15 06:43:54 belaban Exp $

package org.jgroups;

import org.jgroups.util.Streamable;


/**
 * Abstract base class for all headers to be added to a Message.
 * @author Bela Ban
 */
public abstract class Header implements Streamable {

    public Header() {
    }


    /**
     * To be implemented by subclasses. Return the size of this object for the serialized version of it.
     * I.e. how many bytes this object takes when flattened into a buffer. This may be different for each instance,
     * or can be the same. This may also just be an estimation. E.g. FRAG uses it on Message to determine whether
     * or not to fragment the message. Fragmentation itself will be accurate, because the entire message will actually
     * be serialized into a byte buffer, so we can determine the exact size.
     */
    public abstract int size();



    public String toString() {
        return '[' + getClass().getName() + " Header]";
    }

}
