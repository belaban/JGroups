// $Id: Header.java,v 1.8 2005/07/08 11:28:26 belaban Exp $

package org.jgroups;

import org.jgroups.util.Streamable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;


/**
 Abstract base class for all headers to be added to a Message.
 @author Bela Ban
 */
public abstract class Header implements Externalizable {
    public static final long HDR_OVERHEAD=100; // estimated size of a header (used to estimate the size of the entire msg)


    public Header() {

    }


    /**
     * To be implemented by subclasses. Return the size of this object for the serialized version of it.
     * I.e. how many bytes this object takes when flattened into a buffer. This may be different for each instance,
     * or can be the same. This may also just be an estimation. E.g. FRAG uses it on Message to determine whether
     * or not to fragment the message. Fragmentation itself will be accurate, because the entire message will actually
     * be serialized into a byte buffer, so we can determine the exact size.
     */
    public long size() {
        return HDR_OVERHEAD;
    }

//    public void writeTo(DataOutputStream out) throws IOException {
//        ;
//    }
//
//    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
//        ;
//    }


    public String toString() {
        return '[' + getClass().getName() + " Header]";
    }

}
