package org.jgroups.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implementations of Streamable can add their state directly to the output stream, enabling them to bypass costly
 * serialization
 * @author Bela Ban
 * @version $Id: Streamable.java,v 1.1 2004/10/04 20:40:58 belaban Exp $
 */
public interface Streamable {

    /** Write the entire state of the current object (including superclasses) to outstream */
    void writeTo(DataOutputStream out) throws IOException;

    /** Read the state of the current object (including superclasses) from instream */
    void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException;
}
