package org.jgroups.blocks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Performs serialization and de-serialization of RPC call arguments and return values (including exceptions)
 * @author Bela Ban
 * @since  2.x, 4.0
 */
public interface Marshaller {

    /**
     * Estimates the number of bytes needed to serialize an object to an output stream. This is used to create an output
     * stream with an initial capacity, so it does not need to be exact. However, if the estimated size is much smaller than
     * the actual size needed by the arguments, the output stream's buffer will have to be copied, possibly multiple times.
     * @param arg the object; argument to an RPC, or return value (could also be an exception). May be null (e.g. an
     *           RPC returning void)
     * @return the estimated size
     */
    default int estimatedSize(Object arg) {
        return arg == null? 2: 50;
    }

    /**
     * Serializes an object to an output stream
     * @param obj the object to be serialized
     * @param out the output stream, created taking {@link #estimatedSize(Object)} into account
     * @throws IOException thrown if serialization failed
     */
    void objectToStream(Object obj, DataOutput out) throws IOException;

    /**
     * Creates an object from a stream
     * @param in the input stream
     * @return an object read from the input stream
     * @throws IOException thrown if deserialization failed
     * @throws ClassNotFoundException if a requisite class was not found
     */
    Object objectFromStream(DataInput in) throws IOException, ClassNotFoundException;
}
