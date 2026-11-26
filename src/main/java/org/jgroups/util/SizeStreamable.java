package org.jgroups.util;

/**
 * Interface implementing Streamable and returning the size of the marshalled object.
 * @author Bela Ban
 * @since  3.3
 */
public interface SizeStreamable extends Streamable {
    /** Returns the size (in bytes) of the marshalled object */
    int serializedSize();
}
