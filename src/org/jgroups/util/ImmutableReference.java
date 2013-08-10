
package org.jgroups.util;

/**
 * Simple class that holds an immutable reference to another object (or to
 * <code>null</code>).
 *
 * @author Brian Stansberry
 * 
 */
public class ImmutableReference<T> {

    private final T referent;
    
    /** 
     * Create a new ImmutableReference.
     * 
     * @param referent the object to refer to, or <code>null</code>
     */
    public ImmutableReference(T referent) {
        this.referent = referent;
    }
    
    /**
     * Gets the wrapped object, if there is one.
     * 
     * @return the object passed to the constructor, or <code>null</code> if
     *         <code>null</code> was passed to the constructor
     */
    public T get() {
        return referent;
    }
}
