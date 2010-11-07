package org.jgroups.blocks;

/**
 * This exception indicated that lock manager refused to release a lock on 
 * some resource.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LockNotReleasedException extends Exception {

    private static final long serialVersionUID = -350403929687059570L;

	public LockNotReleasedException() {
        super();
    }

    public LockNotReleasedException(String s) {
        super(s);
    }
    
}
