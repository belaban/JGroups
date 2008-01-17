package org.jgroups.blocks;

/**
 * This exception indicates that a MethodCall object is not available. 
 * 
 * @author Richard Achmatowicz (rachmato@redhat.com)
 */
public class InvalidMethodCallException extends Exception {

    private static final long serialVersionUID = 3274824724210168433L;

	public InvalidMethodCallException() {
        super();
    }

    public InvalidMethodCallException(String s) {
        super(s);
    }
    
}