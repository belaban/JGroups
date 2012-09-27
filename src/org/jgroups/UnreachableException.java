package org.jgroups;

/**
 * Thrown if member in different site cannot be contacted; used by RELAY2
 * 
 * @since 3.2
 * @author Bela Ban
 */
public class UnreachableException extends RuntimeException {
    private static final long serialVersionUID=3370509508879095097L;
    protected final Address member;

    public UnreachableException(Address member) {
        super("UnreachableException");
        this.member=member;
    }

    public UnreachableException(String msg, Address member) {
        super(msg);
        this.member=member;
    }


    public String toString() {
        return getMessage() + ": member=" + member;
    }
}
