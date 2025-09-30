
package org.jgroups;

/**
 * Thrown if a message is sent to a suspected member.
 * 
 * @since 2.0
 * @author Bela Ban
 */
public class SuspectedException extends RuntimeException {
    private static final long serialVersionUID = -6663279911010545655L;
    protected final Address member;

    public SuspectedException(Address member) {
        super("SuspectedException");
        this.member=member;
    }

    public String toString() {
        return getMessage() + ": member=" + member;
    }
}
