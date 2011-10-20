
package org.jgroups;

/**
 * Thrown if a message is sent to a suspected member.
 * 
 * @since 2.0
 * @author Bela Ban
 */
public class SuspectedException extends Exception {
    final Object suspect;

    private static final long serialVersionUID=-6663279911010545655L;

    public SuspectedException()                {this.suspect=null;}
    public SuspectedException(Object suspect)  {this.suspect=suspect;}

    public String toString() {return "SuspectedException";}
}
