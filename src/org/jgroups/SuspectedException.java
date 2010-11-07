// $Id: SuspectedException.java,v 1.4 2006/04/29 03:25:32 belaban Exp $

package org.jgroups;

/**
 * Thrown if a message is sent to a suspected member.
 */
public class SuspectedException extends Exception {
    final Object suspect;

    private static final long serialVersionUID=-6663279911010545655L;

    public SuspectedException()                {this.suspect=null;}
    public SuspectedException(Object suspect)  {this.suspect=suspect;}

    public String toString() {return "SuspectedException";}
}
