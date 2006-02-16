// $Id: SuspectedException.java,v 1.3 2006/02/16 08:43:03 belaban Exp $

package org.jgroups;

/**
 * Thrown if a message is sent to a suspected member.
 */
public class SuspectedException extends Exception {
    Object suspect=null;

    private static final long serialVersionUID=-6663279911010545655L;

    public SuspectedException()                {}
    public SuspectedException(Object suspect)  {this.suspect=suspect;}

    public String toString() {return "SuspectedException";}
}
