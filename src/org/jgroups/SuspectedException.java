// $Id: SuspectedException.java,v 1.2 2005/07/17 11:38:05 chrislott Exp $

package org.jgroups;

/**
 * Thrown if a message is sent to a suspected member.
 */
public class SuspectedException extends Exception {
    Object suspect=null;

    public SuspectedException()                {}
    public SuspectedException(Object suspect)  {this.suspect=suspect;}

    public String toString() {return "SuspectedException";}
}
