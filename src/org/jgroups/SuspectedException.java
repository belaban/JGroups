// $Id: SuspectedException.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups;


public class SuspectedException extends Exception {
    Object suspect=null;

    public SuspectedException()                {}
    public SuspectedException(Object suspect)  {this.suspect=suspect;}

    public String toString() {return "SuspectedException";}
}
