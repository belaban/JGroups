// $Id: SuspectEvent.java,v 1.3 2005/07/17 11:38:05 chrislott Exp $

package org.jgroups;

/**
 * Represents a suspect event.
 * Gives access to the suspected member.
 */
public class SuspectEvent {
    final Object suspected_mbr;

    public SuspectEvent(Object suspected_mbr) {this.suspected_mbr=suspected_mbr;}

    public Object getMember() {return suspected_mbr;}
    public String toString() {return "SuspectEvent";}
}
