// $Id: SuspectEvent.java,v 1.2 2004/09/23 16:30:00 belaban Exp $

package org.jgroups;

public class SuspectEvent {
    final Object suspected_mbr;

    public SuspectEvent(Object suspected_mbr) {this.suspected_mbr=suspected_mbr;}

    public Object getMember() {return suspected_mbr;}
    public String toString() {return "SuspectEvent";}
}
