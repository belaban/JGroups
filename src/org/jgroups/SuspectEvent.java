// $Id: SuspectEvent.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups;

public class SuspectEvent {
    Object suspected_mbr;

    public SuspectEvent(Object suspected_mbr) {this.suspected_mbr=suspected_mbr;}

    public Object getMember() {return suspected_mbr;}
    public String toString() {return "SuspectEvent";}
}
