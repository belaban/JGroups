// $Id: Hot_ViewID.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

public class Hot_ViewID {
    public int ltime;
    public Hot_Endpoint coord;

    public String toString() {
	return "ltime=" + ltime + ", coord=" + coord;
    }
}
