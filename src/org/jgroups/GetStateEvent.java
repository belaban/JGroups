// $Id: GetStateEvent.java,v 1.1 2003/09/09 01:24:07 belaban Exp $

package org.jgroups;

public class GetStateEvent {
    Object requestor=null;

    public GetStateEvent(Object requestor) {this.requestor=requestor;}

    public Object getRequestor() {return requestor;}

    public String toString() {return "GetStateEvent[requestor=" + requestor + "]";}
}
