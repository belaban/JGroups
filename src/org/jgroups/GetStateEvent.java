// $Id: GetStateEvent.java,v 1.2 2004/07/05 06:00:40 belaban Exp $

package org.jgroups;

public class GetStateEvent {
    Object requestor=null;

    public GetStateEvent(Object requestor) {this.requestor=requestor;}

    public Object getRequestor() {return requestor;}

    public String toString() {return "GetStateEvent[requestor=" + requestor + ']';}
}
