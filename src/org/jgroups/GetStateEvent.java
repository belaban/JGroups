// $Id: GetStateEvent.java,v 1.3 2004/07/05 14:17:36 belaban Exp $

package org.jgroups;

public class GetStateEvent {
    Object requestor=null;

    public GetStateEvent(Object requestor) {this.requestor=requestor;}

    public Object getRequestor() {return requestor;}

    public String toString() {return "GetStateEvent[requestor=" + requestor + ']';}
}
