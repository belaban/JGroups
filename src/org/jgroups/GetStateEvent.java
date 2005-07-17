// $Id: GetStateEvent.java,v 1.4 2005/07/17 11:38:05 chrislott Exp $

package org.jgroups;

/**
 * Represents a GetState event.
 * Gives access to the requestor.
 */
public class GetStateEvent {
    Object requestor=null;

    public GetStateEvent(Object requestor) {this.requestor=requestor;}

    public Object getRequestor() {return requestor;}

    public String toString() {return "GetStateEvent[requestor=" + requestor + ']';}
}
