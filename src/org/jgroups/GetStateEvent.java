// $Id: GetStateEvent.java,v 1.5 2006/03/16 15:55:06 belaban Exp $

package org.jgroups;

/**
 * Represents a GetState event.
 * Gives access to the requestor.
 */
public class GetStateEvent {
    Object requestor=null;
    String state_id=null;

    public GetStateEvent(Object requestor) {this.requestor=requestor;}

    public Object getRequestor() {return requestor;}

    public String getStateId() {return state_id;}

    public String toString() {return "GetStateEvent[requestor=" + requestor + ", state_id=" + state_id + ']';}
}
