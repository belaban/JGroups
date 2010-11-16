
package org.jgroups;

/**
 * Represents a GetState event.
 * Gives access to the requestor.
 */
public class GetStateEvent {
    Object requestor=null;
    String state_id=null;

    public GetStateEvent(Object requestor, String state_id) {
        this.requestor=requestor;
        this.state_id=state_id;
    }

    public Object getRequestor() {return requestor;}

    public String getStateId() {return state_id;}

    public String toString() {return "GetStateEvent[requestor=" + requestor + ", state_id=" + state_id + ']';}
}
