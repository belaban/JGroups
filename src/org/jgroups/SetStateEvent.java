
package org.jgroups;






/**
 * Encapsulates a state returned by <code>Channel.receive()</code>, as requested by
 * <code>Channel.getState(s)</code> previously.
 * @author Bela Ban
 */
public class SetStateEvent {
    byte[]     state=null;
    String     state_id=null;

    public SetStateEvent(byte[] state, String state_id) {
        this.state=state;
        this.state_id=state_id;
    }


    public byte[] getArg() {return state;}
    public String getStateId() {return state_id;}

    public String toString() {return "SetStateEvent[state=" + state + ", state_id=" + state_id + ']';}

}
