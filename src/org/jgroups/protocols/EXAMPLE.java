// $Id: EXAMPLE.java,v 1.8 2008/10/21 12:10:30 vlada Exp $

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;

import java.io.Serializable;
import java.util.Vector;


class ExampleHeader implements Serializable {
    private static final long serialVersionUID=-8802317525466899597L;
    // your variables

    ExampleHeader() {
    }

    public String toString() {
        return "[EXAMPLE: <variables> ]";
    }
}


/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */
@Unsupported
public class EXAMPLE extends Protocol {
    final Vector members=new Vector();

    /**
     * All protocol names have to be unique !
     */
    public String getName() {
        return "EXAMPLE";
    }


    /**
     * Just remove if you don't need to reset any state
     */
    public static void reset() {
    }


    public Object up(Event evt) {
        Message msg;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                // Do something with the event, e.g. extract the message and remove a header.
                // Optionally pass up
                break;
        }

        return up_prot.up(evt);            // Pass up to the layer above us
    }


    public Object down(Event evt) {

        switch(evt.getType()) {
            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                Vector new_members=((View)evt.getArg()).getMembers();
                synchronized(members) {
                    members.removeAllElements();
                    if(new_members != null && !new_members.isEmpty())
                        for(int i=0; i < new_members.size(); i++)
                            members.addElement(new_members.elementAt(i));
                }
                return down_prot.down(evt);

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                // Do something with the event, e.g. add a header to the message
                // Optionally pass down
                break;
        }

        return down_prot.down(evt);          // Pass on to the layer below us
    }


}
