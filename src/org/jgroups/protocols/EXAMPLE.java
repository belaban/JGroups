// $Id: EXAMPLE.java,v 1.3 2004/09/23 16:29:41 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.stack.Protocol;

import java.io.Serializable;
import java.util.Vector;



class ExampleHeader implements Serializable {
    // your variables

    public ExampleHeader() {
    }

    public String toString() {
	return "[EXAMPLE: <variables> ]";
    }
}


/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */

public class EXAMPLE extends Protocol {
    final Vector   members=new Vector();

    /** All protocol names have to be unique ! */
    public String  getName() {return "EXAMPLE";}





    /** Just remove if you don't need to reset any state */
    public void reset() {}




    public void up(Event evt) {
	Message msg;

	switch(evt.getType()) {

	case Event.MSG:
	    msg=(Message)evt.getArg();
	    // Do something with the event, e.g. extract the message and remove a header.
	    // Optionally pass up
	    break;
	}

	passUp(evt);            // Pass up to the layer above us
    }





    public void down(Event evt) {
	Message msg;

	switch(evt.getType()) {
	case Event.TMP_VIEW:
	case Event.VIEW_CHANGE:
	    Vector new_members=((View)evt.getArg()).getMembers();
	    synchronized(members) {
		members.removeAllElements();
		if(new_members != null && new_members.size() > 0)
		    for(int i=0; i < new_members.size(); i++)
			members.addElement(new_members.elementAt(i));
	    }
	    passDown(evt);
	    break;

	case Event.MSG:
	    msg=(Message)evt.getArg();
	    // Do something with the event, e.g. add a header to the message
	    // Optionally pass down
	    break;
	}

	passDown(evt);          // Pass on to the layer below us
    }



}
