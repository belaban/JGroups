// $Id: DUMMY.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.protocols;

import java.util.Vector;
import org.jgroups.*;
import org.jgroups.stack.*;


/**

 */

public class DUMMY extends Protocol {
    Vector   members=new Vector();
    String   name="DUMMY";

    /** All protocol names have to be unique ! */
    public String  getName() {return "DUMMY";}



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
	    break;
	}

	passDown(evt);          // Pass on to the layer below us
    }



}
