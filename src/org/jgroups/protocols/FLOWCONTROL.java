package org.jgroups.protocols;

import java.util.Vector;
import org.jgroups.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;




/**
 * Title: Flow control layer
 * Description: This layer limits the number of sent messages without a receive of an own message to MAXSENTMSGS,
 * just put this layer above GMS and you will get a more
 * Copyright:    Copyright (c) 2000
 * Company:      Computer Network Laboratory
 * @author Gianluca Collot
 * @version 1.0
 */
public class FLOWCONTROL extends Protocol {

  Vector queuedMsgs = new Vector();
  int sentMsgs = 0;
  static int MAXSENTMSGS = 1;
  Address myAddr;


    public FLOWCONTROL() {
    }
    public String getName() {
	return "FLOWCONTROL";
    }

  /**
   * Checs if up messages are from myaddr and in the case sends down queued messages or
   * decremnts sentMsgs if there are no queued messages
   */
    public void up(Event evt) {
	Message msg;
	switch (evt.getType()) {
	case Event.SET_LOCAL_ADDRESS: myAddr = (Address) evt.getArg();
	    break;

	case Event.MSG:               msg = (Message) evt.getArg();
	    if (Trace.trace) Trace.debug("FLOWCONTROL.up()", "Message received");
	    if (msg.getSrc().equals(myAddr)) {
		if (queuedMsgs.size() > 0) {
		    if (Trace.trace) Trace.debug("FLOWCONTROL.up()", "Message from me received - Queue size was " + queuedMsgs.size());
		    passDown((Event) queuedMsgs.remove(0));
		} else {
		    if (Trace.trace) Trace.debug("FLOWCONTROL.up()", "Message from me received - No messages in queue");
		    sentMsgs--;
		}
	    }
	}
	passUp(evt);
    }

  /**
   * Checs if it can send the message, else puts the message in the queue.
   */
    public void down(Event evt) {
	Message msg;
	if (evt.getType()==Event.MSG) {
	    msg = (Message) evt.getArg();
	    if ((msg.getDest() == null) || (msg.getDest().equals(myAddr))) {
		if (sentMsgs < MAXSENTMSGS) {
		    sentMsgs++;
		    if (Trace.trace) Trace.debug("FLOWCONTROL.down()", "Message " + sentMsgs + " sent");
		} else {
		    queuedMsgs.add(evt); //queues message (we add the event to avoid creating a new event to send the message)
		    return;
		}
	    }
	}
	passDown(evt);
    }

}
