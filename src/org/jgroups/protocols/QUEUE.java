// $Id: QUEUE.java,v 1.1.1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;

import java.util.Properties;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;


/**
 * Queuing layer. Upon reception of event START_QUEUEING, all events traveling through
 * this layer upwards/downwards (depending on direction of event) will be queued. Upon
 * reception of a STOP_QUEUEING event, all events will be released. Finally, the
 * queueing flag is reset.
 * When queueing, only event STOP_QUEUEING (received up or downwards) will be allowed
 * to release queueing.
 * @author Bela Ban
 */

public class QUEUE extends Protocol {
    Vector    up_vec=new Vector();
    Vector    dn_vec=new Vector();
    boolean   queueing_up=false, queueing_dn=false;
    Observer  observer=null;


    public interface Observer {
	/** Called before event is added. Blocks until call returns.
	    @param evt The event
	    @param num_events The number of events in the up vector <em>before</em>
	    this event is added
	    @return boolean True if event should be added. False if it should be discarded */
	boolean addingToUpVector(Event evt, int num_events);

	/** Called before event is added. Blocks until call returns.
	    @param evt The event
	    @param num_events The number of events in the down vector <em>before</em>
	    this event is added
	    @return boolean True if event should be added. False if it should be discarded */
	boolean addingToDownVector(Event evt, int num_events);
    }

    /** Only 1 observer is allowed. More than one might slow down the system. Will be called
	when an event is queued (up or down) */
    public void setObserver(Observer observer) {this.observer=observer;}

    public Vector  getUpVector()     {return up_vec;}
    public Vector  getDownVector()   {return dn_vec;}
    public boolean getQueueingUp()   {return queueing_up;}
    public boolean getQueueingDown() {return queueing_dn;}


    /** All protocol names have to be unique ! */
    public String  getName() {return "QUEUE";}


    public Vector providedUpServices() {
	Vector ret=new Vector();
	ret.addElement(new Integer(Event.START_QUEUEING));
	ret.addElement(new Integer(Event.STOP_QUEUEING));
	return ret;
    }

    public Vector providedDownServices() {
	Vector ret=new Vector();
	ret.addElement(new Integer(Event.START_QUEUEING));
	ret.addElement(new Integer(Event.STOP_QUEUEING));
	return ret;
    }


    public boolean setProperties(Properties props) {
	String     str;

	if(props.size() > 0) {
	    System.err.println("QUEUE.setProperties(): these properties are not recognized:");
	    props.list(System.out);
	    return false;
	}
	return true;
    }



    /**
       Queues or passes up events. No queue sync. necessary, as this method is never called
       concurrently.
     */
    public void up(Event evt) {
	Message msg;
	Vector  event_list;  // to be passed up *before* replaying event queue
	Event   e;


	switch(evt.getType()) {

	case Event.START_QUEUEING:  // start queueing all up events
	    if(Trace.trace) Trace.info("QUEUE.up()", "received START_QUEUEING");
	    queueing_up=true;
	    return;

	case Event.STOP_QUEUEING:         // stop queueing all up events
	    event_list=(Vector)evt.getArg();
	    if(event_list != null)
		for(int i=0; i < event_list.size(); i++)
		    passUp((Event)event_list.elementAt(i));
	    
	    if(Trace.trace) Trace.info("QUEUE.up()", "replaying up events");
	    
	    for(int i=0; i < up_vec.size(); i++) {
		e=(Event)up_vec.elementAt(i);
		passUp(e);
	    }

	    up_vec.removeAllElements();
	    queueing_up=false;
	    return;
	}
	
	if(queueing_up) {
	    if(Trace.trace) {
		Trace.info("QUEUE.up()", "queued up event " + evt);
	    }
	    if(observer != null) {
		if(observer.addingToUpVector(evt, up_vec.size()) == false)
		    return;  // discard event (don't queue)
	    }
	    up_vec.addElement(evt);
	}
	else
	    passUp(evt);            // Pass up to the layer above us
    }




    
    public void down(Event evt) {
	Message msg;
	Vector  event_list;  // to be passed down *before* replaying event queue

	switch(evt.getType()) {
	    
	case Event.START_QUEUEING:  // start queueing all down events
	    if(Trace.trace) Trace.info("QUEUE.down()", "received START_QUEUEING");
	    queueing_dn=true;
	    return;

	case Event.STOP_QUEUEING:         // stop queueing all down events	    
	    if(Trace.trace) Trace.info("QUEUE.down()", "received STOP_QUEUEING");
	    event_list=(Vector)evt.getArg();
	    if(event_list != null)  // play events first (if available)
		for(int i=0; i < event_list.size(); i++)
		    passDown((Event)event_list.elementAt(i));
	    
	    if(Trace.trace) Trace.info("QUEUE.down()", "replaying down events ("+ dn_vec.size() +")");
	    
	    for(int i=0; i < dn_vec.size(); i++) {
		passDown((Event)dn_vec.elementAt(i));
	    }

	    dn_vec.removeAllElements();
	    queueing_dn=false;
	    return;
  	}
	    
	if(queueing_dn) {
	    if(Trace.trace)
		Trace.info("QUEUE.down()", "queued down event: " + Util.printEvent(evt));

	    if(observer != null) {
		if(observer.addingToDownVector(evt, dn_vec.size()) == false)
		    return;  // discard event (don't queue)
	    }
	    dn_vec.addElement(evt);
	}
	else
	    passDown(evt);          // Pass up to the layer below us
    }



}
