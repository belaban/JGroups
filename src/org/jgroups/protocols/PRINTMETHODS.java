// $Id: PRINTMETHODS.java,v 1.3 2004/03/30 06:47:21 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.blocks.MethodCall;
import org.jgroups.stack.Protocol;


public class PRINTMETHODS extends Protocol {

    public PRINTMETHODS() {}

    public String        getName()             {return "PRINTMETHODS";}


    public void up(Event evt) {
	Object       obj=null;
	byte[]       buf;
    	Message      msg;

	if(evt.getType() == Event.MSG) {
	    msg=(Message)evt.getArg();
	    if(msg.getLength() > 0) {
		try {
		    obj=msg.getObject();
		    if(obj != null && obj instanceof MethodCall)
			System.out.println("--> PRINTMETHODS: received " + obj);
		}
		catch(ClassCastException cast_ex) {}
		catch(Exception e) {}
	    }
	}

	passUp(evt);
    }
    


    public void down(Event evt) {
	Object       obj=null;
	byte[]       buf;
	Message      msg;

	if(evt.getType() == Event.MSG) {

	}
	passDown(evt);
    }




}
