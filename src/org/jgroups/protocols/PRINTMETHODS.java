// $Id: PRINTMETHODS.java,v 1.1.1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.blocks.MethodCall;


public class PRINTMETHODS extends Protocol {

    public PRINTMETHODS() {}

    public String        getName()             {return "PRINTMETHODS";}


    public void up(Event evt) {
	Object       obj=null;
	byte[]       buf;
    	Message      msg;

	if(evt.getType() == Event.MSG) {
	    msg=(Message)evt.getArg();
	    buf=msg.getBuffer();
	    if(buf != null) {
		try {
		    obj=Util.objectFromByteBuffer(buf);
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
