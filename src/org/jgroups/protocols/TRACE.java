// $Id: TRACE.java,v 1.3 2007/01/11 12:57:17 belaban Exp $

package org.jgroups.protocols;
import org.jgroups.Event;
import org.jgroups.stack.Protocol;



public class TRACE extends Protocol {

    public TRACE() {}

    public String        getName()             {return "TRACE";}

    

    public Object up(Event evt) {
	System.out.println("---------------- TRACE (received) ----------------------");
	System.out.println(evt);
	System.out.println("--------------------------------------------------------");
	passUp(evt);
    }


    public Object down(Event evt) {
	System.out.println("------------------- TRACE (sent) -----------------------");
	System.out.println(evt);
	System.out.println("--------------------------------------------------------");
	passDown(evt);
    }


    public String toString() {
	return "Protocol TRACE";
    }


}
