// $Id: ProtocolTester.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.debug;


import org.jgroups.Message;
import org.jgroups.Event;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;




/**
 * Generic class to test one or more protocol layers directly. Sets up a protocol stack consisting of
 * the top layer (which is essentially given by the user and is the test harness), the specified protocol(s) and
 * a bottom layer (which is automatically added), which sends all received messages immediately back up the
 * stack (swapping sender and receiver address of the message).
 * @author Bela Ban, March 23 2001
 */
public class ProtocolTester {
    Protocol     harness=null, top, bottom;
    String       props=null;
    Configurator config=null;



    public ProtocolTester(String prot_spec, Protocol harness, boolean trace) throws Exception {
	if(prot_spec == null || harness == null)
	    throw new Exception("ProtocolTester(): prot_spec or harness is null");

	props=prot_spec;
	this.harness=harness;
	props="LOOPBACK:" + props; // add a loopback layer at the bottom of the stack

	if(trace)
	    Trace.init();

	config=new Configurator();
	top=config.setupProtocolStack(props, null);
	harness.setDownProtocol(top);
	top.setUpProtocol(harness); // +++

	bottom=getBottomProtocol(top);
	config.startProtocolStack(bottom);

	// has to be set after StartProtocolStack, otherwise the up and down handler threads in the harness
	// will be started as well (we don't want that) !
	// top.setUpProtocol(harness);
    }

    
    public String getProtocolSpec() {
	return props;
    }


    public void stop() {
        Protocol p;
	if(harness != null) {
            p=harness;
            while(p != null) {
                p.stop();
                p=p.getDownProtocol();
            }
	    config.stopProtocolStack(harness);
	}
	else if(top != null) {
            p=top;
            while(p != null) {
                p.stop();
                p=p.getDownProtocol();
            }
	    config.stopProtocolStack(top);
	}
    }

    
    Protocol getBottomProtocol(Protocol top) {
	Protocol tmp;

	if(top == null)
	    return null;
	
	tmp=top;
	while(tmp.getDownProtocol() != null)
	    tmp=tmp.getDownProtocol();
	return tmp;
    }




    public static void main(String[] args) {
	String         props;
	ProtocolTester t;
	Harness        h;
	boolean        trace=false;

	if(args.length < 1 || args.length > 2) {
	    System.out.println("ProtocolTester <protocol stack spec> [-trace]");
	    return;
	}
	props=args[0];
	if(args.length == 2 && args[1].equals("-trace"))
	    trace=true;

	try {
	    h=new Harness();
	    t=new ProtocolTester(props, h, trace);
	    System.out.println("protocol specification is " + t.getProtocolSpec());
	    h.down(new Event(Event.BECOME_SERVER));
	    for(int i=0; i < 5; i++) {
		System.out.println("Sending msg #" + i);
		h.down(new Event(Event.MSG, new Message(null, null, "Hello world #" + i)));
	    }
	    Util.sleep(500);
	    t.stop();
	}
	catch(Exception ex) {
	    System.err.println(ex);
	}
    }



    private static class Harness extends Protocol {

	public String getName() {
	    return "Harness";
	}


	public void up(Event evt) {
	    System.out.println("Harness.up(): " + evt);
	}

    }
    
}
