// $Id: UpHandlerTest.java,v 1.2 2004/03/30 06:47:34 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;



/**
 * Uses the pass-though facility of the Channel: events are passed (mostly) unfiltered from the channel
 * to the application. The app quickly joins a groups, sends a message and then leaves again. The events
 * received during this period are shown.
 * @author Bela Ban
 */
public class UpHandlerTest implements UpHandler {
    Channel channel;

    public void start() throws Exception {
	channel=new JChannel();
	channel.setUpHandler(this);
	channel.connect("UpHandlerTestGroup");

	channel.send(new Message(null, null, new String("Hello").getBytes()));
	Util.sleep(2000);
	channel.close();
    }


    public void up(Event evt) {
	System.out.println(evt);
    }


    public static void main(String[] args) {
	try {
	    new UpHandlerTest().start();
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }
}
