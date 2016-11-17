package org.jgroups.tests.helpers;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.GMS;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class SendUnicast {
    public void sendUnicast(GMS gms,Address dest) {
        Message msg=new Message(dest, "sorry for the interruption :-)");
        gms.down(new Event(Event.MSG, msg));
        // System.out.printf("** injected message %s\n", msg.printHeaders());
    }
}
