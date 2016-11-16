package org.jgroups.tests.helpers;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.GMS;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class SendUnicast {
    public void sendUnicast(GMS gms,Address dest) {
        gms.down(new Message(dest, "sorry for the interruption :-)"));
    }
}
