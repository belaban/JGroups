package org.jgroups.tests.helpers;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.protocols.pbcast.GMS;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class SendUnicast {
    public void sendUnicast(GMS gms,Address dest) {
        gms.down(new BytesMessage(dest, "sorry for the interruption :-)"));
    }
}
