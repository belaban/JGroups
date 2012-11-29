package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.UNICAST2;

/**
 * @author Bela Ban
 * @since 3.3
 */
public class MessageBeforeConnectedTestHelper extends Helper {

    protected MessageBeforeConnectedTestHelper(Rule rule) {
        super(rule);
    }

    /**
     * Sends a unicast message up UNICAST2
     */
    public void sendUnicast(JChannel ch) throws Exception {
        final Message msg=new Message(ch.getAddress(), ch.getAddress(), "hello-1");

        // Add a UNICAST2 header
        final UNICAST2 unicast=(UNICAST2)ch.getProtocolStack().findProtocol(UNICAST2.class);
        UNICAST2.Unicast2Header hdr=UNICAST2.Unicast2Header.createDataHeader(1, (short)1, true);
        msg.putHeader(unicast.getId(), hdr);

        new Thread() {
            public void run() {
                unicast.up(new Event(Event.MSG, msg));
            }
        }.start();
    }

}
