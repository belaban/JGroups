package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.UNICAST3;

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
        final Message msg=new Message(ch.getAddress(), "hello-1").src(ch.getAddress());

        // Add a UNICAST2 header
        final UNICAST3 unicast=(UNICAST3)ch.getProtocolStack().findProtocol(UNICAST3.class);
        UNICAST3.Header hdr=UNICAST3.Header.createDataHeader(1, (short)1, true);
        msg.putHeader(unicast.getId(), hdr);

        new Thread() {
            public void run() {
                unicast.down(new Event(Event.MSG, msg));
            }
        }.start();
    }

}
