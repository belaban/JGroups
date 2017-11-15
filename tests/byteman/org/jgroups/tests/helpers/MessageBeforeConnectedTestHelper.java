package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.BytesMessage;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.UnicastHeader3;

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
        final Message msg=new BytesMessage(ch.getAddress(), "hello-1").setSrc(ch.getAddress());

        // Add a UNICAST2 header
        final UNICAST3 unicast=ch.getProtocolStack().findProtocol(UNICAST3.class);
        UnicastHeader3 hdr=UnicastHeader3.createDataHeader(1, (short)1, true);
        msg.putHeader(unicast.getId(), hdr);

        new Thread() {
            public void run() {
                unicast.down(msg);
            }
        }.start();
    }

}
