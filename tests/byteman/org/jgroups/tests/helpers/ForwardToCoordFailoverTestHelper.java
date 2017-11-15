package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.BytesMessage;
import org.jgroups.Event;
import org.jgroups.stack.Protocol;

/**
 * @author Bela Ban
 * @since 3.3
 */
public class ForwardToCoordFailoverTestHelper extends Helper {

    protected ForwardToCoordFailoverTestHelper(Rule rule) {
        super(rule);
    }

    public void sendMessages(final Protocol prot, final int start, final int end) {
        final Thread sender=new Thread() {
            public void run() {
                for(int i=start; i <= end; i++) {
                    Event evt=new Event(Event.FORWARD_TO_COORD, new BytesMessage(null, i));
                    System.out.println("[byteman] --> sending message " + i);
                    prot.down(evt);
                }
            }
        };
        sender.setName("BytemanSenderThread");
        sender.start();
        try {
            sender.join(1000);
        }
        catch(InterruptedException e) {
        }
    }
}
