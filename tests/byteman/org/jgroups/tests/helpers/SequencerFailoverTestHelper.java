package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.BytesMessage;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;

/**
 * @author Bela Ban
 * @since 3.1
 */
public class SequencerFailoverTestHelper extends Helper {

    protected SequencerFailoverTestHelper(Rule rule) {
        super(rule);
    }

    public void sendMessages(final Protocol prot, final int start, final int end) {
        final Thread sender=new Thread() {
            public void run() {
                for(int i=start; i <= end; i++) {
                    Message msg=new BytesMessage(null, i);
                    System.out.println("[" + prot.getValue("local_addr") + "] --> sending message " + i);
                    prot.down(msg);
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
