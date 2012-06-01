package org.jgroups.tests.byteman;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @since 3.1
 */
public class SequencerFailoverTestHelper extends Helper {
    protected SequencerFailoverTestHelper(Rule rule) {
        super(rule);
    }

    public void sendMessages(final Protocol prot, final int start, final int end) {
        new Thread() {
            public void run() {
                for(int i=start; i <= end; i++) {
                    Message msg=new Message(null, i);
                    System.out.println("--> Sending message " + i);
                    prot.down(new Event(Event.MSG, msg));
                }
            }
        }.start();
        Util.sleep(1000);
    }
}
