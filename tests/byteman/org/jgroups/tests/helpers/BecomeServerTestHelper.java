package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Message;
import org.jgroups.util.BoundedList;

/**
 * @author Bela Ban
 * @since 3.3
 */
public class BecomeServerTestHelper extends Helper {

    protected BecomeServerTestHelper(Rule rule) {
        super(rule);
    }

    /**
     * Checks if any of the messages in the list has a non-zero length
     * @param list
     * @return
     */
    public boolean messageReceived(BoundedList<Message> list) {
        if(list == null || list.isEmpty())
            return false;
        for(Message msg: list)
            if(msg.getLength() > 0)
                return true;
        return false;
    }

    public int rv(String rv_name, String ctx) {
        traceln(String.format("-- acquiring rendezvous %s (ctx: %s)\n", rv_name, ctx));
        int rc=rendezvous(rv_name);
        traceln(String.format("-- acquired rendezvous %s (ctx: %s): rc=%d\n", rv_name, ctx, rc));
        return rc;
    }
}
