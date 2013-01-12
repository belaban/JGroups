package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Address;
import org.jgroups.tests.byteman.TCPConnectionMapTest;

/**
 * @author Bela Ban
 * @since  3.3
 */
public class TCPConnectionMapTestHelper extends Helper {
    protected TCPConnectionMapTestHelper(Rule rule) {
        super(rule);
    }

    public boolean isAddress(Object obj) {
        return obj instanceof Address;
    }

    public boolean isA(Object obj) {
        return obj.equals(TCPConnectionMapTest.A);
    }

   /* public void stackTrace() {
        try {
            throw new Exception();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }*/

}
