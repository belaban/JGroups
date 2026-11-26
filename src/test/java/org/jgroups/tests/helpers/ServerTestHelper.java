package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Address;
import org.jgroups.tests.byteman.ServerTest;

/**
 * @author Bela Ban
 * @since  3.3
 */
public class ServerTestHelper extends Helper {
    protected ServerTestHelper(Rule rule) {
        super(rule);
    }

    public static boolean isAddress(Object obj) {
        return obj instanceof Address;
    }

    public static boolean isA(Object obj) {
        return obj.equals(ServerTest.A);
    }
    public static boolean isB(Object obj) {
        return obj.equals(ServerTest.B);
    }

}
