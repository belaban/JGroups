package org.jgroups.tests.perf;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: MemberInfo.java,v 1.1 2004/01/23 00:08:31 belaban Exp $
 */
public class MemberInfo {
    public long num_msgs_expected=0;
    public long num_msgs_received=0;

    public MemberInfo(long num_msgs_expected) {
        this.num_msgs_expected=num_msgs_expected;
    }
}
