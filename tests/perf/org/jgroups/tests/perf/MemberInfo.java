package org.jgroups.tests.perf;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: MemberInfo.java,v 1.2 2004/01/23 02:19:48 belaban Exp $
 */
public class MemberInfo {
    public  long start=0;
    public  long stop=0;
    public  long num_msgs_expected=0;
    public  long num_msgs_received=0;
    boolean done=false;
    long    total_bytes_received=0;

    public MemberInfo(long num_msgs_expected) {
        this.num_msgs_expected=num_msgs_expected;
    }
}
