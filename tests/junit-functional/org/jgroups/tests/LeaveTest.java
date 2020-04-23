package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  4.0.19
 */
@Test(groups={Global.FUNCTIONAL,Global.ENCRYPT},singleThreaded=true)
public class LeaveTest extends BaseLeaveTest  {

    @AfterMethod protected void destroy() {
        super.destroy();
    }

    /** For some obscure TestNG reasons, this method is needed. Remove it and all tests are executed in separate threads,
     * which makes the testsuite fail!!! */
    public void dummy() {}

    protected JChannel create(String name) throws Exception {
        return new JChannel(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          // omit MERGE3 from the stack -- nodes are leaving gracefully
          new NAKACK2().useMcastXmit(false),
          new UNICAST3(),
          new STABLE(),
          new GMS().setJoinTimeout(1000).printLocalAddress(false))
          .name(name);
    }
}
