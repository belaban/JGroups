package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.logging.Logger;

/**
 * Test for {@link FD_SOCK2} not closing its sockets within the {@link FD_SOCK2#stop()} call and subsequently failing
 * when subsequently started again (e.g. triggered by redeploy in WildFly). Reproducible by connecting and disconnecting
 * the channel in a loop. Typically happens on a modern machine within seconds.
 *
 * @author Radoslav Husar
 * @see <a href="https://issues.redhat.com/browse/JGRP-2647">JGRP-2647</a>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class FD_SOCK2_Test {

    protected JChannel a;

    public void testSocketClosing() throws Exception {
        a = new JChannel(Util.getTestStack(
                new FD_SOCK2().setPortRange(1).setOffset(1))
        ).name("A");

        for (int i = 0; i < 1_000; i++) {
            try {
                a.connect(FD_SOCK2_Test.class.getSimpleName());
            } catch (Exception e) {
                Logger.getLogger(FD_SOCK2_Test.class.getSimpleName()).info(String.format("FD_SOCK2 socket close test failed on run #%d", i));
                throw e;
            }

            a.disconnect();
        }
    }

    @AfterMethod
    protected void destroy() {
        Util.close(a);
    }
}
