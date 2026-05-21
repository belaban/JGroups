package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.BaseBundler;
import org.jgroups.protocols.Bundler;
import org.jgroups.protocols.PerDestinationBundler;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests https://redhat.atlassian.net/browse/JGRP-3008
 * @author Bela Ban
 * @since  5.5.6
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class PerDestinationBundlerTest {
    protected JChannel            a,b;
    protected static final String CLUSTER=PerDestinationBundlerTest.class.getSimpleName();

    @BeforeMethod protected void init() throws Exception {
        a=create("A").connect(CLUSTER);
        b=create("B").connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b);
    }

    @AfterMethod protected void destroy() {
        Util.closeReverse(a,b);
    }

    /**
     * A,B, with A sending messages to B. B leaves but A's send-queue still has 5 messages to B. A's run()
     * method will always spin.
     */
    public void testViewChange() throws Exception {
        // stop A's bundler thread
        PerDestinationBundler pd=(PerDestinationBundler)a.stack().getTransport().getBundler();
        pd.stopSingleThreadRunner();

        // now send 5 messages to B; they'll increment msgs_available to 5 and end up in the queue to B
        for(int i=1; i <=5; i++) {
            Message msg=new ObjectMessage(b.address(), "msg-" + i);
            pd.doSend(msg);
        }

        // Now inject a view A, which removes B
        Util.shutdown(b); // separates B
        View v=new View(a.address(), 5, List.of(a.address()));
        GMS gms=a.stack().findProtocol(GMS.class);
        gms.installView(v);
        Util.waitUntilAllChannelsHaveSameView(5000, 100, a);
        Util.sleep(1000); // wait for B to be removed from A's destinations

        // Now start the runner again: it will spin on msgs_available==5 and never becoming 0
        pd.startSingleThreadRunner();

        // Now wait for msgs_available to become 0
        Util.waitUntil(5000, 100, () -> pd.messagesAvailable() == 0,
                       () -> String.format("msgs_available=%d (expected 0)", pd.messagesAvailable()));

    }

    protected static JChannel create(String name) throws Exception {
        Protocol[] stack=Util.getTestStack();
        TP tp=stack[0].getTransport();
        Bundler bundler=new PerDestinationBundler().useSingleSenderThread(true);
        ((BaseBundler)bundler).removeDelay(100);
        tp.setBundler(bundler);
        return new JChannel(stack).name(name);
    }
}
