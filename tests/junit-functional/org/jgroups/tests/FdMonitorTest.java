package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;



@Test(groups=Global.FUNCTIONAL)
public class FdMonitorTest {
    protected static final String CLUSTER_NAME = "FdMonitorTest";
    protected JChannel a, b;


    @BeforeMethod
    public void setup() throws Exception {
        a=createChannel("A");
        b=createChannel("B");
    }

    @AfterMethod
    public void destroy() {
        Util.close(b,a);
    }

    public void testFdMonitorActivation() throws Exception {
        a.connect(CLUSTER_NAME);
        assert !FD(a).isMonitorRunning();

        b.connect(CLUSTER_NAME);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        validateFdMonitor("");

        reconnect(b);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        // Util.sleep(60000);
        validateFdMonitor("after B reconnect");
    }


    protected void validateFdMonitor(String msg) {
        assert FD(a).isMonitorRunning() : "A.FD.monitor "+msg;
        assert FD(b).isMonitorRunning() : "B.FD.monitor "+msg;
    }

    protected static void reconnect(JChannel node) throws Exception {
        node.disconnect();
        node.connect(CLUSTER_NAME);
    }

    protected static FD FD(JChannel ch) {
        return (FD) ch.getProtocolStack().findProtocol(FD.class);
    }
    
    protected JChannel createChannel(String name) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new SHARED_LOOPBACK_PING(),
                                 new FD().setValue("timeout", 1000).setValue("max_tries", 3),
                                 new NAKACK2(),
                                 new UNICAST3(),
                                 new GMS().setValue("print_local_addr",false)).name(name);
        ch.setName(name);
        return ch;
    }
}
