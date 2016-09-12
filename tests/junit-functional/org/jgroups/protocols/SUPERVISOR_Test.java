package org.jgroups.protocols;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.rules.Rule;
import org.jgroups.protocols.rules.SUPERVISOR;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED},singleThreaded=true)
public class SUPERVISOR_Test {
    protected JChannel a, b;

    @BeforeMethod
    protected void setup() throws Exception {
        a=createChannel("A");
        a.connect("SUPERVISOR_Test");
        b=createChannel("B");
        b.connect("SUPERVISOR_Test");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
    }

    @AfterMethod
    protected void destroy() {
        Util.close(b,a);
    }

    public void testFailureDetectionRule() {
        SUPERVISOR sva=(SUPERVISOR)a.getProtocolStack().findProtocol(SUPERVISOR.class);
        sva.installRule(500, new RestartFailureDetector());

        SUPERVISOR svb=(SUPERVISOR)b.getProtocolStack().findProtocol(SUPERVISOR.class);
        svb.installRule(500, new RestartFailureDetector());

        assertFailureDetectorRunning(a, b);

        System.out.println("stopping failure detection, waiting for failure detection restart rule to restart failure detection");
        stopFailureDetection(a, b);

        for(int i=0; i < 10; i++) {
            if(isFailureDetectionRunning(a, b))
                break;
            Util.sleep(500);
        }

        assertFailureDetectorRunning(a, b);
    }

    protected boolean isFailureDetectionRunning(JChannel ... channels) {
        for(JChannel ch: channels) {
            FD fd=(FD)ch.getProtocolStack().findProtocol(FD.class);
            if(!fd.isMonitorRunning())
                return false;
        }
        return true;
    }

    protected JChannel createChannel(String name) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new SHARED_LOOPBACK_PING(),
                                 new FD(),
                                 new NAKACK2().setValue("use_mcast_xmit", false),
                                 new UNICAST3(),
                                 new STABLE().setValue("max_bytes", 50000),
                                 new GMS().setValue("print_local_addr", false),
                                 new SUPERVISOR());
        ch.setName(name);
        return ch;
    }


    protected void assertFailureDetectorRunning(JChannel ... channels) {
        for(JChannel ch: channels) {
            System.out.print("Checking " + ch.getName() + ": ");
            FD fd=(FD)ch.getProtocolStack().findProtocol(FD.class);
            assert fd.isMonitorRunning();
            System.out.println("running");
        }
    }

    protected void stopFailureDetection(JChannel ... channels) {
        for(JChannel ch: channels) {
            FD fd=(FD)ch.getProtocolStack().findProtocol(FD.class);
            fd.stopFailureDetection();
        }
    }


    protected static class RestartFailureDetector extends Rule {
        protected FD fd;

        public String name()        {return "MyRule";}
        public String description() {return "Checks if FD.Monitor is running and - if not - starts it";}

        public void init() {
            super.init();
            fd=(FD)sv.getProtocolStack().findProtocol(FD.class);
            if(fd == null)
                throw new IllegalStateException("FD not found in stack");
        }

        public boolean eval()      {return sv.getView().size() > 1 &&  !fd.isMonitorRunning();}
        public String  condition() {return "The view size is > 1 (" + sv.getView() + ") but the monitor is not running";}

        public void trigger() throws Throwable {
            System.out.println(getClass().getSimpleName() + ": restarting FD.Monitor: view=" + sv.getView());
            fd.startFailureDetection();
        }
    }


}
