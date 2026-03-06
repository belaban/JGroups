package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.protocols.BasicTCP;
import org.jgroups.protocols.FD_SOCK;
import org.jgroups.protocols.FD_SOCK2;
import org.jgroups.protocols.VERIFY_SUSPECT2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.FastArray;
import org.jgroups.util.Util;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Tests A,B; B leaves gracefully or ungracefully with enable_suspect_events set to true.
 * JIRA: https://issues.redhat.com/browse/JGRP-2974
 * @author Bela Ban
 * @since  5.5.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="configs")
public class EnableSuspectEventsTest2 {

    @DataProvider
    static Object[][] configs() {
        return new Object[][]{
          {"tcp.xml"},
          {"tcp-new.xml"},
          {"tcp-nio.xml"},
          {"tcp-nio-new.xml"}
        };
    }

    public void testUngracefulLeaveParticipant(String config) throws Exception {
        _testLeave(config, false, false);
    }

    public void testUngracefulLeaveCoord(String config) throws Exception {
        _testLeave(config, false, true);
    }

    public void testGracefulLeaveParticipant(String config) throws Exception {
        _testLeave(config, true, false);
    }

    public void testGracefulLeaveCoord(String config) throws Exception {
        _testLeave(config, true, true);
    }


    protected static void _testLeave(String cfg, boolean graceful, boolean coordinator_leave) throws Exception {
        try(JChannel a=create(cfg, "A"); JChannel b=create(cfg, "B"); JChannel c=create(cfg, "C")) {
            MyUpHandler up_a=new MyUpHandler(), up_b=new MyUpHandler(), up_c=new MyUpHandler();
            a.setUpHandler(up_a); b.setUpHandler(up_b); c.setUpHandler(up_c);
            a.connect("EnableSuspectEventsTest2");
            b.connect("EnableSuspectEventsTest2");
            c.connect("EnableSuspectEventsTest2");
            Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b,c);
            // A is connected to B and C (as coord), but B and C are not connected, so establish a connection by
            // sending a message from B to C
            b.send(new EmptyMessage(c.address()));
            JChannel leaver=coordinator_leave? a : b, remaining=coordinator_leave? b : a;
            System.out.printf("%s leaves %s:\n", leaver.address(), graceful? "gracefully" : "ungracefully");
            leave(leaver, graceful);
            Util.waitUntil(5000, 50, () -> remaining.view().size() == 2 && c.view().size() == 2,
                           () -> String.format("views: %s", Util.printViews(remaining, c)));
            System.out.printf("Views:\n%s\n", Util.printViews(a,b,c));

            // test that the coord did receive a suspect event if graceful
            MyUpHandler up_coord=(MyUpHandler)remaining.getUpHandler();
            int expected=graceful? 0 : 1;
            BooleanSupplier pred=() -> graceful? up_coord.size() == expected && up_c.size() == expected :
              up_coord.size() == expected && up_c.size() <= expected;
            Util.waitUntilTrue(5000, 100, pred);
            System.out.printf("%s: %d suspicions, C: %d suspicions\n", remaining.address(), up_coord.size(), up_c.size());

            assert up_coord.size() == expected;
            if(graceful)
                assert up_c.size() == 0;
            else
                assert up_c.size() == 0 || up_c.size() == 1;
        }
    }

    protected static void leave(JChannel ch, boolean graceful) throws Exception {
        if(graceful)
            ch.disconnect();
        else
            Util.shutdown(ch, graceful);
    }

    protected static JChannel create(String cfg, String name) throws Exception {
        ProtocolStackConfigurator conf=ConfiguratorFactory.getStackConfigurator(cfg);
        List<ProtocolConfiguration> prots=conf.getProtocolStack();
        for(ProtocolConfiguration config: prots) {
            Map<String,String> props=config.getProperties();
            props.put("preview_warning", "false");
        }
        JChannel ch=new JChannel(conf).name(name);
        ProtocolStack stack=ch.stack();
        stack.removeProtocols(FD_SOCK.class, FD_SOCK2.class);
        BasicTCP transport=(BasicTCP)stack.getTransport();
        transport.enableSuspectEvents(true);
        GMS gms=stack.findProtocol(GMS.class);
        gms.setJoinTimeout(500);
        VERIFY_SUSPECT2 vs=stack.findProtocol(VERIFY_SUSPECT2.class);
        if(vs != null)
            vs.setTimeout(500);
        return ch;
    }

    protected static class MyUpHandler implements UpHandler {
        protected final List<Address> suspected_members=new FastArray<>();

        protected int size() {return suspected_members.size();}

        @Override
        public UpHandler setLocalAddress(Address a) {return null;}

        @Override
        public Object up(Event evt) {
            if(evt.type() == Event.SUSPECT)
                suspected_members.addAll(evt.arg());
            return null;
        }

        @Override public Object up(Message msg) {return null;}

        @Override
        public String toString() {
            return String.format("num_suspicions: %d", size());
        }
    }
}
