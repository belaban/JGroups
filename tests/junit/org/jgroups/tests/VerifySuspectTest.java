package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.VERIFY_SUSPECT;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.Vector;


/**
 * Tests the VERIFY_SUSPECT protocol
 * @author Dennis Reed
 */
public class VerifySuspectTest extends TestCase {
    IpAddress a1;
    IpAddress a2;
    Vector members;
    View v;
    Simulator s;

    public VerifySuspectTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();

        a1=new IpAddress(1111);
        a2=new IpAddress(2222);

        members=new Vector();
        members.add(a1);
        members.add(a2);
        v=new View(a1, 1, members);

        s=new Simulator();
        s.setLocalAddress(a1);
        s.setView(v);
        s.addMember(a1);

        VERIFY_SUSPECT v=new VERIFY_SUSPECT();
        Properties props=new Properties();
        props.setProperty("bind_addr", "127.0.0.1");
        props.setProperty("timeout", "100");
        v.setProperties(props);

        Protocol[] stack=new Protocol[]{v};
        s.setProtocolStack(stack);

        s.start();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        s.stop();
    }

    // test basic functionality
    public void testVerify() {
        Receiver r=new Receiver();
        s.setReceiver(r);

        // trigger the timer
        s.receive(new Event(Event.SUSPECT,a2));

        Util.sleep(50);

        // should not be suspected yet
        assertEquals(r.isSuspected(), false);

        Util.sleep(100);

        // should be suspected now
        assertEquals(r.isSuspected(), true);
    }

    // test JGRP-1382
    // Member should not be suspected after it has left the cluster
    public void testJGRP1382() {
        Receiver r=new Receiver();
        s.setReceiver(r);

        // trigger the timer
        s.receive(new Event(Event.SUSPECT,a2));

        Util.sleep(50);

        // send new view without a2
        members.remove(a2);
        v=new View(a1, 2, members);
        Event evt=new Event(Event.VIEW_CHANGE, v);
        s.send(evt);

        Util.sleep(100);

        // should not be suspected
        assertEquals(r.isSuspected(), false);
    }

    static class Receiver implements Simulator.Receiver {
        boolean suspected = false;

        public void receive(Event evt) {
            if(evt.getType() == Event.SUSPECT) {
				suspected = true;
            }
        }

        boolean isSuspected()
        {
            return suspected;
        }
    }

    public static Test suite() {
        return new TestSuite(VerifySuspectTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
