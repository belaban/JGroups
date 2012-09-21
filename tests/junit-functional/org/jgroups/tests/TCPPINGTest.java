package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.debug.Simulator;
import org.jgroups.stack.IpAddress;
import org.jgroups.Address;
import org.jgroups.stack.Protocol;
import org.jgroups.protocols.TCPPING;
import org.jgroups.protocols.PingHeader;

import java.util.Properties;
import java.util.Vector;
import java.util.Set;
import java.util.HashSet;

/**
 * Tests the TCPPING protocol
 * @author Dennis Reed
 */
public class TCPPINGTest extends TestCase {
    IpAddress a1, a2;
    Simulator s;
    MessageInterceptor interceptor;
	String old_initial_hosts;

    public TCPPINGTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        // Clear jgroups.tcpping.initial_hosts, because we need to override it
        old_initial_hosts = System.clearProperty ( "jgroups.tcpping.initial_hosts" );

        super.setUp();
        a1=new IpAddress(1111); // own address
        a2=new IpAddress(2222);
        Vector members=new Vector();
        members.add(a1);
        View v=new View(a1, 1, members);
        s=new Simulator();
        s.setLocalAddress(a1);
        s.setView(v);
        s.addMember(a1);
        TCPPING tcpping=new TCPPING();
        Properties props=new Properties();
        props.setProperty("initial_hosts", "127.0.0.1[1111],127.0.0.1[2222]");
        props.setProperty("max_dynamic_hosts", "10"); // for testDynamicMembers
        tcpping.setPropertiesInternal(props);
        interceptor = new MessageInterceptor();
        Protocol[] stack=new Protocol[]{tcpping, interceptor};
        s.setProtocolStack(stack);
        s.start();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        s.stop();

        // Reset previous value of jgroups.tcpping.initial_hosts
        System.setProperty ( "jgroups.tcpping.initial_hosts", old_initial_hosts);
    }

    public void testTCPPING() throws InterruptedException {
        Event evt=new Event(Event.FIND_INITIAL_MBRS);
        s.send(evt);

        Set<Address> pinged = interceptor.getPinged();
        assertEquals(1,pinged.size());
        assertFalse(pinged.contains(a1));
        assertTrue(pinged.contains(a2));
    }

    public void testDynamicMembers() throws InterruptedException {
        // Send view contianing a new unknown member
        IpAddress a3=new IpAddress(3333);
        Vector members=new Vector();
        members.add(a1);
        members.add(a3);
        View v=new View(a1, 2, members);
        Event evt=new Event(Event.VIEW_CHANGE, v);
        s.send(evt);

        evt=new Event(Event.FIND_INITIAL_MBRS);
        s.send(evt);

        Set<Address> pinged = interceptor.getPinged();
        assertEquals(2,pinged.size());
        assertFalse(pinged.contains(a1));
        assertTrue(pinged.contains(a2));
        assertTrue(pinged.contains(a3));
    }

    static class MessageInterceptor extends Protocol {
        Set<Address> pinged;

        public MessageInterceptor () {
            reset();
        }

        public String getName () {
            return "MessageInterceptor";
        }

        public void reset () {
            this.pinged = new HashSet<Address>();
        }

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                PingHeader hdr=(PingHeader)msg.getHeader("TCPPING");
                if(hdr != null && hdr.type == PingHeader.GET_MBRS_REQ) {
                    pinged.add(msg.getDest());
                }
            }

            return super.down(evt);
        }

        public Set<Address> getPinged ()
        {
            return pinged;
        }
    }

    public static Test suite() {
        return new TestSuite(TCPPINGTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
