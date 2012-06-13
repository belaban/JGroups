package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.debug.Simulator;
import org.jgroups.stack.IpAddress;
import org.jgroups.Address;
import org.jgroups.stack.Protocol;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.util.Digest;
import org.jgroups.util.Range;

import java.util.Properties;
import java.util.Vector;

/**
 * Tests the NAKACK protocol's REBROADCAST behavior
 * @author Dennis Reed
 */
public class NAKACK_REBROADCAST_Test extends TestCase {
    IpAddress a1, a2;
    NAKACK nak;
    MessageInterceptor interceptor;
    Simulator s;

    public NAKACK_REBROADCAST_Test(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        a1=new IpAddress(1111); // own address
        a2=new IpAddress(2222);
        Vector members=new Vector();
        members.add(a1);
        members.add(a2);
        View v=new View(a1, 1, members);
        s=new Simulator();
        s.setLocalAddress(a1);
        s.setView(v);
        s.addMember(a1);
        NAKACK nak=new NAKACK();
        interceptor = new MessageInterceptor();
        Protocol[] stack=new Protocol[]{nak, interceptor};
        s.setProtocolStack(stack);
        s.start();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        s.stop();
    }

    public void testRebroadcast() throws InterruptedException {
        Digest digest=new Digest(a2, 0, 2, 2);
        Event evt=new Event(Event.REBROADCAST, digest);
        s.send(evt);

        Range range = interceptor.getRange();
        interceptor.reset();
        assertNotNull(range);
        assertEquals(1, range.low);
        assertEquals(2, range.high);
    }

    public void testRebroadcastSingle() throws InterruptedException {
        Digest digest=new Digest(a2, 0, 1, 1);
        Event evt=new Event(Event.REBROADCAST, digest);
        s.send(evt);

        Range range = interceptor.getRange();
        interceptor.reset();
        assertNotNull(range);
        assertEquals(1, range.low);
        assertEquals(1, range.high);
    }

    static class MessageInterceptor extends Protocol {
        private Range range;

        public MessageInterceptor () {
        }

        public String getName () {
            return "MessageInterceptor";
        }

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                NakAckHeader hdr=(NakAckHeader)msg.getHeader("NAKACK");
                if(hdr != null && hdr.type == NakAckHeader.XMIT_REQ) {
                    this.range=hdr.range;
                }
            }

            return super.down(evt);
        }

        public Range getRange ()
        {
            return this.range;
        }

        public void reset ()
        {
            this.range = null;
        }
    }

    public static Test suite() {
        return new TestSuite(NAKACK_REBROADCAST_Test.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
