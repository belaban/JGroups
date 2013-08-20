package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.FLUSH.FlushHeader;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.DefaultTimeScheduler;
import org.jgroups.util.TimeScheduler;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Tests the FLUSH STOP_FLUSH behavior
 * @author Dennis Reed
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class FLUSH_STOP_FLUSH_Test {
    static final short FLUSH_ID=ClassConfigurator.getProtocolId(FLUSH.class);
    IpAddress a1;
    FLUSH flush;
    StopFlushInterceptor downInterceptor;
    BlockInterceptor upInterceptor;

    @BeforeMethod
    public void setUp() throws Exception {
        a1=new IpAddress(1111);

        flush=new FLUSH();
        downInterceptor = new StopFlushInterceptor(a1);
        downInterceptor.setUpProtocol(flush);
        flush.setDownProtocol(downInterceptor);

        TP transport=new TP() {
            public boolean supportsMulticasting() {return false;}
            public void sendMulticast(byte[] data, int offset, int length) throws Exception {}
            public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
            public String getInfo() {return null;}
            public Object down(Event evt) { return null; }
            protected PhysicalAddress getPhysicalAddress() {return null;}
            public TimeScheduler getTimer() {return new DefaultTimeScheduler(1);}
        };
        downInterceptor.setDownProtocol(transport);

        upInterceptor = new BlockInterceptor();
        flush.setUpProtocol(upInterceptor);

        flush.start();

        List<Address> members=new ArrayList<Address>(1);
        members.add(a1);
        View view=new View(a1, 1, members);

        // set the local address
        flush.down(new Event(Event.SET_LOCAL_ADDRESS,a1));

        // set dummy view
        flush.up(new Event(Event.VIEW_CHANGE,view));
    }

    @AfterMethod
    protected void tearDown() {
        flush.stop();
    }

    @Test
    public void testStopFlush() throws InterruptedException {
        flush.down(new Event(Event.SUSPEND));
        Assert.assertTrue(upInterceptor.isBlocked());

        flush.down(new Event(Event.RESUME));
        Assert.assertFalse(upInterceptor.isBlocked());

        // Verify flushParticipants is set correctly on the STOP_FLUSH message
        Collection<Address> flushParticipants = downInterceptor.getFlushParticipants();
        Assert.assertNotNull(flushParticipants);
        Assert.assertEquals(1, flushParticipants.size());
        Assert.assertTrue(flushParticipants.contains(a1));
    }

    @Test
    public void testRogueStopFlush() throws InterruptedException {
        flush.down(new Event(Event.SUSPEND));
        Assert.assertTrue(upInterceptor.isBlocked());

        // STOP_FLUSH that is not addressed to this member
        Address a2 = new IpAddress(2222);
        Message msg = new Message(null, a2, null);
        Collection<Address> flushMembers = new ArrayList<Address>();
        flushMembers.add(a2);
        msg.putHeader(FLUSH_ID, new FlushHeader(FlushHeader.STOP_FLUSH, 1, flushMembers));
        flush.up(new Event(Event.MSG, msg));

        // Should still be blocked
        Assert.assertTrue(upInterceptor.isBlocked());
    }

    static class StopFlushInterceptor extends Protocol {
        private Collection<Address> flushParticipants;
        private final Address       address;


        public StopFlushInterceptor ( Address address ) {
            this.address = address;
        }

        public String getName () {
            return "StopFlushInterceptor";
        }

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                FlushHeader hdr=(FlushHeader)msg.getHeader(FLUSH_ID);
                if(hdr != null &&  hdr.getType() == FlushHeader.STOP_FLUSH)
                    this.flushParticipants = hdr.getFlushParticipants();

                // loopback
                if(msg.getDest() == null || msg.getDest().equals(this.address))
                {
                    msg.setSrc(this.address);
                    getUpProtocol().up(evt);
                }
            }

            return super.down(evt);
        }

        public Collection<Address> getFlushParticipants ()
        {
            return this.flushParticipants;
        }
    }

    static class BlockInterceptor extends Protocol {
        private boolean blocked = false;
        
        public BlockInterceptor () {
        }

        public String getName () {
            return "BlockInterceptor";
        }

        public Object up(Event evt) {
            if(evt.getType() == Event.BLOCK) {
                this.blocked = true;
            } else if(evt.getType() == Event.UNBLOCK) {
                this.blocked = false;
            }

            return null;
        }

        public boolean isBlocked()
        {
            return this.blocked;
        }
    }
}
