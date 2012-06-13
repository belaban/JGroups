package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.Vector;

/**
 * Tests the NAKACK protocol's REBROADCAST behavior
 * @author Dennis Reed
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class NAKACK_REBROADCAST_Test {
    static final short NAKACK_ID=ClassConfigurator.getProtocolId(NAKACK2.class);
    IpAddress a1;
    NAKACK2 nak;
    MessageInterceptor interceptor;

    @BeforeMethod
    public void setUp() throws Exception {
        a1=new IpAddress(1111);

        nak=new NAKACK2();
        interceptor = new MessageInterceptor();
        nak.setDownProtocol(interceptor);
        TP transport=new TP() {
            public boolean supportsMulticasting() {return false;}
            public void sendMulticast(byte[] data, int offset, int length) throws Exception {}
            public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
            public String getInfo() {return null;}
            public Object down(Event evt) {return null;}
            protected PhysicalAddress getPhysicalAddress() {return null;}
            public TimeScheduler getTimer() {return new DefaultTimeScheduler(1);}
        };
        interceptor.setDownProtocol(transport);

		nak.start();

        Digest digest=new Digest(a1, 0, 0);
        Event evt=new Event(Event.SET_DIGEST, digest);
        nak.down(evt);
    }

    @Test
    public void testRebroadcast() throws InterruptedException {
        Digest digest=new Digest(a1, 2, 2);
        Event evt=new Event(Event.REBROADCAST, digest);
        nak.down(evt);

        SeqnoList range = interceptor.getRange();
        Assert.assertNotNull(range);
        Assert.assertEquals(2, range.size());
        for(long i: range)
        	Assert.assertTrue(i == 1 || i == 2);
    }

    @Test
    public void testRebroadcastSingle() throws InterruptedException {
        Digest digest=new Digest(a1, 1, 1);
        Event evt=new Event(Event.REBROADCAST, digest);
        nak.down(evt);

        SeqnoList range = interceptor.getRange();
        Assert.assertNotNull(range);
        Assert.assertEquals(1, range.size());
        for(long i: range)
        	Assert.assertTrue(i == 1);
    }

    static class MessageInterceptor extends Protocol {
        private SeqnoList range;

        public MessageInterceptor () {
        }

        public String getName () {
            return "MessageInterceptor";
        }

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                NakAckHeader2 hdr=(NakAckHeader2)msg.getHeader(NAKACK_ID);
                if(hdr != null && hdr.getType() == NakAckHeader2.XMIT_REQ) {
                    this.range=(SeqnoList)msg.getObject();
                }
            }

            return super.down(evt);
        }

        public SeqnoList getRange ()
        {
            return this.range;
        }
    }
}
