package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Util;
import org.jgroups.util.MergeId;
import org.jgroups.util.TimeScheduler;
import org.jgroups.debug.Simulator;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.*;

import java.nio.ByteBuffer;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the GMS protocol for merging functionality
 * @author Bela Ban
 * @version $Id: GMS_MergeTest.java,v 1.2 2009/05/19 15:35:32 belaban Exp $
 */
@Test(groups={Global.STACK_INDEPENDENT})
public class GMS_MergeTest extends ChannelTestBase {
    static final String props="SHARED_LOOPBACK:PING:pbcast.NAKACK:UNICAST:pbcast.STABLE:pbcast.GMS:FC:FRAG2";

    private JChannel c1, c2, c3;


    @BeforeClass
    void setUp() throws Exception {
        c1=new JChannel(props);
        c2=new JChannel(props);
        c3=new JChannel(props);
        c1.connect("GMS_MergeTest");
        c2.connect("GMS_MergeTest");
        c3.connect("GMS_MergeTest");
    }


    @AfterClass
    void tearDown() throws Exception {
        Util.close(c3, c2, c1);
    }


    /**
     * Simulates the death of a merge leader after having sent a MERG_REQ. Because there is no MergeView or CANCEL_MERGE
     * message, the MergeCanceller has to null merge_id after a timeout
     */
    public void testMergeRequestTimeout() throws Exception {
        Message merge_request=new Message();
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ);
        MergeId new_merge_id=MergeId.create(c1.getAddress());
        hdr.setMergeId(new_merge_id);
        merge_request.putHeader(GMS.name, hdr);
        GMS gms=(GMS)c1.getProtocolStack().findProtocol(GMS.class);
        MergeId merge_id=gms.getMergeId();
        assert merge_id == null;
        gms.up(new Event(Event.MSG, merge_request));
        merge_id=gms.getMergeId();
        assert new_merge_id.equals(merge_id);

        long timeout=(long)(gms.getMergeTimeout() * 1.5);
        System.out.println("sleeping for " + timeout + " ms, then fetching merge_id: should be null (cancelled by the MergeCanceller)");
        Util.sleep(timeout);
        merge_id=gms.getMergeId();
        assert merge_id == null : "MergeCanceller didn't kick in";
    }




}