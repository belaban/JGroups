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
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the GMS protocol for merging functionality
 * @author Bela Ban
 * @version $Id: GMS_MergeTest.java,v 1.3 2009/05/20 06:53:41 belaban Exp $
 */
@Test(groups={Global.STACK_INDEPENDENT}, sequential=true)
public class GMS_MergeTest extends ChannelTestBase {
    static final String props="SHARED_LOOPBACK:PING(timeout=50):pbcast.NAKACK:UNICAST:pbcast.STABLE:pbcast.GMS:FC:FRAG2";



    @BeforeClass
    void setUp() throws Exception {
    }


    @AfterClass
    void tearDown() throws Exception {
    }


    /**
     * Simulates the death of a merge leader after having sent a MERG_REQ. Because there is no MergeView or CANCEL_MERGE
     * message, the MergeCanceller has to null merge_id after a timeout
     */
    public static void testMergeRequestTimeout() throws Exception {
        JChannel c1=new JChannel(props);
        try {
            c1.connect("GMS_MergeTest");
            Message merge_request=new Message();
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ);
            MergeId new_merge_id=MergeId.create(c1.getAddress());
            hdr.setMergeId(new_merge_id);
            merge_request.putHeader(GMS.name, hdr);
            GMS gms=(GMS)c1.getProtocolStack().findProtocol(GMS.class);
            MergeId merge_id=gms.getMergeId();
            assert merge_id == null;
            System.out.println("starting merge");
            gms.up(new Event(Event.MSG, merge_request));
            merge_id=gms.getMergeId();
            System.out.println("merge_id = " + merge_id);
            assert new_merge_id.equals(merge_id);

            long timeout=(long)(gms.getMergeTimeout() * 1.5);
            System.out.println("sleeping for " + timeout + " ms, then fetching merge_id: should be null (cancelled by the MergeCanceller)");
            Util.sleep(timeout);
            merge_id=gms.getMergeId();
            System.out.println("merge_id = " + merge_id);
            assert merge_id == null : "MergeCanceller didn't kick in";
        }
        finally {
            Util.close(c1);
        }
    }


    public static void testSimpleMerge() throws Exception {
        JChannel[] channels=null;
        try {
            channels=create("A", "B", "C", "D");
            print(channels);
            View view=channels[channels.length -1].getView();
            assert view.size() == channels.length : "view is " + view;

            System.out.println("\ncreating partitions: ");
            String[][] partitions=generate(new String[]{"A", "B"}, new String[]{"C", "D"});
            createPartitions(channels, partitions);
            print(channels);
        }
        finally {
            close(channels);
        }
    }



    private static void close(JChannel[] channels) {
        for(int i=channels.length -1; i <= 0; i--) {
            JChannel ch=channels[i];
            Util.close(ch);
        }
    }


    private static JChannel[] create(String ... names) throws Exception {
        JChannel[] retval=new JChannel[names.length];
        for(int i=0; i < retval.length; i++) {
            JChannel ch=new JChannel(props);
            ch.setName(names[i]);
            retval[i]=ch;
            ch.connect("GMS_MergeTest");
        }
        return retval;
    }

    private static void createPartitions(JChannel[] channels, String[]... partitions) throws Exception {
        checkUniqueness(partitions);

    }

    private static String[][] generate(String[] ... partitions) {
        String[][] retval=new String[partitions.length][];
        System.arraycopy(partitions, 0, retval, 0, partitions.length);
        return retval;
    }

    private static void checkUniqueness(String[] ... partitions) throws Exception {
        Set<String> set=new HashSet<String>();
        for(String[] partition: partitions) {
            for(String tmp: partition) {
                if(!set.add(tmp))
                    throw new Exception("partitions are overlapping: element " + tmp + " is in multiple partitions");
            }
        }
    }

    private static void print(JChannel[] channels) {
        for(JChannel ch: channels) {
            System.out.println(ch.getName() + ": " + ch.getView());
        }
    }


}