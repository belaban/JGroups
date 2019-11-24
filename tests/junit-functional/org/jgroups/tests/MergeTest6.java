package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Tests merging with a dead merge leader https://issues.jboss.org/browse/JGRP-2276:
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MergeTest6 {
    protected JChannel           two, three, four, five, six, seven;
    protected static final short GMS_ID=ClassConfigurator.getProtocolId(GMS.class);

    @BeforeMethod
    void setUp() throws Exception {
        two=createChannel(2);
        three=createChannel(3);
        four=createChannel(4);
        five=createChannel(5);
        six=createChannel(6);
        seven=createChannel(7);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, two, three,four, five,six,seven);
    }

    @AfterMethod void tearDown() throws Exception {Util.close(seven,six,five,four,three,two);}


    /**
     * Tests whether the following scenario is successfully healed by {@link MERGE3}:
     * <pre>
     *     - (2 left)
     *     - 3: 3|6 -> {3,4,5,6,7}
     *     - 4: 3|6 -> {3,4,5,6,7}
     *     - 5: 3|6 -> {3,4,5,6,7}
     *     - 6: 3|6 -> {3,4,5,6,7}
     *     - 7: 2|5 -> {2,3,4,5,6,7}
     * </pre>
     * In this case, 7 and 3 should become merge coordinators and establish a MergeView
     */
    public void testViewInconsistency() throws Exception {
        System.out.println("Initial view:\n");
        for(JChannel ch: Arrays.asList(two,three,four,five,six,seven))
            System.out.printf("%s: %s\n", ch.getAddress(), ch.getView());

        ProtocolStack stack=seven.getProtocolStack();
        Protocol drop_view=new DropView().setAddress(seven.getAddress());

        stack.insertProtocol(drop_view, ProtocolStack.Position.BELOW, NAKACK2.class); // GMS.class);

        Util.close(two);

        for(int i=0; i < 10; i++) {
            View view_3=three.getView();
            boolean same=true;
            for(JChannel ch: Arrays.asList(four,five,six)) {
                if(!ch.getView().equals(view_3)) {
                    same=false;
                    break;
                }
            }
            if(same)
                break;
            Util.sleep(500);
        }

        View view_3=three.getView();
        assert Stream.of(four,five,six).allMatch(ch -> ch.getView().equals(view_3))
          : Stream.of(four,five,six).map(JChannel::getView).collect(Collectors.toList());
        assert !view_3.equals(seven.getView());

        System.out.println("Views after member 7 dropped view");
        Stream.of(three,four,five,six,seven).forEach(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()));

        stack.removeProtocol(drop_view);

        System.out.println("-- waiting for merge to heal partition");
        Util.waitUntilAllChannelsHaveSameView(1500000, 1000, three,four,five,six,seven);
        Stream.of(three,four,five,six,seven).forEach(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()));
    }



    protected static View createView(int id, JChannel... mbrs) {
        Address[] members=new Address[mbrs.length];
        for(int i=0; i < mbrs.length; i++)
            members[i]=mbrs[i].getAddress();
        return View.create(mbrs[0].getAddress(), id, members);
    }



    protected static JChannel createChannel(int num) throws Exception {
        JChannel ch=new JChannel(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new MERGE3().setMinInterval(3000).setMaxInterval(4000).setValue("check_interval", 7000),
          new FD_ALL().setValue("timeout", 8000).setValue("interval", 3000)
            .setValue("timeout_check_interval", 10000),

          new NAKACK2().setValue("use_mcast_xmit", false)
            .setValue("log_discard_msgs", false).setValue("log_not_found_msgs", false),
          new UNICAST3(),
          new STABLE().setValue("max_bytes", 50000),
          new GMS().setValue("print_local_addr", false)
            .setValue("join_timeout", 100)
            .setValue("leave_timeout", 100)
            .setValue("merge_timeout", 5000)
            .setValue("log_view_warnings", false)
            .setValue("view_ack_collection_timeout", 50)
            .setValue("log_collect_msgs", false))
          .name(String.valueOf(num));
        // the address generator makes sure that 2's UUID is lower than 3's UUID, so 2 is chosen as merge leader
        ch.addAddressGenerator(() -> new UUID(0, num));
        return ch.connect("MergeTest6");
    }



    protected static void injectView(View view, JChannel... channels) {
        for(JChannel ch: channels) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }

    protected static void checkInconsistencies(JChannel... channels) {
        for(JChannel ch: channels) {
            MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
            merge.checkInconsistencies();
        }
    }



    /** Drops a received VIEW message (needs to be placed below GMS) */
    protected static class DropView extends Protocol {
        protected Address local_addr;
        protected boolean first_view_received;

        protected DropView setAddress(Address addr) {this.local_addr=addr; return this;}

        public Object down(Event evt) {
            if(evt.type() == Event.SET_LOCAL_ADDRESS)
                local_addr=evt.arg();
            return down_prot.down(evt);
        }

        public Object up(Message msg) {
            GMS.GmsHeader hdr=msg.getHeader(GMS_ID); View view;
            if(hdr != null && hdr.getType() == GMS.GmsHeader.VIEW && (view=readView(msg)) != null) {
                if(!first_view_received) {
                    System.out.printf("%s: dropped view %s\n", local_addr, view);
                    first_view_received=true;
                    return null;
                }
            }
            return up_prot.up(msg);
        }

        public void up(MessageBatch batch) {
            for(Message msg: batch) {
                GMS.GmsHeader hdr=msg.getHeader(GMS_ID); View view;
                if(hdr != null && hdr.getType() == GMS.GmsHeader.VIEW) {
                    view=readView(msg);
                    if(!first_view_received) {
                        first_view_received=true;
                        batch.remove(msg);
                        System.out.printf("%s: dropped view %s (in message batch)\n", local_addr, view);
                    }
                }
            }
            if(!batch.isEmpty())
                up_prot.up(batch);
        }

        protected static View readView(Message msg) {
            try {
                Tuple<View,Digest> tuple=GMS._readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                return tuple != null? tuple.getVal1() : null;
            }
            catch(Exception e) {
                return null;
            }
        }
    };


}
