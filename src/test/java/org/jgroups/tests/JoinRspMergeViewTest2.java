package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests the case where we have 2 members A,B with a MergeView. Member C joins but misses the first JoinRsp. The second
 * JoinRsp should lead to a deserialization issue, see https://redhat.atlassian.net/browse/JGRP-3033
 * @author Bela Ban
 * @since  5.6.0
 */
@Test(groups=Global.FUNCTIONAL)
public class JoinRspMergeViewTest2 {
    protected JChannel     a,b,c;
    protected static short GMS_ID=ClassConfigurator.getProtocolId(GMS.class);

    @BeforeMethod
    protected void init() throws Exception {
        a=new JChannel(Util.getTestStack()).name("A").connect(JoinRspMergeViewTest2.class.getSimpleName());
        b=new JChannel(Util.getTestStack()).name("B").connect(JoinRspMergeViewTest2.class.getSimpleName());
        c=new JChannel(Util.getTestStack()).name("C");
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);
    }

    @AfterMethod
    protected void destroy() {
        Util.close(c,b,a);
    }

    /**
     * Cluster A and B. C joins, but discards its JoinRsps. A MergeView is installed in A and B. Now, C doesn't drop the
     * JoinRsps and gets a JoinRsp with a MergeView, which should result in an exception with the unpatched code.
     */
    public void testIncorrectDeserialization() throws Exception {
        DropJoinResponses drop=new DropJoinResponses().enable(true);
        GMS gms_c=c.stack().findProtocol(GMS.class);
        gms_c.setMaxJoinAttempts(100).setJoinTimeout(2000);
        c.stack().insertProtocol(drop, ProtocolStack.Position.BELOW, GMS.class);

        Runnable r=() -> {
            injectMergeView(); // injects MV={A,B,C} into A and B
            drop.enable(false);
        };
        new Thread(r).start();

        c.connect(JoinRspMergeViewTest2.class.getSimpleName()); // fails without patch for JGRP-3033
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);
        System.out.printf("\nviews:\n%s\n", Stream.of(a,b,c).map(ch -> String.format("%s  -> %s", ch.address(), ch.view()))
          .collect(Collectors.joining("\n")));


    }

    // Injects a MergeView {A,B,C} into A and B (*not* C!)
    protected void injectMergeView() {
        // Inject a MergeView
        View v1=View.create(a.address(), 5, a.address());
        View v2=View.create(b.address(), 6, b.address());
        View v3=View.create(c.address(), 3, c.address());
        MergeView mv=new MergeView(a.address(), 7, List.of(a.address(), b.address(), c.address()), List.of(v1,v2,v3));
        Stream.of(a,b).forEach(ch -> {
            GMS gms=ch.stack().findProtocol(GMS.class);
            gms.installView(mv);
        });
        System.out.printf("\nviews:\n%s\n", Stream.of(a,b).map(ch -> String.format("%s  -> %s", ch.address(), ch.view()))
          .collect(Collectors.joining("\n")));
    }

    // used by C, drops received JoinRsps until disabled
    protected static class DropJoinResponses extends Protocol {
        protected boolean enabled=true;

        protected DropJoinResponses enable(boolean f) {this.enabled=f; return this;}

        @Override
        public void up(MessageBatch batch) {
            if(enabled) {
                for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
                    Message msg=it.next();
                    GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
                    if(hdr != null) {
                        if(hdr.getType() == GMS.GmsHeader.JOIN_RSP) {
                            System.out.printf("-- dropped JOIN-RSP from %s -> %s: %s\n", msg.src(), msg.dest(), msg.printHeaders());
                            it.remove(); // drop JoinRsp
                        }
                    }
                }
            }
            up_prot.up(batch);
        }

        @Override
        public Object up(Message msg) {
            if(enabled) {
                GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
                if(hdr != null) {
                    if(hdr.getType() == GMS.GmsHeader.JOIN_RSP) {
                        System.out.printf("-- dropped JOIN-RSP from %s -> %s: %s\n", msg.src(), msg.dest(), msg.printHeaders());
                        return null; // drop the first JOIN-RSP from A -> C
                    }
                }
            }
            return up_prot.up(msg);
        }
    }
}
