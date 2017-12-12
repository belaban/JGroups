package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.FRAG3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.protocols.pbcast.ParticipantGmsImpl;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Digest;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests DeltaViews (https://issues.jboss.org/browse/JGRP-2159)
 * @author Bela Ban
 * @since  4.0.1
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class DeltaViewTest {
    protected JChannel                        j, k, l;
    protected static final String             CLUSTER=DeltaViewTest.class.getSimpleName();
    protected static final short              GMS_ID=ClassConfigurator.getProtocolId(GMS.class);



    @BeforeMethod protected void setup() throws Exception {
        j=create("J");
        k=create("K");
        l=create("L");
    }

    @AfterMethod protected void destroy() {Util.close(l, k, j);}


    public void testDeltaViews() throws Exception {
        DelayViewsAndJoinRsps del=new DelayViewsAndJoinRsps(j);

        j.connect(CLUSTER);
        j.getProtocolStack().insertProtocol(del, ProtocolStack.Position.BELOW, GMS.class);

        k.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(5000, 500, j,k);
        System.out.printf("\nJ: %s\nK: %s\n\n", j.getView(), k.getView());

        l.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, j, k, l);
        System.out.printf("\nJ: %s\nK: %s\nL: %s\n", j.getView(), k.getView(), l.getView());
    }



    protected JChannel create(String name) throws Exception {
        JChannel ch=new JChannel(Util.getTestStack()).name(name);
        ch.getProtocolStack().removeProtocol(STABLE.class, FRAG2.class, FRAG3.class);
        GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
        gms.setViewAckCollectionTimeout(1000);
        gms.setValue("join_timeout", 1500);
          //.setValue("install_view_locally_first", false); // setting this to true should fix the issue!
        return ch;
    }



    // up first view: queue
    // down JOIN-RSP: queue
    // up second view: queue
    // down JOIN-RSP:
    // ==> send first and second JOIN-RSP
    // ==> deliver first and second view (second should fail)
    // ==> remove this protocol
    protected class DelayViewsAndJoinRsps extends Protocol {
        protected final List<Message> views=new ArrayList<>(2); // views to be delivered
        protected final List<Message> join_rsps=new ArrayList<>(2); // JOIN-RSPS to be sent to K
        protected final JChannel      ch;
        protected boolean             removed;

        public DelayViewsAndJoinRsps(JChannel ch) {
            this.ch=ch;
        }

        public Object down(Message msg) {
            if(isJoinRsp(msg)) {
                checkDone(msg, join_rsps);
                return null;
            }
            return down_prot.down(msg);
        }

        public Object up(Message msg) {
            if(isView(msg)) {
                checkDone(msg, views);
                return null;
            }
            return up_prot.up(msg);
        }

        public void up(MessageBatch batch) {
            for(Message msg: batch) {
                if(isView(msg)) {
                    batch.remove(msg);
                    checkDone(msg, views);
                }
            }
            if(!batch.isEmpty())
                up_prot.up(batch);
        }


        protected synchronized void checkDone(Message msg, List<Message> list) {
            list.add(msg);
            if((join_rsps.size() >= 2 || views.size() >= 2) && !removed) {
                flushMessages();
                ch.getProtocolStack().removeProtocol(this);
                removed=true;
            }
        }

        protected void flushMessages() {
            System.out.printf("** flushing %d JOIN-RSPs and %d views:\n", join_rsps.size(), views.size());
            int count=1;
            for(Message msg: join_rsps) {
                try {
                    JoinRsp join_rsp=Util.streamableFromBuffer(JoinRsp::new, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    System.out.printf("join-rsp #%d to %s: %s\n", count++, msg.dest(), join_rsp.getView());
                }
                catch(Throwable t) {
                    log.error("failed unmarshalling JOIN-RSP", t);
                }
            }

            // deliver the views
            count=1;
            for(Message msg: views) {
                try {
                    Tuple<View,Digest> tuple=GMS._readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    System.out.printf("view #%d: %s\n", count++, tuple.getVal1());
                }
                catch(Throwable t) {
                    log.error("failed unmarshalling view", t);
                }
            }
            System.out.println("\n");

            // send the JOIN-RSPs: the first JOIN-RSP neeeds to be handled by a client, the second by a participant: if we
            // sent them right next to each other, then the second would not be installed if GMS.impl is still a client.
            // We therefore need to wait until impl is a participant. This is a kludge, but better then using sleep()
            Message join_rsp_msg=join_rsps.remove(0);
            down_prot.down(join_rsp_msg);

            join_rsp_msg=join_rsps.remove(0);
            JoinRsp join_rsp=null;
            try {
                join_rsp=Util.streamableFromBuffer(JoinRsp::new, join_rsp_msg.getRawBuffer(), join_rsp_msg.getOffset(), join_rsp_msg.getLength());
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
            installJoinRspInParticipant(k, join_rsp);
            join_rsps.clear();

            // deliver the views
            for(Message msg: views)
                up_prot.up(msg);
            views.clear();
        }

        // Waits until GMS.impl is ParticipantGmsImpl, then calls impl.handleJoinRsp()
        protected void installJoinRspInParticipant(JChannel ch, JoinRsp rsp) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            for(int i=0; i < 10; i++) {
                if(ParticipantGmsImpl.class.equals(gms.getImpl().getClass())) {
                    gms.getImpl().handleJoinResponse(rsp);
                    break;
                }
                Util.sleep(500);
            }
            if(!ParticipantGmsImpl.class.equals(gms.getImpl().getClass()))
                throw new IllegalStateException(String.format("GMS.impl is not participant: %s", gms.getImpl().getClass().getSimpleName()));
        }
    }

    protected static boolean isView(Message msg) {
        GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
        return hdr != null && hdr.getType() == GMS.GmsHeader.VIEW;
    };
    protected static boolean isJoinRsp(Message msg) {
        GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
        return hdr != null && hdr.getType() == GMS.GmsHeader.JOIN_RSP;
    };

}

