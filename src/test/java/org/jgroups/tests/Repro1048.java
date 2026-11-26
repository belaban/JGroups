package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.FRAG4;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UFC;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Single-JVM reproducer for JGroups issue 1048: a stale view reaches FlowControl after a newer one,
 * and evicts a member that is still in the cluster.
 *
 * WHAT IT SHOWS
 *
 * GMS.installView() guards against stale views correctly, but it sets `view` under the lock and
 * dispatches after releasing it (5.5.1 lines 691-699):
 *
 *     finally { lock.unlock(); }
 *     down_prot.down(view_event);
 *     up_prot.up(view_event);
 *
 * So installation order and dispatch order are separate. Two threads can be inside installView()
 * for different views, both passing the guard legitimately: the joining application thread inside
 * connect() (ClientGmsImpl.installViewIfValidJoinRsp -> GMS.installView) and a JGroups thread
 * handling the coordinator's next view. If anything below GMS is slow for the older view's
 * down_prot.down(), the newer view completes its whole dispatch first and the older one arrives at
 * FlowControl afterwards. FlowControl.up() has no view-id check, so handleViewChange() runs with
 * the older member list and retainAll() drops a live member. handleCreditRequest() then does
 * map.get(sender), finds null, and returns without replenishing: that peer's credit window drains
 * one way and never refills.
 *
 * The only thing simulated here is "something below GMS is slow", by SlowViewDown below. In
 * production that was FD_SOCK2/TCP setting up connections to a member that was itself still
 * starting; measured stalls there were 6.0s (four occurrences) and 8.03s.
 *
 * JIRA https://redhat.atlassian.net/browse/JGRP-3036
 */
@Test(groups=Global.FUNCTIONAL)
public class Repro1048 {
    static final String CLUSTER="repro1048";

    public void test1048() throws Exception {
        JChannel a=channel("A", false);
        JChannel b=channel("B", true);
        JChannel c=channel("C", false);
        try {
            a.connect(CLUSTER);
            say("A connected, view=%s", a.getView());

            // B joins on an APPLICATION thread, which is what connect() is in a real deployment.
            // Its first view's down-dispatch is then held, so connect() does not return yet.
            Thread joiner=new Thread(() -> {
                try {
                    b.connect(CLUSTER);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }, "app-joiner-B");
            joiner.start();

            // Let B install its first view and enter the stall, then let C join. The coordinator moves to a 3-member
            // view and B applies it on a JGroups thread, while B's own 2-member view is still undelivered.
            Util.sleep(2000);
            c.connect(CLUSTER);
            say("C connected, view=%s", c.getView());

            joiner.join();
            say("B connect() returned, view=%s", b.getView());
            Util.sleep(1000);

            System.out.printf("\n-------------- channel views:\n%s\n",
                              Stream.of(a,b,c).map(ch -> String.format("%s: %s", ch.address(), ch.view()))
                                .collect(Collectors.joining("\n")));

            System.out.printf("\n-------------- GMS views:\n%s\n\n",
                              Stream.of(a,b,c)
                                .map(ch -> ch.stack().findProtocol(GMS.class))
                                .map(gms -> String.format("%s: %s", ((GMS)gms).addr(), ((GMS)gms).view()))
                                .collect(Collectors.joining("\n")));

            // The coordinator's view is authoritative: every member in it is alive.
            View          cluster = a.getView();
            List<Address> members = cluster.getMembers();
            UFC           ufc     = b.getProtocolStack().findProtocol(UFC.class);
            String        senders = ufc.printSenderCredits();
            String        recv    = ufc.printReceiverCredits();

            List<Address> missing=new ArrayList<>();
            for(Address m: members)
                if(!inMap(senders, m) || !inMap(recv, m))
                    missing.add(m);

            say("cluster view (from coordinator A): %s", cluster);
            say("B's channel view:                 %s", b.getView());
            say("B's UFC credits:%n%s", ufc.printCredits());

            if(missing.isEmpty()) {
                say("NOT REPRODUCED: B's credit maps contain every member of %s", cluster.getViewId());
                return;
            }
            say("REPRODUCED: %s is in view %s but missing from B's UFC maps."
                + " handleCreditRequest() will drop its credit requests, and its window drains one way.",
                missing, cluster.getViewId());
            assert false : String.format("REPRODUCED: %s is in view %s but missing from B's UFC maps."
                                           + " handleCreditRequest() will drop its credit requests, and its window drains one way.",
                                         missing, cluster.getViewId());
        }
        finally {
            Util.close(c, b, a);
        }
    }

    /** Minimal TCP stack. SlowViewDown sits directly above the transport, where a slow protocol is. */
    static JChannel channel(String name, boolean slow) throws Exception {
        JChannel ch=new JChannel(Util.getTestStack(new UFC().setMaxCredits(2_000_000), new FRAG4())).name(name);
        if(slow) {
            SlowViewDown sv=new SlowViewDown().delay(4000);
            ch.stack().insertProtocol(sv, ProtocolStack.Position.ABOVE, TP.class);
        }
        return ch;
    }

    /** printMap() emits "<addr>: <credits>" per line. */
    static boolean inMap(String credit_map, Address mbr) {
        for(String line: credit_map.split("\n")) {
            int idx=line.lastIndexOf(':');
            if(idx > 0 && line.substring(0, idx).trim().equals(mbr.toString()))
                return true;
        }
        return false;
    }

    static void say(String fmt, Object... args) {
        //noinspection StringConcatenationInFormatCall
        System.out.printf("%tT.%<tL  " + fmt + "%n", prepend(System.currentTimeMillis(), args));
    }

    static Object[] prepend(Object first, Object[] rest) {
        Object[] all=new Object[rest.length + 1];
        all[0]=first;
        System.arraycopy(rest, 0, all, 1, rest.length);
        return all;
    }

    /**
     * Holds the down-dispatch of a view, and only when the dispatching thread is not a JGroups
     * stack thread. That restriction matters: delaying a stack thread blocks that sender's message
     * queue, so the newer view would never arrive and nothing would reproduce.
     */
    public static class SlowViewDown extends Protocol {
        protected long delay=8000;

        public SlowViewDown delay(long d) {delay=d; return this;}

        public Object down(Event evt) {
            if(evt.getType() == Event.VIEW_CHANGE) {
                View   v      = evt.getArg();
                String thread = Thread.currentThread().getName();
                boolean off_stack=!thread.startsWith("jgroups-") && !thread.startsWith("thread-");
                if(v.getViewId().getId() <= 1 && off_stack) {
                    say("   [SlowViewDown] holding down-dispatch of %s for %dms on thread %s",
                        v.getViewId(), delay, thread);
                    Util.sleep(delay);
                    say("   [SlowViewDown] releasing %s", v.getViewId());
                }
            }
            return down_prot.down(evt);
        }
    }
}
