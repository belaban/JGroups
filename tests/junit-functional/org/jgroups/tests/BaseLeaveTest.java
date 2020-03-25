package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.GmsImpl;
import org.jgroups.protocols.pbcast.ViewHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests graceful leaves of multiple members, especially coord and next-in-line.
 * Nodes are leaving gracefully so no merging is expected.<br/>
 * Reproducer for https://issues.jboss.org/browse/JGRP-2293.
 *
 * @author Radoslav Husar
 * @author Bela Ban
 */
@Test(groups={Global.FUNCTIONAL,Global.ENCRYPT},singleThreaded=true)
public abstract class BaseLeaveTest {
    protected final String      cluster_name=getClass().getSimpleName();
    protected static final int  NUM=10;
    protected JChannel[]        channels;

    protected void setup(int num) throws Exception {
        channels=new JChannel[num];
        for(int i = 0; i < channels.length; i++)
            channels[i] = create(String.valueOf(i + 1)).connect(cluster_name);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
        System.out.printf("-- initial view fo r cluster %s:\n%s\n\n",
                          cluster_name, Stream.of(channels).map(ch -> ch.getAddress() + ": " + ch.getView())
                            .collect(Collectors.joining("\n")));
    }

    protected void destroy() {
        Util.closeFast(channels);
        assert channels == null || Stream.of(channels).filter(Objects::nonNull).allMatch(JChannel::isClosed);
        System.out.println("\n\n================================================================\n\n");
    }

    protected abstract JChannel create(String name) throws Exception;

    public void testGracefulLeave() throws Exception {
        setup(NUM);
        for(int j=0; j < channels.length; j++)
            System.out.printf("%-4s: view is %s\n", channels[j].getAddress(), channels[j].getView());
        System.out.println("\n");

        JChannel[] remaining_channels=new JChannel[channels.length-2];
        System.arraycopy(channels, 2, remaining_channels, 0, channels.length-2);
        Stream.of(channels).map(c -> c.getProtocolStack().findProtocol(GMS.class)).forEach(p -> ((Protocol)p).setLevel("trace"));
        Stream.of(channels).limit(2).forEach(Util::close);
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, remaining_channels);
        for(int i=0; i < remaining_channels.length; i++)
            System.out.printf("%-4s: view is %s\n", remaining_channels[i].getAddress(), remaining_channels[i].getView());
    }



    /** A single member (coord) leaves */
    public void testLeaveOfSingletonCoord() throws Exception {
        setup(1);
        JChannel ch=channels[0];
        assert ch.getView().size() == 1;
        Util.close(ch);
        assert ch.getView() == null;
    }

    /** The coord leaves */
    public void testCoordLeave() throws Exception {
        setup(NUM);
        Util.close(channels[0]);
        List<JChannel> list=Stream.of(channels).filter(JChannel::isConnected).collect(Collectors.toList());
        Util.waitUntilAllChannelsHaveSameView(5000, 1000, list);
        assert Arrays.stream(channels, 0, channels.length).filter(JChannel::isConnected)
          .peek(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()))
          .allMatch(ch -> ch.getView().size() == channels.length-1 && ch.getView().getCoord().equals(channels[1].getAddress()));
    }

    /** A participant leaves */
    public void testParticipantLeave() throws Exception {
        setup(NUM);
        Util.close(channels[2]);
        List<JChannel> list=Stream.of(channels).filter(JChannel::isConnected).collect(Collectors.toList());
        Util.waitUntilAllChannelsHaveSameView(5000, 1000, list);
        assert Arrays.stream(channels, 0, channels.length).filter(JChannel::isConnected)
          .peek(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()))
          .allMatch(ch -> ch.getView().size() == channels.length-1 && ch.getView().getCoord().equals(channels[0].getAddress()));
    }

    /** The first N coords leave, one after the other */
    public void testSequentialLeavesOfCoordinators() throws Exception {
        setup(NUM);
        Arrays.stream(channels, 0, channels.length/2).forEach(Util::close);
        Util.waitUntilAllChannelsHaveSameView(5000, 1000,
                                              Arrays.stream(channels, channels.length/2, channels.length).collect(Collectors.toList()));
        Arrays.stream(channels, 0, channels.length).forEach(ch -> {
            if(ch.isConnected())
                System.out.printf("%s: %s\n", ch.getAddress(), ch.getView());
        });
        Address coord=channels[channels.length/2].getAddress();
        System.out.printf("-- new coord is %s\n", coord);
        assert Arrays.stream(channels, channels.length/2, channels.length)
          .allMatch(ch -> ch.getView().size() == channels.length/2 && ch.getView().getCoord().equals(coord));
    }

    /** The coord and next-coord leave concurrently (next leaves first) */
    public void testLeaveOfNextAndCoord() throws Exception {
        setup(NUM);
        testLeaveOfFirstNMembers(Comparator.comparingInt(GmsImpl.Request::getType), 2);
    }

    /** The coord and next N members concurrently (next leaves first) */
    public void testLeaveOfNext8AndCoord() throws Exception {
        setup(NUM);
        testLeaveOfFirstNMembers(Comparator.comparingInt(GmsImpl.Request::getType), 8);
    }

    /** The coord and next-coord leave concurrently (coord leaves first) */
    public void testLeaveOfCoordAndNext() throws Exception {
        setup(NUM);
        testLeaveOfFirstNMembers(Comparator.comparingInt(GmsImpl.Request::getType).reversed(), 2);
    }

    /** The coord and next-coord leave concurrently (coord leaves first), but these are the only members in the cluster */
    public void testLeaveOfCoordAndNextWithOnly2Members() throws Exception {
        setup(2);
        testLeaveOfFirstNMembers(Comparator.comparingInt(GmsImpl.Request::getType).reversed(), 2);
    }

    /** The coord and next N members concurrently (coord leaves first) */
    public void testLeaveOfCoordAndNext8() throws Exception {
        setup(NUM);
        testLeaveOfFirstNMembers(Comparator.comparingInt(GmsImpl.Request::getType).reversed(), 8);
    }

    /**
     * The second half of the cluster (6,7,8,9,10) sends LEAVE requests to 1, but 1 leaves before they get a response.
     * Requires them to resend their LEAVE requests to 2 on the view change when 2 takes over as coord.
     */
    public void testLeaveOfSecondHalfWithCoordLeaving() throws Exception {
        setup(NUM);
        Stream.of(channels).forEach(ch -> ch.getProtocolStack()
          .removeProtocols(FailureDetection.class, FD_SOCK.class));
        Comparator<GmsImpl.Request> comp=Comparator.comparingInt(GmsImpl.Request::getType).reversed();
        GMS gms=channels[0].getProtocolStack().findProtocol(GMS.class);
        ViewHandler vh=gms.getViewHandler();
        MyViewHandler my_vh=new MyViewHandler(gms, vh.reqProcessor(),
                                              GmsImpl.Request::canBeProcessedTogether, comp, 6).processing(true);
        setViewHandler(my_vh, gms);
        testConcurrentLeaves(0,5,6,7,8,9);
        my_vh.processing(false);
        setViewHandler(vh, gms);

        assert Stream.of(0,5,6,7,8,9).map(i -> channels[i]).allMatch(JChannel::isClosed);
        assert Stream.of(1,2,3,4).map(i -> channels[i]).allMatch(JChannel::isConnected);
        assert Stream.of(1,2,3,4).map(i -> channels[i]).allMatch(ch -> ch.getView().getCoord().equals(channels[1].getAddress()));
    }

    /** The first channels.length_LEAVERS leave concurrently */
    public void testConcurrentLeaves2() throws Exception {
        setup(NUM);
        testConcurrentLeaves(2);
    }

    /** The first channels.length_LEAVERS leave concurrently */
    public void testConcurrentLeaves8() throws Exception {
        setup(NUM);
        Stream.of(channels).forEach(ch -> ch.getProtocolStack().removeProtocols(FailureDetection.class, FD_SOCK.class));
        testConcurrentLeaves(8);
    }

    /** The first num_leavers leave concurrently */
    protected void testConcurrentLeaves(int num_leavers) throws Exception {
        JChannel[] remaining_channels=new JChannel[channels.length - num_leavers];
        System.arraycopy(channels, num_leavers, remaining_channels, 0, channels.length - num_leavers);
        Stream.of(channels).limit(num_leavers).forEach(ch -> new Thread(() -> Util.close(ch)).start());

        // confirm that all channels that were closed are actually in CLOSED state
        Util.waitUntil(30000, 1000,
                       () -> Stream.of(channels).limit(num_leavers).allMatch(ch -> ch.isClosed() && ch.getView() == null),
                       () -> "channels:\n" + Stream.of(channels).limit(num_leavers)
                         .map(ch -> ch.getAddress() + ": " + ch.getState() + " (view=" + ch.getView() + ")")
                         .collect(Collectors.joining("\n")));

        // confirm that all remaining channels (might be 0) are in CONNECTED state and have the same view
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, remaining_channels);
        Arrays.stream(channels, 0, channels.length).filter(JChannel::isConnected)
          .forEach(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()));
    }

    protected void testConcurrentLeaves(int ... leavers) throws Exception {
        IntStream.of(leavers).forEach(i -> new Thread(() -> Util.close(channels[i])).start());
        List<Integer>remaining=IntStream.range(0, channels.length).boxed().collect(Collectors.toList());
        List<Integer>left=IntStream.of(leavers).boxed().collect(Collectors.toList());
        List<JChannel> left_channels=left.stream().map(i -> channels[i]).collect(Collectors.toList());
        remaining.removeAll(left);

        // confirm that all channels that were closed are actually in CLOSED state
        Util.waitUntil(30000, 1000,
                       () -> left_channels.stream().allMatch(ch -> ch.isClosed() && ch.getView() == null),
                       () -> "channels:\n" + left_channels.stream()
                         .map(ch -> ch.getAddress() + ": " + ch.getState() + " (view=" + ch.getView() + ")")
                         .collect(Collectors.joining("\n")));

        List<JChannel> remaining_channels=remaining.stream().map(i -> channels[i]).collect(Collectors.toList());
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, remaining_channels);
        Arrays.stream(channels, 0, channels.length).filter(JChannel::isConnected)
          .forEach(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()));
    }


    /** Sorts and delivers requests LEAVE and COORD_LEAVE according to parameter 'comp' */
    protected void testLeaveOfFirstNMembers(Comparator<GmsImpl.Request> comp, int leavers) throws Exception {
        GMS gms=channels[0].getProtocolStack().findProtocol(GMS.class);
        ViewHandler vh=gms.getViewHandler();
        MyViewHandler my_vh=new MyViewHandler(gms, vh.reqProcessor(),
                                              GmsImpl.Request::canBeProcessedTogether, comp, leavers).processing(true);
        setViewHandler(my_vh, gms);
        testConcurrentLeaves(leavers);
        my_vh.processing(false);
        setViewHandler(vh, gms);

        assert Arrays.stream(channels, 0, leavers).allMatch(ch -> ch.getView() == null)
          : "views are:\n" + Arrays.stream(channels, 0, leavers)
          .map(ch -> ch.getAddress() + ": " + ch.getView()).collect(Collectors.joining("\n"));
        assert leavers >= channels.length || Arrays.stream(channels, leavers, channels.length - 1)
          .allMatch(ch -> ch.getView().size() == channels.length - leavers && ch.getView().getCoord().equals(channels[leavers].getAddress()));
    }

    protected static void setViewHandler(ViewHandler<GmsImpl.Request> vh, GMS gms) {
        Field vh_field=Util.getField(GMS.class, "view_handler");
        Util.setField(vh_field, gms, vh);
    }



    protected static class MyViewHandler extends ViewHandler<GmsImpl.Request> {
        protected final Comparator<GmsImpl.Request> comparator;
        protected final int                         max_reqs;

        public MyViewHandler(GMS gms, Consumer<Collection<GmsImpl.Request>> req_processor,
                             BiPredicate<GmsImpl.Request,GmsImpl.Request> req_matcher,
                             Comparator<GmsImpl.Request> comparator, int max_reqs) {
            super(gms, req_processor, req_matcher);
            this.comparator=comparator;
            this.max_reqs=max_reqs;
        }

        @Override protected boolean _add(GmsImpl.Request req) {
            super._add(req);
            return checkQueue();
        }

        @Override protected boolean _add(GmsImpl.Request... reqs) {
            super._add(reqs);
            return checkQueue();
        }

        @Override protected boolean _add(Collection<GmsImpl.Request> reqs) {
            super._add(reqs);
            return checkQueue();
        }

        protected boolean checkQueue() {
            if(requests.size() >= max_reqs) {
                List<GmsImpl.Request> l=new ArrayList<>(requests);
                l.sort(this.comparator);
                System.out.printf("-- sorted requests from %s to: %s\n", requests, l);
                requests.clear();
                requests.addAll(l);
                process(requests);
            }
            return false;
        }
    }
}
