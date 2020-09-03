package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * Protocol to discover subgroups; e.g., existing due to a network partition (that healed). Example: group
 * {p,q,r,s,t,u,v,w} is split into 3 subgroups {p,q}, {r,s,t,u} and {v,w}. This protocol will eventually send
 * a MERGE event with the views of each subgroup up the stack: {p,r,v}. <p/>
 * Works as follows (https://issues.jboss.org/browse/JGRP-1387): every member periodically broadcasts its address (UUID),
 * logical name, physical address and ViewID information. Other members collect this information and see if the ViewIds
 * are different (indication of different subpartitions). If they are, the member with the lowest address (first in the
 * sorted list of collected addresses) sends a MERGE event up the stack, which will be handled by GMS.
 * The others do nothing.<p/>
 * The advantage compared to MERGE2 is that there are no merge collisions caused by multiple merges going on.
 * Also, the INFO traffic is spread out over max_interval, and every member sends its physical address with INFO, so
 * we don't need to fetch the physical address first.
 *
 * @author Bela Ban, Nov 2011
 * @since 3.1
 */
@MBean(description="Protocol to discover subgroups existing due to a network partition")
public class MERGE3 extends Protocol {
    

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    @Property(description="Minimum time in ms before sending an info message",type=AttributeType.TIME)
    protected long                          min_interval=1000;

    @Property(description="Interval (in milliseconds) when the next info " +
            "message will be sent. A random value is picked from range [1..max_interval]",type=AttributeType.TIME)
    protected long                          max_interval=10000;

    @Property(description="The max number of merge participants to be involved in a merge. 0 sets this to unlimited.")
    protected int                           max_participants_in_merge=100;

    /* ---------------------------------------------- JMX -------------------------------------------------------- */
    @Property(description="Interval (in ms) after which we check for view inconsistencies",type=AttributeType.TIME)
    protected long                          check_interval;

    @ManagedAttribute(description="Number of cached ViewIds")
    public int getViews() {return views.size();}

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected Address                       local_addr;

    protected volatile View                 view;

    protected TimeScheduler                 timer;

    protected final InfoSender              info_sender=new InfoSender();

    protected Future<?>                     info_sender_future;

    protected Future<?>                     view_consistency_checker;

    // hashmap to keep track of view-id sent in INFO messages. Keys=senders, values = ViewId sent
    protected final Map<Address,ViewId>     views=new HashMap<>();

    protected final ResponseCollector<View> view_rsps=new ResponseCollector<>();

    protected boolean                       transport_supports_multicasting=true;

    protected String                        cluster_name;

    protected final Consumer<PingData>      discovery_rsp_cb=this::sendInfoMessage;

    protected final Event                   ASYNC_DISCOVERY_EVENT=new Event(Event.FIND_MBRS_ASYNC, discovery_rsp_cb);


    @ManagedAttribute(description="Whether or not the current member is the coordinator")
    protected volatile boolean              is_coord;
    
    @ManagedAttribute(description="Number of times a MERGE event was sent up the stack")
    protected int                           num_merge_events;



    public int getNumMergeEvents() {return num_merge_events;}

    @ManagedAttribute(description="Is the view consistency checker task running")
    public synchronized boolean isViewConsistencyCheckerRunning() {
        return view_consistency_checker != null && !view_consistency_checker.isDone();
    }

    @ManagedAttribute(description="Is the view consistency checker task running")
    public boolean isMergeTaskRunning() {return isViewConsistencyCheckerRunning();}

    @ManagedAttribute(description="Is the info sender task running")
    public synchronized boolean isInfoSenderRunning() {
        return info_sender_future != null && !info_sender_future.isDone();
    }

    @ManagedOperation(description="Lists the contents of the cached views")
    public String dumpViews() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<ViewId,Set<Address>> entry: convertViews().entrySet())
            sb.append(entry.getKey()).append(": [")
              .append(Util.printListWithDelimiter(entry.getValue(), ", ", Util.MAX_LIST_PRINT_SIZE)).append("]\n");
        return sb.toString();
    }

    @ManagedOperation(description="Clears the views cache")
    public void clearViews() {synchronized(views) {views.clear();}}


    @ManagedOperation(description="Send INFO")
    public void sendInfo() {
        new InfoSender().run();
    }

    @ManagedOperation(description="Check views for inconsistencies")
    public void checkInconsistencies() {new ViewConsistencyChecker().run();}


    public void init() throws Exception {
        if(min_interval >= max_interval)
            throw new IllegalArgumentException("min_interval (" + min_interval + ") has to be < max_interval (" + max_interval + ")");
        if(check_interval == 0)
            check_interval=computeCheckInterval();
        else {
            if(check_interval <= max_interval) {
                log.warn("set check_interval=%d as it is <= max_interval", computeCheckInterval());
                check_interval=computeCheckInterval();
            }
        }
        if(max_interval <= 0)
            throw new Exception("max_interval must be > 0");
        transport_supports_multicasting=getTransport().supportsMulticasting();
    }

    public void start() throws Exception {
        super.start();
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved");
    }

    public void stop() {
        super.stop();
        is_coord=false;
        stopViewConsistencyChecker();
        stopInfoSender();
    }

    public long getMinInterval() {
        return min_interval;
    }

    public MERGE3 setMinInterval(long i) {
        if(min_interval < 0 || min_interval >= max_interval)
            throw new IllegalArgumentException("min_interval (" + min_interval + ") has to be < max_interval (" + max_interval + ")");
        min_interval=i;
        return this;
    }

    public long getMaxInterval() {
        return max_interval;
    }

    public MERGE3 setMaxInterval(long val) {
        if(val <= 0)
            throw new IllegalArgumentException("max_interval must be > 0");
        max_interval=val;
        check_interval=computeCheckInterval();
        return this;
    }

    public long    getCheckInterval()               {return check_interval;}
    public MERGE3  setCheckInterval(long ci)        {this.check_interval=ci; return this;}
    public int     getMaxParticipantsInMerge()      {return max_participants_in_merge;}
    public MERGE3  setMaxParticipantsInMerge(int m) {this.max_participants_in_merge=m; return this;}

    public boolean isCoord() {return is_coord;}


    protected long computeCheckInterval() {
        return (long)(max_interval * 1.6);
    }

    protected boolean isMergeRunning() {
        Object retval=up_prot.up(new Event(Event.IS_MERGE_IN_PROGRESS));
        return retval instanceof Boolean && (Boolean)retval;
    }

    protected synchronized void startInfoSender() {
        if(info_sender_future == null || info_sender_future.isDone())
            info_sender_future=timer.scheduleWithDynamicInterval(info_sender, getTransport() instanceof TCP);
    }

    protected synchronized void stopInfoSender() {
        if(info_sender_future != null) {
            info_sender_future.cancel(true);
            // info_sender.stop();
            info_sender_future=null;
        }
    }

    protected synchronized void startViewConsistencyChecker() {
        if(view_consistency_checker == null || view_consistency_checker.isDone())
            view_consistency_checker=timer.scheduleWithDynamicInterval(new ViewConsistencyChecker());
    }

    protected synchronized void stopViewConsistencyChecker() {
        if(view_consistency_checker != null) {
            view_consistency_checker.cancel(true);
            view_consistency_checker=null;
        }
    }


    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster_name=evt.getArg();
                break;

            case Event.DISCONNECT:
            case Event.TMP_VIEW:
                stopViewConsistencyChecker();
                stopInfoSender();
                break;

            case Event.VIEW_CHANGE:
                stopViewConsistencyChecker(); // should already be stopped
                stopInfoSender();             // should already be stopped
                Object ret=down_prot.down(evt);
                view=evt.getArg();
                clearViews();

                if(ergonomics && max_participants_in_merge > 0)
                    max_participants_in_merge=Math.max(100, view.size() / 3);

                startInfoSender();
                startViewConsistencyChecker();

                Address coord=view.getCoord();
                if(Objects.equals(coord, local_addr))
                    is_coord=true;
                else {
                    is_coord=false;
                    clearViews();
                }
                return ret;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    public Object up(Message msg) {
        MergeHeader hdr=msg.getHeader(getId());
        if(hdr == null)
            return up_prot.up(msg);
        Address sender=msg.getSrc();
        switch(hdr.type) {
            case INFO:
                addInfo(sender, hdr.view_id, hdr.logical_name, hdr.physical_addr);
                break;
            case VIEW_REQ:
                View viewToSend=view;
                Message view_rsp=new BytesMessage(sender).setFlag(Message.Flag.INTERNAL)
                  .putHeader(getId(), MergeHeader.createViewResponse()).setArray(marshal(viewToSend));
                log.trace("%s: sending view rsp: %s", local_addr, viewToSend);
                down_prot.down(view_rsp);
                break;
            case VIEW_RSP:
                View tmp_view=readView(msg.getArray(), msg.getOffset(), msg.getLength());
                log.trace("%s: received view rsp from %s: %s", local_addr, msg.getSrc(), tmp_view);
                if(tmp_view != null)
                    view_rsps.add(sender, tmp_view);
                break;
            default:
                log.error("Type %s not known", hdr.type);
        }
        return null;
    }


    public static List<View> detectDifferentViews(Map<Address,View> map) {
        final List<View> ret=new ArrayList<>();
        for(View view: map.values()) {
            if(view == null)
                continue;
            ViewId vid=view.getViewId();
            if(!Util.containsViewId(ret, vid))
                ret.add(view);
        }
        return ret;
    }

    public static ByteArray marshal(View view) {
        try {
            return Util.streamableToBuffer(view);
        }
        catch(Exception e) {
            return null;
        }
    }

    protected View readView(byte[] buffer, int offset, int length) {
        try {
            return buffer != null? Util.streamableFromBuffer(View::new, buffer, offset, length) : null;
        }
        catch(Exception ex) {
            log.error("%s: failed reading View from message: %s", local_addr, ex);
            return null;
        }
    }

    protected MergeHeader createInfo() {
        PhysicalAddress physical_addr=local_addr != null?
          (PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr)) : null;
        return MergeHeader.createInfo(view.getViewId(), NameCache.get(local_addr), physical_addr);
    }

    /** Adds received INFO to views hashmap */
    protected void addInfo(Address sender, ViewId view_id, String logical_name, PhysicalAddress physical_addr) {
        if(logical_name != null && sender instanceof UUID)
            NameCache.add(sender, logical_name);
        if(physical_addr != null)
            down(new Event(Event.ADD_PHYSICAL_ADDRESS, new Tuple<>(sender, physical_addr)));
        synchronized(views) {
            ViewId existing=views.get(sender);
            if(existing == null || existing.compareTo(view_id) < 0)
                views.put(sender, view_id);
        }
    }

    protected Map<ViewId,Set<Address>> convertViews() {
        Map<ViewId,Set<Address>> retval=new HashMap<>();
        synchronized(views) {
            for(Map.Entry<Address,ViewId> entry : views.entrySet()) {
                Address key=entry.getKey();
                ViewId view_id=entry.getValue();
                Set<Address> existing=retval.get(view_id);
                if(existing == null)
                    retval.put(view_id, existing=new ConcurrentSkipListSet<>());
                existing.add(key);
            }
        }
        return retval;
    }

    protected boolean differentViewIds() {
        ViewId first=null;
        synchronized(views) {
            for(ViewId view_id : views.values()) {
                if(first == null)
                    first=view_id;
                else if(!first.equals(view_id))
                    return true;
            }
        }
        return false;
    }

    protected void sendInfoMessage(PingData data) {
        if(data == null)
            return;
        Address target=data.getAddress();
        if(local_addr.equals(target))
            return;
        Address dest=data.getPhysicalAddr();
        if(dest == null) {
            log.warn("%s: physical address for %s not found; dropping INFO message to %s",
                     local_addr, target, target);
            return;
        }
        MergeHeader hdr=createInfo();
        Message info=new EmptyMessage(dest).setFlag(Message.Flag.INTERNAL).putHeader(getId(), hdr);
        down_prot.down(info);
    }

    protected class InfoSender implements TimeScheduler.Task {
        public void run() {
            if(view == null) {
                log.warn("%s: view is null, cannot send INFO message", local_addr);
                return;
            }

            MergeHeader hdr=createInfo();
            if(transport_supports_multicasting) {
                Message msg=new EmptyMessage().setFlag(Message.Flag.INTERNAL).putHeader(getId(), hdr)
                  .setFlag(Message.TransientFlag.DONT_LOOPBACK);
                down_prot.down(msg);
            }
            else
                down_prot.down(ASYNC_DISCOVERY_EVENT);
        }

        public long nextInterval() {
            return Math.max(min_interval, Util.random(max_interval) + max_interval/2);
        }

        public String toString() {
            return MERGE3.class.getSimpleName() + ": " + getClass().getSimpleName();
        }
    }


    protected class ViewConsistencyChecker implements TimeScheduler.Task {

        public void run() {
            try {
                MergeHeader hdr=createInfo();
                addInfo(local_addr, hdr.view_id, hdr.logical_name, hdr.physical_addr);
                if(!differentViewIds()) {
                    log.trace("%s: found no inconsistent views: %s", local_addr, dumpViews());
                    return;
                }
                _run();
            }
            finally {
                clearViews();
            }
        }

        protected void _run() {
            SortedSet<Address>       coords=new TreeSet<>();
            Map<ViewId,Set<Address>> converted_views=convertViews();

            converted_views.keySet().stream().map(ViewId::getCreator).forEach(coords::add);

            // add merge participants
            coords.addAll(converted_views.values().stream().filter(set -> !set.isEmpty())
                            .map(set -> set.iterator().next()).collect(Collectors.toList()));

            if(coords.size() <= 1) {
                log.trace("%s: cancelling merge as we only have 1 coordinator: %s", local_addr, coords);
                return;
            }
            log.trace("%s: merge participants are %s", local_addr, coords);

            if(max_participants_in_merge > 0 && coords.size() > max_participants_in_merge) {
                int old_size=coords.size();
                coords.removeIf(next -> coords.size() > max_participants_in_merge);
                log.trace("%s: reduced %d coords to %d", local_addr, old_size, max_participants_in_merge);
            }

            // grab views from all members in coords
            view_rsps.reset(coords);
            for(Address target: coords) {
                if(target.equals(local_addr)) {
                    if(view != null)
                        view_rsps.add(local_addr, view);
                    continue;
                }
                Message view_req=new EmptyMessage(target).setFlag(Message.Flag.INTERNAL)
                  .putHeader(getId(), MergeHeader.createViewRequest());
                down_prot.down(view_req);
            }
            view_rsps.waitForAllResponses(check_interval / 10);
            Map<Address,View> results=view_rsps.getResults();
            log.trace("%s: got all results: %s", local_addr, results);
            Map<Address,View> merge_views=new HashMap<>();
            results.entrySet().stream().filter(entry -> entry.getValue() != null).forEach(entry -> merge_views.put(entry.getKey(), entry.getValue()));
            view_rsps.reset();

            if(merge_views.size() >= 2) {
                Collection<View> tmp_views=merge_views.values();
                if(Util.allEqual(tmp_views)) {
                    log.trace("%s: all views are the same, suppressing sending MERGE up. Views: %s", local_addr, tmp_views);
                    return;
                }

                up_prot.up(new Event(Event.MERGE, merge_views));
                num_merge_events++;
            }
            else
                log.trace("%s: %d merged views. Nothing to do", local_addr, merge_views.size());
        }

        public long nextInterval() {
            return check_interval;
        }

        public String toString() {
            return String.format("%s: %s (interval=%dms", MERGE3.class.getSimpleName(), getClass().getSimpleName(), check_interval);
        }

    }


    public static class MergeHeader extends Header {
        protected Type                        type=Type.INFO;
        protected ViewId                      view_id;
        protected String                      logical_name;
        protected PhysicalAddress             physical_addr;


        public MergeHeader() {
        }

        public static MergeHeader createInfo(ViewId view_id, String logical_name, PhysicalAddress physical_addr) {
            return new MergeHeader(Type.INFO, view_id, logical_name, physical_addr);
        }

        public static MergeHeader createViewRequest() {
            return new MergeHeader(Type.VIEW_REQ, null, null, null);
        }

        public static MergeHeader createViewResponse() {
            return new MergeHeader(Type.VIEW_RSP, null, null, null);
        }

        protected MergeHeader(Type type, ViewId view_id, String logical_name, PhysicalAddress physical_addr) {
            this.type=type;
            this.view_id=view_id;
            this.logical_name=logical_name;
            this.physical_addr=physical_addr;
        }
        public short getMagicId() {return 75;}
        public Supplier<? extends Header> create() {return MergeHeader::new;}

        @Override
        public int serializedSize() {
            int retval=Global.BYTE_SIZE; // for the type
            retval+=Util.size(view_id);
            retval+=Global.BYTE_SIZE;     // presence byte for logical_name
            if(logical_name != null)
                retval+=logical_name.length() +2;
            retval+=Util.size(physical_addr);
            return retval;
        }

        @Override
        public void writeTo(DataOutput outstream) throws IOException {
            outstream.writeByte(type.ordinal()); // a byte if ok as we only have 3 types anyway
            Util.writeViewId(view_id,outstream);
            Bits.writeString(logical_name,outstream);
            Util.writeAddress(physical_addr, outstream);
        }

        @Override
        public void readFrom(DataInput instream) throws IOException, ClassNotFoundException {
            type=Type.values()[instream.readByte()];
            view_id=Util.readViewId(instream);
            logical_name=Bits.readString(instream);
            physical_addr=(PhysicalAddress)Util.readAddress(instream);
        }

        public String toString() {
            return String.format("%s: %s, logical_name=%s, physical_addr=%s",
                                 type, view_id != null? "view_id=" + view_id : "", logical_name, physical_addr);
        }

        protected enum Type {INFO, VIEW_REQ, VIEW_RSP}
    }
}
