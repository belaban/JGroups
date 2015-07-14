package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;


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
    @Property(description="Minimum time in ms before sending an info message")
    protected long min_interval=1000;

    @Property(description="Interval (in milliseconds) when the next info " +
            "message will be sent. A random value is picked from range [1..max_interval]")
    protected long max_interval=10000;

    @Property(description="The max number of merge participants to be involved in a merge. 0 sets this to unlimited.")
    protected int  max_participants_in_merge=100;

    /* ---------------------------------------------- JMX -------------------------------------------------------- */
    @Property(description="Interval (in ms) after which we check for view inconsistencies")
    protected long check_interval=0;

    @ManagedAttribute(description="Number of cached ViewIds")
    public int getViews() {return views.size();}

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected Address        local_addr=null;

    protected volatile View  view;

    protected TimeScheduler  timer;

    protected Future<?>      info_sender;

    protected Future<?>      view_consistency_checker;

    // hashmap to keep track of view-id sent in INFO messages. Keys=senders, values = ViewId sent
    protected final Map<Address,ViewId> views=new HashMap<>();

    protected final ResponseCollector<View> view_rsps=new ResponseCollector<>();

    protected boolean        transport_supports_multicasting=true;

    protected String         cluster_name;



    @ManagedAttribute(description="Whether or not the current member is the coordinator")
    protected volatile boolean is_coord=false;
    
    @ManagedAttribute(description="Number of times a MERGE event was sent up the stack")
    protected int           num_merge_events=0;

    @ManagedAttribute(description="Is the view consistency checker task running")
    public synchronized boolean isViewConsistencyCheckerRunning() {
        return view_consistency_checker != null && !view_consistency_checker.isDone();
    }

    @ManagedAttribute(description="Is the view consistency checker task running")
    public boolean isMergeTaskRunning() {return isViewConsistencyCheckerRunning();}

    @ManagedAttribute(description="Is the info sender task running")
    public synchronized boolean isInfoSenderRunning() {
        return info_sender != null && !info_sender.isDone();
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
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved");
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

    public void stop() {
        super.stop();
        is_coord=false;
        stopViewConsistencyChecker();
        stopInfoSender();
    }

    public long getMinInterval() {
        return min_interval;
    }

    public void setMinInterval(long i) {
        if(min_interval < 0 || min_interval >= max_interval)
            throw new IllegalArgumentException("min_interval (" + min_interval + ") has to be < max_interval (" + max_interval + ")");
        min_interval=i;
    }

    public long getMaxInterval() {
        return max_interval;
    }

    public void setMaxInterval(long val) {
        if(val <= 0)
            throw new IllegalArgumentException("max_interval must be > 0");
        max_interval=val;
        check_interval=computeCheckInterval();
    }


    protected long computeCheckInterval() {
        return (long)(max_interval * 1.6);
    }

    protected boolean isMergeRunning() {
        Object retval=up_prot.up(new Event(Event.IS_MERGE_IN_PROGRESS));
        return retval instanceof Boolean && (Boolean)retval;
    }

    protected synchronized void startInfoSender() {
        if(info_sender == null || info_sender.isDone())
            info_sender=timer.scheduleWithDynamicInterval(new InfoSender());
    }

    protected synchronized void stopInfoSender() {
        if(info_sender != null) {
            info_sender.cancel(true);
            info_sender=null;
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
                cluster_name=(String)evt.getArg();
                break;

            case Event.DISCONNECT:
                stopViewConsistencyChecker();
                stopInfoSender();
                break;

            case Event.TMP_VIEW:
                stopViewConsistencyChecker();
                stopInfoSender();
                break;
        
            case Event.VIEW_CHANGE:
                stopViewConsistencyChecker(); // should already be stopped
                stopInfoSender();             // should already be stopped
                Object ret=down_prot.down(evt);
                view=(View)evt.getArg();
                clearViews();

                if(ergonomics && max_participants_in_merge > 0)
                    max_participants_in_merge=Math.max(100, view.size() / 3);

                startInfoSender();

                List<Address> mbrs=view.getMembers();
                Address coord=mbrs.isEmpty()? null : mbrs.get(0);
                if(coord != null && coord.equals(local_addr)) {
                    is_coord=true;
                    startViewConsistencyChecker(); // start task if we became coordinator (doesn't start if already running)
                }
                else {
                    // if we were coordinator, but are no longer, stop task. this happens e.g. when we merge and someone
                    // else becomes the new coordinator of the merged group
                    is_coord=false;
                    clearViews();
                }
                return ret;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                MergeHeader hdr=(MergeHeader)msg.getHeader(getId());
                if(hdr == null)
                    break;
                Address sender=msg.getSrc();
                switch(hdr.type) {
                    case INFO:
                        addInfo(sender, hdr.view_id, hdr.logical_name, hdr.physical_addr);
                        break;
                    case VIEW_REQ:
                        Message view_rsp=new Message(sender).setFlag(Message.Flag.INTERNAL)
                          .putHeader(getId(), MergeHeader.createViewResponse()).setBuffer(marshal(view));
                        down_prot.down(new Event(Event.MSG, view_rsp));
                        break;
                    case VIEW_RSP:
                        View tmp_view=readView(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(tmp_view != null)
                            view_rsps.add(sender, tmp_view);
                        break;
                    default:
                        log.error("Type %s not known", hdr.type);
                }
                return null;
        }
        return up_prot.up(evt);
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

    public static Buffer marshal(View view) {
        return Util.streamableToBuffer(view);
    }

    protected View readView(byte[] buffer, int offset, int length) {
        try {
            return buffer != null? Util.streamableFromBuffer(View.class, buffer, offset, length) : null;
        }
        catch(Exception ex) {
            log.error("%s: failed reading View from message: %s", local_addr, ex);
            return null;
        }
    }

    protected MergeHeader createInfo() {
        PhysicalAddress physical_addr=local_addr != null?
          (PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr)) : null;
        return MergeHeader.createInfo(view.getViewId(), UUID.get(local_addr), physical_addr);
    }

    /** Adds received INFO to views hashmap */
    protected void addInfo(Address sender, ViewId view_id, String logical_name, PhysicalAddress physical_addr) {
        if(logical_name != null && sender instanceof UUID)
            UUID.add(sender, logical_name);
        if(physical_addr != null)
            down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<>(sender, physical_addr)));
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

    protected class InfoSender implements TimeScheduler.Task {
        protected final long discovery_timeout=(max_interval + min_interval) /2;

        public void run() {
            if(view == null) {
                log.warn("view is null, cannot send INFO message");
                return;
            }

            MergeHeader hdr=createInfo();
            // not needed; this is done below in ViewConsistencyChecker
            // addInfo(local_addr, hdr.view_id, hdr.logical_name, hdr.physical_addr);
            if(transport_supports_multicasting) { // mcast the discovery request to all but self
                Message msg=new Message().setFlag(Message.Flag.INTERNAL).putHeader(getId(), hdr)
                  .setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
                down_prot.down(new Event(Event.MSG, msg));
                return;
            }

            Responses rsps=(Responses)down_prot.down(Event.FIND_MBRS_EVT);
            rsps.waitFor(discovery_timeout); // return immediately if done
            rsps.done();
            if(rsps.isEmpty())
                return;

            log.trace("discovery protocol returned %d responses: %s", rsps.size(), rsps);
            for(PingData rsp: rsps) {
                Address target=rsp.getAddress();
                if(local_addr.equals(target))
                    continue; // skip discovery request to self
                Address dest=rsp.getPhysicalAddr();
                if(dest == null) continue;
                Message info=new Message(dest).setFlag(Message.Flag.INTERNAL).putHeader(getId(), hdr);
                down_prot.down(new Event(Event.MSG, info));
            }
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
            SortedSet<Address> coords=new TreeSet<>();

            // Only add view creators which *are* actually in the set as well, e.g.
            // A|4: {A,B,C} and
            // B|4: {D} would only add A to the coords list. A is a real coordinator
            Map<ViewId,Set<Address>> converted_views=convertViews();
            for(Map.Entry<ViewId,Set<Address>> entry: converted_views.entrySet()) {
                Address coord=entry.getKey().getCreator();
                Set<Address> members=entry.getValue();
                if(members != null && members.contains(coord))
                    coords.add(coord);
            }

            Address merge_leader=coords.isEmpty() ? null : coords.first();
            if(merge_leader == null || local_addr == null || !merge_leader.equals(local_addr)) {
                log.trace("I (%s) won't be the merge leader", local_addr);
                return;
            }

            log.debug("I (%s) will be the merge leader", local_addr);

            // add merge participants
            for(Set<Address> set: converted_views.values()) {
                if(!set.isEmpty())
                    coords.add(set.iterator().next());
            }

            if(coords.size() <= 1) {
                log.trace("cancelling merge as we only have 1 coordinator: %s", coords);
                return;
            }
            log.trace("merge participants are %s", coords);

            if(max_participants_in_merge > 0 && coords.size() > max_participants_in_merge) {
                int old_size=coords.size();
                for(Iterator<Address> it=coords.iterator(); it.hasNext();) {
                    Address next=it.next();
                    if(next.equals(merge_leader))
                        continue;
                    if(coords.size() > max_participants_in_merge)
                        it.remove();
                }
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
                Message view_req=new Message(target).setFlag(Message.Flag.INTERNAL)
                  .putHeader(getId(), MergeHeader.createViewRequest());
                down_prot.down(new Event(Event.MSG, view_req));
            }
            view_rsps.waitForAllResponses(check_interval / 10);
            Map<Address,View> results=view_rsps.getResults();
            Map<Address,View> merge_views=new HashMap<>();

            for(Map.Entry<Address,View> entry: results.entrySet())
                if(entry.getValue() != null)
                    merge_views.put(entry.getKey(), entry.getValue());
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
        }

        public long nextInterval() {
            return check_interval;
        }

        public String toString() {
            return MERGE3.class.getSimpleName() + ": " + getClass().getSimpleName();
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

        public int size() {
            int retval=Global.BYTE_SIZE; // for the type
            retval+=Util.size(view_id);
            retval+=Global.BYTE_SIZE;     // presence byte for logical_name
            if(logical_name != null)
                retval+=logical_name.length() +2;
            retval+=Util.size(physical_addr);
            return retval;
        }

        public void writeTo(DataOutput outstream) throws Exception {
            outstream.writeByte(type.ordinal()); // a byte if ok as we only have 3 types anyway
            Util.writeViewId(view_id,outstream);
            Bits.writeString(logical_name,outstream);
            Util.writeAddress(physical_addr, outstream);
        }

        @SuppressWarnings("unchecked")
        public void readFrom(DataInput instream) throws Exception {
            type=Type.values()[instream.readByte()];
            view_id=Util.readViewId(instream);
            logical_name=Bits.readString(instream);
            physical_addr=(PhysicalAddress)Util.readAddress(instream);
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(type + ": ");
            if(view_id != null)
                sb.append("view_id=" + view_id);
            sb.append(", logical_name=" + logical_name + ", physical_addr=" + physical_addr);
            return sb.toString();
        }

        protected static enum Type {INFO, VIEW_REQ, VIEW_RSP}
    }
}
