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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * The advantage compared to {@link MERGE2} is that there are no merge collisions caused by multiple merges going on.
 * Also, the INFO traffic is spread out over max_interval, and every member sends its physical address with INFO, so
 * we don't need to fetch the physical address first.
 *
 * @author Bela Ban, Nov 2011
 * @since 3.1
 */
@MBean(description="Protocol to discover subgroups existing due to a network partition")
public class MERGE3 extends Protocol {
    

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    protected long min_interval=1000;
    
    protected long max_interval=10000;

    @Property(description="The max number of merge participants to be involved in a merge. 0 sets this to unlimited.")
    protected int max_participants_in_merge=100;

    /* ---------------------------------------------- JMX -------------------------------------------------------- */
    @ManagedAttribute(description="Interval (in ms) after which we check for view inconsistencies",writable=true)
    protected long check_interval=0;

    @ManagedAttribute(description="Number of cached ViewIds")
    public int getViews() {return views.size();}

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected Address       local_addr=null;

    protected View          view;

    protected TimeScheduler timer;

    protected Future<?>     info_sender;

    protected Future<?>     view_consistency_checker;

    // hashmap to keep track of view-id sent in INFO messages
    protected final ConcurrentMap<ViewId,SortedSet<Address>> views=new ConcurrentHashMap<ViewId,SortedSet<Address>>(view != null? view.size() : 16);

    protected final ResponseCollector<View> view_rsps=new ResponseCollector<View>();

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
        for(Map.Entry<ViewId,SortedSet<Address>> entry: views.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        return sb.toString();
    }

    @ManagedOperation(description="Clears the views cache")
    public void clearViews() {views.clear();}



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
                log.warn("set check_interval=" + computeCheckInterval() + " as it is <= max_interval");
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

    @Property(description="Minimum time in ms before sending an info message")
    public void setMinInterval(long i) {
        if(min_interval < 0 || min_interval >= max_interval)
            throw new IllegalArgumentException("min_interval (" + min_interval + ") has to be < max_interval (" + max_interval + ")");
        min_interval=i;
    }

    public long getMaxInterval() {
        return max_interval;
    }


    @Property(description="Interval (in milliseconds) when the next info " +
      "message will be sent. A random value is picked from range [1..max_interval]")
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
        return retval instanceof Boolean && ((Boolean)retval).booleanValue();
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
        
            case Event.VIEW_CHANGE:
                stopViewConsistencyChecker();
                stopInfoSender();
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
                        if(hdr.logical_name != null && sender instanceof UUID)
                            UUID.add(sender, hdr.logical_name);
                        if(hdr.physical_addrs != null) {
                            for(PhysicalAddress physical_addr: hdr.physical_addrs)
                                down(new Event(Event.SET_PHYSICAL_ADDRESS,
                                               new Tuple<Address,PhysicalAddress>(sender, physical_addr)));
                        }
                        SortedSet<Address> existing=views.get(hdr.view_id);
                        if(existing == null) {
                            existing=new ConcurrentSkipListSet<Address>();
                            SortedSet<Address> tmp=views.putIfAbsent(hdr.view_id, existing);
                            if(tmp != null)
                                existing=tmp;
                        }
                        existing.add(sender);
                        if(log.isTraceEnabled())
                            log.trace(local_addr + " <-- " + sender + ": " + hdr + ", cached views: " + views.size());
                        break;
                    case VIEW_REQ:
                        View tmp_view=view != null? view.copy() : null;
                        Header tmphdr=MergeHeader.createViewResponse(tmp_view);
                        Message view_rsp=new Message(sender);
                        view_rsp.putHeader(getId(), tmphdr);
                        down_prot.down(new Event(Event.MSG, view_rsp));
                        break;
                    case VIEW_RSP:
                        if(hdr.view != null)
                            view_rsps.add(sender, hdr.view);
                        break;
                    default:
                        log.error("Type " + hdr.type + " not known");
                }
                return null;
        }
        return up_prot.up(evt);
    }





    public static List<View> detectDifferentViews(Map<Address,View> map) {
        final List<View> ret=new ArrayList<View>();
        for(View view: map.values()) {
            if(view == null)
                continue;
            ViewId vid=view.getVid();
            if(!Util.containsViewId(ret, vid))
                ret.add(view);
        }
        return ret;
    }


    protected class InfoSender implements TimeScheduler.Task {

        public void run() {
            if(view == null) {
                log.warn("view is null, cannot send INFO message");
                return;
            }
            PhysicalAddress physical_addr=local_addr != null?
              (PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr)) : null;
            String logical_name=UUID.get(local_addr);
            ViewId view_id=view.getViewId();
            MergeHeader hdr=MergeHeader.createInfo(view_id, logical_name, Arrays.asList(physical_addr));

            if(transport_supports_multicasting) {
                Message msg=new Message();
                msg.putHeader(getId(), hdr);
                down_prot.down(new Event(Event.MSG, msg));
                return;
            }

            Discovery discovery_protocol=(Discovery)stack.findProtocol(Discovery.class);
            if(discovery_protocol == null) {
                log.warn("no discovery protocol found, cannot ask for physical addresses to send INFO message");
                return;
            }
            Collection<PhysicalAddress> physical_addrs=discovery_protocol.fetchClusterMembers(cluster_name);
            if(physical_addrs == null)
                physical_addrs=(Collection<PhysicalAddress>)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESSES));

            if(physical_addrs == null || physical_addrs.isEmpty())
                return;
            if(log.isTraceEnabled())
                log.trace("discovery protocol " + discovery_protocol.getName() + " returned " + physical_addrs.size() +
                            " physical addresses: " + Util.printListWithDelimiter(physical_addrs, ", ", 10));
            for(Address addr: physical_addrs) {
                Message info=new Message(addr);
                info.putHeader(getId(), hdr);
                down_prot.down(new Event(Event.MSG, info));
            }
        }

        public long nextInterval() {
            return Math.max(min_interval, Util.random(max_interval) + max_interval/2);
        }
    }


    protected class ViewConsistencyChecker implements TimeScheduler.Task {

        public void run() {
            try {
                if(views.size() <= 1) {
                    if(log.isTraceEnabled())
                        log.trace("found no inconsistent views: " + dumpViews());
                    return;
                }
                _run();
            }
            finally {
                clearViews();
            }
        }

        protected void _run() {
            SortedSet<Address> coords=new TreeSet<Address>();

            for(ViewId view_id: views.keySet())
                coords.add(view_id.getCreator());

            Address merge_leader=coords.isEmpty() ? null : coords.first();
            if(merge_leader == null || local_addr == null || !merge_leader.equals(local_addr)) {
                if(log.isTraceEnabled())
                    log.trace("I (" + local_addr + ") won't be the merge leader");
                return;
            }

            if(log.isDebugEnabled())
                log.debug("I (" + local_addr + ") will be the merge leader");

            // add merge participants
            for(SortedSet<Address> set: views.values()) {
                if(!set.isEmpty())
                    coords.add(set.first());
            }

            if(coords.size() <= 1) {
                log.trace("cancelling merge as we only have 1 coordinator: " + coords);
                return;
            }
            if(log.isTraceEnabled())
                log.trace("merge participants are " + coords);

            if(max_participants_in_merge > 0 && coords.size() > max_participants_in_merge) {
                int old_size=coords.size();
                for(Iterator<Address> it=coords.iterator(); it.hasNext();) {
                    Address next=it.next();
                    if(next.equals(merge_leader))
                        continue;
                    if(coords.size() > max_participants_in_merge)
                        it.remove();
                }
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": reduced " + old_size + " coords to " + max_participants_in_merge);
            }

            // grab views from all members in coords
            view_rsps.reset(coords);
            for(Address target: coords) {
                if(target.equals(local_addr)) {
                    if(view != null)
                        view_rsps.add(local_addr, view.copy());
                    continue;
                }
                Message view_req=new Message(target);
                Header hdr=MergeHeader.createViewRequest();
                view_req.putHeader(getId(), hdr);
                down_prot.down(new Event(Event.MSG, view_req));
            }
            view_rsps.waitForAllResponses(check_interval / 10);
            Map<Address,View> results=view_rsps.getResults();
            Map<Address,View> merge_views=new HashMap<Address,View>();

            for(Map.Entry<Address,View> entry: results.entrySet())
                if(entry.getValue() != null)
                    merge_views.put(entry.getKey(), entry.getValue());

            if(merge_views.size() >= 2) {
                up_prot.up(new Event(Event.MERGE, merge_views));
                num_merge_events++;
            }
        }

        public long nextInterval() {
            return check_interval;
        }

    }


    public static class MergeHeader extends Header {
        protected Type                        type=Type.INFO;
        protected ViewId                      view_id;
        protected View                        view;
        protected String                      logical_name;
        protected Collection<PhysicalAddress> physical_addrs;


        public MergeHeader() {
        }

        public static MergeHeader createInfo(ViewId view_id, String logical_name, Collection<PhysicalAddress> physical_addrs) {
            return new MergeHeader(Type.INFO, view_id, null, logical_name, physical_addrs);
        }

        public static MergeHeader createViewRequest() {
            return new MergeHeader(Type.VIEW_REQ, null, null, null, null);
        }

        public static MergeHeader createViewResponse(View view) {
            return new MergeHeader(Type.VIEW_RSP, null, view, null, null);
        }

        protected MergeHeader(Type type, ViewId view_id, View view, String logical_name, Collection<PhysicalAddress> physical_addrs) {
            this.type=type;
            this.view_id=view_id;
            this.view=view;
            this.logical_name=logical_name;
            this.physical_addrs=physical_addrs;
        }

        public int size() {
            int retval=Global.BYTE_SIZE; // for the type
            retval+=Util.size(view_id);
            retval+=Util.size(view);
            retval+=Global.BYTE_SIZE;     // presence byte for logical_name
            if(logical_name != null)
                retval+=logical_name.length() +2;
            retval+=Util.size(physical_addrs);
            return retval;
        }

        public void writeTo(DataOutput outstream) throws Exception {
            outstream.writeByte(type.ordinal()); // a byte if ok as we only have 3 types anyway
            Util.writeViewId(view_id,outstream);
            Util.writeView(view, outstream);
            Util.writeString(logical_name, outstream);
            Util.writeAddresses(physical_addrs, outstream);
        }

        @SuppressWarnings("unchecked")
        public void readFrom(DataInput instream) throws Exception {
            type=Type.values()[instream.readByte()];
            view_id=Util.readViewId(instream);
            view=Util.readView(instream);
            logical_name=Util.readString(instream);
            physical_addrs=(Collection<PhysicalAddress>)Util.readAddresses(instream,ArrayList.class);
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(type + ": ");
            if(view_id != null)
                sb.append("view_id=" + view_id);
            else if(view != null)
                sb.append(" view=").append(view);
            sb.append(", logical_name=" + logical_name + ", physical_addr=" + physical_addrs);
            return sb.toString();
        }

        protected static enum Type {INFO, VIEW_REQ, VIEW_RSP}
    }
}
