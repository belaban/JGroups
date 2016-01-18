package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.AbstractProtocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Protocol to discover subgroups; e.g., existing due to a network partition (that healed). Example: group
 * {p,q,r,s,t,u,v,w} is split into 3 subgroups {p,q}, {r,s,t,u} and {v,w}. This protocol will eventually send
 * a MERGE event with the coordinators of each subgroup up the stack: {p,r,v}. Note that - depending on the time
 * of subgroup discovery - there could also be 2 MERGE events, which first join 2 of the subgroups, and then the
 * resulting group to the last subgroup. The real work of merging the subgroups into one larger group is done
 * somewhere above this protocol (typically in the GMS protocol).<p>
 * This protocol works as follows:
 * <ul>
 * <li>If coordinator: periodically multicast a discovery message. Everyone (or (if configured) only the coords) reply
 * with their current view. Based on the received views:
 * <li>If there is more than 1 coordinator:
 *     <ol>
 *     <li>Get all coordinators
 *     <li>Create a MERGE event with the list of coordinators as argument
 *     <li>Send the event up the stack
 *     </ol>
 * </ul>
 *
 * <p>
 *
 * Provides: sends MERGE event with list of coordinators up the stack<br>
 * @author Bela Ban, Oct 16 2001
 * @deprecated Use {@link org.jgroups.protocols.MERGE3} instead, as it causes less traffic, especially for large clusters
 */
@MBean(description="Protocol to discover subgroups existing due to a network partition")
@Deprecated
public class MERGE2 extends AbstractProtocol {
    

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Minimum time in ms between runs to discover other clusters")
    protected long                    min_interval=5000;
    
    @Property(description="Maximum time in ms between runs to discover other clusters")
    protected long                    max_interval=20000;

    @Property(description="Number of inconsistent  views with only 1 coord after a MERGE event is sent up")
    protected int                     inconsistent_view_threshold=1;

    @Property(description="When receiving a multicast message, checks if the sender is member of the cluster. " +
      "If not, initiates a merge. Generates a lot of traffic for large clusters when there is a lot of merging")
    protected boolean                 merge_fast=true;

    @Property(description="The delay (in milliseconds) after which a merge fast execution is started")
    protected long                    merge_fast_delay=1000;

    @Property(description="Always sends a discovery response, no matter what",writable=true)
    protected boolean                 force_sending_discovery_rsps=true;

    @Property(description="Time (in ms) to wait for all discovery responses")
    protected long                    discovery_timeout=5000;

    /* ---------------------------------------------- JMX -------------------------------------------------------- */
    @ManagedAttribute(writable=false, description="whether or not a merge task is currently running " +
            "(should be the case in a coordinator")
    public boolean isMergeTaskRunning() {
        return task.isRunning();
    }

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected Address                 local_addr;
    protected volatile View           view;
    protected final Set<Address>      members=new HashSet<>();
    protected final Set<Address>      merge_candidates=new CopyOnWriteArraySet<>();
    protected final FindSubgroupsTask task=new FindSubgroupsTask();
    @ManagedAttribute(description="Whether this member is the current coordinator")
    protected volatile boolean        is_coord=false;
    protected volatile Address        current_coord;
    protected TimeScheduler           timer;

    @ManagedAttribute(description="Number of inconsistent 1-coord views until a MERGE event is sent up the stack")
    protected int                     num_inconsistent_views;

    @ManagedAttribute(description="Number of times a MERGE event was sent up the stack")
    protected int                     num_merge_events;

    protected final Map<Address,View> views=new ConcurrentHashMap<>();
    protected final Lock              discovery_lock=new ReentrantLock();
    protected final CondVar           discovery_cond=new CondVar(discovery_lock);
    protected volatile boolean        fetching_done=false;

    protected boolean                 transport_supports_multicasting=true;
    protected String                  cluster_name;



    public void init() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved");

        if(min_interval <= 0 || max_interval <= 0)
            throw new Exception("min_interval and max_interval have to be > 0");            
        
        if(max_interval <= min_interval)
            throw new Exception ("max_interval has to be greater than min_interval");
        transport_supports_multicasting=getTransport().supportsMulticasting();
    }

    public long getMinInterval() {
        return min_interval;
    }

    public void setMinInterval(long i) {
        min_interval=i;
    }

    public long getMaxInterval() {
        return max_interval;
    }

    public void setMaxInterval(long l) {
        max_interval=l;
    }

    protected boolean isMergeRunning() {
        Object retval=up_prot.up(new Event(Event.IS_MERGE_IN_PROGRESS));
        return retval instanceof Boolean && (Boolean)retval;
    }

    /** Discovers members and detects whether we have multiple coordinator. If so, kicks off a merge */
    @ManagedOperation
    public void sendMergeSolicitation() {
        task.findAndNotify();
    }

    @ManagedAttribute(description="The address of the current coordinator")
    public String getCurrentCoord() {return current_coord != null? current_coord.toString() : "n/a";}

    @ManagedOperation public void startMergeTask() {task.start();}
    
    @ManagedOperation public void stopMergeTask() {task.stop();}

    @ManagedOperation(description="Fetches all views")
    public String fetchAllViews() {
        try {
            task.fetchViews();
            StringBuilder sb=new StringBuilder();
            for(Map.Entry<Address,View> entry: views.entrySet())
                sb.append(entry.getKey() + ": " + entry.getValue() + "\n");
            return sb.toString();
        }
        catch(Throwable e) {
            return null;
        }
    }


    public void stop() {
        is_coord=false;
        merge_candidates.clear();
        task.stop();
    }


    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster_name=(String)evt.getArg();
                return down_prot.down(evt);

            case Event.VIEW_CHANGE:
                Object ret=down_prot.down(evt);
                view=(View)evt.getArg();
                List<Address> mbrs=view.getMembers();
                if(mbrs == null || mbrs.isEmpty() || local_addr == null) {
                    task.stop();
                    return ret;
                }
                members.clear();
                members.addAll(mbrs);
                merge_candidates.removeAll(members);

                current_coord=mbrs.isEmpty()? null : mbrs.get(0);
                if(current_coord != null && current_coord.equals(local_addr)) {
                    is_coord=true;
                    task.start(); // start task if we became coordinator (doesn't start if already running)
                }
                else {
                    // if we were coordinator, but are no longer, stop task. this happens e.g. when we merge and someone
                    // else becomes the new coordinator of the merged group
                    is_coord=false;
                    task.stop();
                }
                return ret;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                return down_prot.down(evt);

            default:
                return down_prot.down(evt);          // Pass on to the layer below us
        }
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                MergeHeader hdr=(MergeHeader)msg.getHeader(id);
                if(hdr == null) {
                    if(merge_fast)
                        mergeFast(msg.dest(), msg.src());
                    break;
                }
                else {
                    handle(hdr, msg.src());
                    return null;
                }
        }
        return up_prot.up(evt);
    }


    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            MergeHeader hdr=(MergeHeader)msg.getHeader(id);
            if(hdr != null) {
                batch.remove(msg);
                handle(hdr, batch.sender());
            }
        }
        if(!batch.isEmpty()) {
            if(merge_fast)
                mergeFast(batch.dest(), batch.sender());
            up_prot.up(batch);
        }
    }

    protected void mergeFast(final Address dest, final Address sender) {
        if(dest == null) {
            if(!members.contains(sender) && merge_candidates.add(sender)) {
                timer.schedule(new Runnable() {
                    public void run() {
                        if(!members.contains(sender))
                            task.findAndNotify();
                    }
                }, merge_fast_delay, TimeUnit.MILLISECONDS);
            }
        }
    }


    protected void handle(MergeHeader hdr, Address sender) {
        switch(hdr.type) {
            case 1: // request
                boolean send_discovery_rsp=force_sending_discovery_rsps || is_coord
                  || current_coord == null || current_coord.equals(sender);
                if(!send_discovery_rsp) {
                    log.trace("%s: suppressing discovery response as I'm not a coordinator and the " +
                                "discovery request was not sent by a coordinator", local_addr);
                    return;
                }
                if(isMergeRunning()) {
                    log.trace("%s: suppressing discovery response as a merge is in progress", local_addr);
                    return;
                }

                if(view != null) {
                    Message msg=new Message(sender).putHeader(id,new MergeHeader(MergeHeader.RSP,view));
                    down_prot.down(new Event(Event.MSG, msg));
                }
                break;
            case 2: // response
                View rsp_view=hdr.view;
                if(rsp_view != null && !fetching_done) {
                    views.put(sender,rsp_view);
                    List<View> diff_views=Util.detectDifferentViews(views);
                    if(diff_views.size() > 1) {
                        discovery_lock.lock();
                        try {
                            fetching_done=true;
                            discovery_cond.signal(true);
                        }
                        finally {
                            discovery_lock.unlock();
                        }
                    }
                }
                break;
        }
    }


    /**
     * Task periodically executing (if role is coordinator). Gets the initial membership and determines
     * whether there are subgroups (multiple coordinators for the same group). If yes, it sends a MERGE event
     * with the list of the coordinators up the stack
     */
    protected class FindSubgroupsTask implements Condition {
        @GuardedBy("this")
        protected Future<?>  future;
        protected final Lock lock=new ReentrantLock();


        public synchronized void start() {
            if(future == null || future.isDone()) {
                future=timer.scheduleWithDynamicInterval(new TimeScheduler.Task() {
                    public long nextInterval() {
                        return computeInterval();
                    }

                    public void run() {
                        findAndNotify();
                    }

                    public String toString() {
                        return MERGE2.class.getSimpleName() + ": " + getClass().getSimpleName();
                    }
                });
            }
        }

        public synchronized void stop() {
            if(future != null) {
                future.cancel(true);
                future=null;
            }
        }

        public synchronized boolean isRunning() {
            return future != null && !future.isDone();
        }

        public boolean isMet() {return fetching_done;}


        public void findAndNotify() {
            if(isMergeRunning())
                return;
            if(lock.tryLock()) {
                try {
                    _findAndNotify();
                }
                catch(InterruptedException iex) {

                }
                catch(Throwable t) {
                    log.error(Util.getMessage("FindSubgroupsTaskFailed"), t);
                }
                finally {
                    lock.unlock();
                }
            }
        }

        protected void _findAndNotify() throws InterruptedException {
            fetchViews();

            if(log.isTraceEnabled()) {
                StringBuilder sb=new StringBuilder();
                sb.append(local_addr + ": discovery results:\n");
                for(Map.Entry<Address,View> entry: views.entrySet())
                    sb.append("[" + entry.getKey() + "]: " + entry.getValue().getViewId()).append("\n");
                log.trace(sb);
            }

            // A list of different views
            List<View> different_views=Util.detectDifferentViews(views);
            if(different_views.size() <= 1) {
                num_inconsistent_views=0;
                return;
            }
            Collection<Address> merge_participants=Util.determineMergeParticipants(views);
            if(merge_participants.size() == 1) {
                if(num_inconsistent_views < inconsistent_view_threshold) {
                    log.debug("%s: dropping MERGE for inconsistent views (%s) as inconsistent view threshold (%d) " +
                                "has not yet been reached (%d)",
                              local_addr, Util.printViews(different_views), inconsistent_view_threshold, num_inconsistent_views);
                    num_inconsistent_views++;
                    return;
                }
                else
                    num_inconsistent_views=0;
            }
            else
                num_inconsistent_views=0;

            if(log.isDebugEnabled()) {
                StringBuilder sb=new StringBuilder();
                sb.append(local_addr + " found different views : " + Util.printViews(different_views) +
                        "; sending up MERGE event with merge participants " + merge_participants + ".\n");
                sb.append("Discovery results:\n");
                for(Map.Entry<Address,View> entry: views.entrySet())
                    sb.append("[" + entry.getKey() + "]: coord=" + entry.getValue().getCreator()).append("\n");
                log.debug(sb.toString());
            }
            Event evt=new Event(Event.MERGE, views);
            try {
                up_prot.up(evt);
                num_merge_events++;
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedSendingUpMERGEEvent"), t);
            }
        }

        /** Fetches views from members in the cluster. Returns when either discovery_timeout ms have elapsed or
         * more than 1 different view has been found */
        protected void fetchViews() throws InterruptedException {
            views.clear();
            View tmp_view=view;
            if(tmp_view != null)
                views.put(local_addr, tmp_view);
            fetching_done=false;

            if(transport_supports_multicasting) {
                Message discovery_req=new Message(null).putHeader(id, new MergeHeader(MergeHeader.REQ, null))
                  .setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
                down_prot.down(new Event(Event.MSG, discovery_req));
                try {
                    discovery_cond.waitFor(this, discovery_timeout, TimeUnit.MILLISECONDS);
                }
                finally {
                    fetching_done=true;
                }
                return;
            }

            Responses rsps=(Responses)down_prot.down(Event.FIND_MBRS_EVT);
            rsps.waitFor(discovery_timeout); // return immediately if done
            if(rsps.isEmpty())
                return;

            log.trace("discovery protocol returned %d responses: %s", rsps.size(), rsps);
            for(PingData rsp: rsps) {
                PhysicalAddress addr=rsp.getPhysicalAddr();
                if(addr == null) continue;
                Message discovery_req=new Message(addr).putHeader(id, new MergeHeader(MergeHeader.REQ, null))
                  .setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
                down_prot.down(new Event(Event.MSG, discovery_req));
            }

            try {
                discovery_cond.waitFor(this, discovery_timeout, TimeUnit.MILLISECONDS);
            }
            finally {
                fetching_done=true;
            }
        }


        /**
         * Returns a random value within [min_interval - max_interval]
         */
        protected long computeInterval() {
            return min_interval + Util.random(max_interval - min_interval);
        }
    }


    protected static class MergeHeader extends AbstractHeader {
        protected byte    type=1; // 1 == req, 2 == rsp
        protected View    view;
        protected static final byte REQ=1;
        protected static final byte RSP=2;

        public MergeHeader() {
        }

        public MergeHeader(byte type, View view) {
            this.type=type;
            this.view=view;
        }

        public String toString() {
            return (type==1? "req" : "rsp") + ", view=" + view;
        }

        public int size() {
            return Global.BYTE_SIZE + Util.size(view);
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeView(view, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            view=Util.readView(in);
        }
    }

}
