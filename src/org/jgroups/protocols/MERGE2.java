package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.*;
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
 * <li>If coordinator: periodically retrieve the initial membership (using the FIND_INITIAL_MBRS event provided e.g.
 *     by PING or TCPPING protocols. This list contains {coord,addr} pairs.
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
 * Requires: FIND_INITIAL_MBRS event from below<br>
 * Provides: sends MERGE event with list of coordinators up the stack<br>
 * @author Bela Ban, Oct 16 2001
 */
@MBean(description="Protocol to discover subgroups existing due to a network partition")
public class MERGE2 extends Protocol {
    

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Minimum time in ms between runs to discover other clusters")
    protected long min_interval=5000;
    
    @Property(description="Maximum time in ms between runs to discover other clusters")
    protected long max_interval=20000;

    @Property(description="Number of inconsistent  views with only 1 coord after a MERGE event is sent up")
    protected int inconsistent_view_threshold=1;

    @Property(description="When receiving a multicast message, checks if the sender is member of the cluster. " +
      "If not, initiates a merge. Generates a lot of traffic for large clusters when there is a lot of merging")
    protected boolean merge_fast=true;

    @Property(description="The delay (in milliseconds) after which a merge fast execution is started")
    protected long merge_fast_delay=1000;

    /* ---------------------------------------------- JMX -------------------------------------------------------- */
    @ManagedAttribute(writable=false, description="whether or not a merge task is currently running " +
            "(should be the case in a coordinator")
    public boolean isMergeTaskRunning() {
        return task.isRunning();
    }

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected Address local_addr=null;

    protected View view;

    protected final Set<Address> members=new HashSet<Address>();

    protected final Set<Address> merge_candidates=new CopyOnWriteArraySet<Address>();
    
    protected final FindSubgroupsTask task=new FindSubgroupsTask();
    
    protected volatile boolean is_coord=false;
    
    protected TimeScheduler timer;

    @ManagedAttribute(description="Number of inconsistent 1-coord views until a MERGE event is sent up the stack")
    protected int num_inconsistent_views=0;

    @ManagedAttribute(description="Number of times a MERGE event was sent up the stack")
    protected int num_merge_events=0;


    public void init() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved");

        if(min_interval <= 0 || max_interval <= 0)
            throw new Exception("min_interval and max_interval have to be > 0");            
        
        if(max_interval <= min_interval)
            throw new Exception ("max_interval has to be greater than min_interval");        
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

    public List<Integer> requiredDownServices() {
        return Arrays.asList(Event.FIND_INITIAL_MBRS, Event.FIND_ALL_VIEWS);
    }

    /** Discovers members and detects whether we have multiple coordinator. If so, kicks off a merge */
    @ManagedOperation
    public void sendMergeSolicitation() {
        task.findAndNotify();
    }

    @ManagedOperation public void startMergeTask() {task.start();}
    
    @ManagedOperation public void stopMergeTask() {task.stop();}

    public void stop() {
        is_coord=false;
        merge_candidates.clear();
        task.stop();
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
        
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
                Address coord=mbrs.isEmpty()? null : mbrs.get(0);
                if(coord != null && coord.equals(local_addr)) {
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
                if(!merge_fast)
                    break;
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest != null)
                    break;
                final Address sender=msg.getSrc();
                if(!members.contains(sender) && merge_candidates.add(sender)) {
                    timer.schedule(new Runnable() {
                        public void run() {
                            if(!members.contains(sender))
                                task.findAndNotify();
                        }
                    }, merge_fast_delay, TimeUnit.MILLISECONDS);
                }
                break;
        }
        return up_prot.up(evt);
    }


    public void up(MessageBatch batch) {
        if(merge_fast && batch.dest() == null) {
            final Address sender=batch.sender();
            if(!members.contains(sender) && merge_candidates.add(sender)) {
                timer.schedule(new Runnable() {
                    public void run() {
                        if(!members.contains(sender))
                            task.findAndNotify();
                    }
                }, merge_fast_delay, TimeUnit.MILLISECONDS);
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /**
     * Task periodically executing (if role is coordinator). Gets the initial membership and determines
     * whether there are subgroups (multiple coordinators for the same group). If yes, it sends a MERGE event
     * with the list of the coordinators up the stack
     */
    protected class FindSubgroupsTask {
        @GuardedBy("this")
        private Future<?> future;
        private Lock      lock=new ReentrantLock();

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


        public void findAndNotify() {
            if(isMergeRunning())
                return;
            if(lock.tryLock()) {
                try {
                    _findAndNotify();
                }
                catch(Throwable t) {
                    log.error("FindSubgroupsTask failed", t);
                }
                finally {
                    lock.unlock();
                }
            }
        }

        protected void _findAndNotify() {
            List<PingData> discovery_rsps=findAllViews();

            if(log.isTraceEnabled()) {
                StringBuilder sb=new StringBuilder();
                sb.append("Discovery results:\n");
                for(PingData data: discovery_rsps)
                    sb.append("[" + data.getAddress() + "]: " + data.printViewId()).append("\n");
                log.trace(sb);
            }

            // Create a map of senders and the views they sent
            Map<Address,View> views=getViews(discovery_rsps);

            // A list of different views
            List<View> different_views=Util.detectDifferentViews(views);
            if(different_views.size() <= 1) {
                num_inconsistent_views=0;
                return;
            }
            Collection<Address> merge_participants=Util.determineMergeParticipants(views);
            if(merge_participants.size() == 1) {
                if(num_inconsistent_views < inconsistent_view_threshold) {
                    if(log.isDebugEnabled())
                        log.debug(local_addr + ": dropping MERGE for inconsistent views " + Util.printViews(different_views) +
                                    " as inconsistent view threshold (" + inconsistent_view_threshold +
                                    ") has not yet been reached (" + num_inconsistent_views + ")");
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
                for(PingData data: discovery_rsps)
                    sb.append("[" + data.getAddress() + "]: coord=" + data.getCoordAddress()).append("\n");
                log.debug(sb.toString());
            }
            Event evt=new Event(Event.MERGE, views);
            try {
                up_prot.up(evt);
                num_merge_events++;
            }
            catch(Throwable t) {
                log.error("failed sending up MERGE event", t);
            }
        }


        /**
         * Returns a random value within [min_interval - max_interval]
         */
        protected long computeInterval() {
            return min_interval + Util.random(max_interval - min_interval);
        }

        /** Returns a list of PingData with only the view from members around the cluster */
        @SuppressWarnings("unchecked")
        protected List<PingData> findAllViews() {
            List<PingData> retval=(List<PingData>)down_prot.down(new Event(Event.FIND_ALL_VIEWS));
            if(retval == null) return Collections.emptyList();
            if(is_coord && local_addr != null) {
                PingData tmp=new PingData(local_addr, view, true);
                //let's make sure that we add ourself as a coordinator
                if(!retval.contains(tmp))
                    retval.add(tmp);
            } 
            return retval;
        }



        public Map<Address,View> getViews(List<PingData> initial_mbrs) {
            Map<Address,View> retval=new HashMap<Address,View>();
            for(PingData response: initial_mbrs) {
                if(!response.isServer())
                    continue;
                Address sender=response.getAddress();
                View view=response.getView();
                if(sender == null || view == null)
                    continue;
                retval.put(sender,view);
            }
            return retval;
        }


    }
}
