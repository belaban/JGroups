package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


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
 * @version $Id: MERGE2.java,v 1.70 2009/08/27 13:17:01 belaban Exp $
 */
@MBean(description="Protocol to discover subgroups existing due to a network partition")
@DeprecatedProperty(names={"use_separate_thread"})
public class MERGE2 extends Protocol {
    
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @ManagedAttribute(description="Minimum time between runs to discover other clusters", writable=true)
    @Property(description="Lower bound in msec to run merge protocol. Default is 5000 msec")
    private long min_interval=5000;
    
    @ManagedAttribute(description="Maximum time between runs to discover other clusters", writable=true)
    @Property(description="Upper bound in msec to run merge protocol. Default is 20000 msec")
    private long max_interval=20000;   

    /** Number of inconsistent  views with only 1 coord after a MERGE event is sent up */
    @Property
    @ManagedAttribute(description="Number of inconsistent  views with only 1 coord after a MERGE event is sent up",
                      writable=true)
    private int inconsistent_view_threshold=1;

    /* ---------------------------------------------- JMX -------------------------------------------------------- */
    @ManagedAttribute(writable=false, description="whether or not a merge task is currently running " +
            "(should be the case in a coordinator")
    public boolean isMergeTaskRunning() {
        return task.isRunning();
    }

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    private Address local_addr=null;

    private View view;
    
    private final FindSubgroupsTask task=new FindSubgroupsTask();   
    
    private volatile boolean is_coord=false;
    
    private TimeScheduler timer;

    @ManagedAttribute(description="Number of inconsistent 1-coord views until a MERGE event is sent up the stack")
    private int num_inconsistent_views=0;
    
    
    
    public MERGE2() {        
    }


    public void init() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved");
        
        if(min_interval <= 0 || max_interval <= 0) {
            throw new Exception("min_interval and max_interval have to be > 0");            
        }
        
        if(max_interval <= min_interval) {
            throw new Exception ("max_interval has to be greater than min_interval");        
        }  
    }


    public String getName() {
        return "MERGE2";
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

    public Vector<Integer> requiredDownServices() {
        Vector<Integer> retval=new Vector<Integer>(1);
        retval.addElement(new Integer(Event.FIND_INITIAL_MBRS));
        return retval;
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
        task.stop();
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
        
            case Event.VIEW_CHANGE:
                Object ret=down_prot.down(evt);
                view=(View)evt.getArg();
                Vector<Address> mbrs=view.getMembers();
                if(mbrs == null || mbrs.isEmpty() || local_addr == null) {
                    task.stop();
                    return ret;
                }
                Address coord=mbrs.elementAt(0);
                if(coord.equals(local_addr)) {
                    is_coord=true;
                    task.start(); // start task if we became coordinator (doesn't start if already running)
                }
                else {
                    // if we were coordinator, but are no longer, stop task. this happens e.g. when we merge and someone
                    // else becomes the new coordinator of the merged group
                    if(is_coord) {
                        is_coord=false;
                    }
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


    /**
	 * Task periodically executing (if role is coordinator). Gets the initial membership and determines
	 * whether there are subgroups (multiple coordinators for the same group). If yes, it sends a MERGE event
	 * with the list of the coordinators up the stack
	 */
    private class FindSubgroupsTask {
        @GuardedBy("this")
        private Future<?> future;

        public synchronized void start() {
            if(future == null || future.isDone()) {
                future=timer.scheduleWithFixedDelay(new Runnable() {
                    public void run() {
                        findAndNotify();
                    }
                },  Math.max(5000L, computeInterval()), computeInterval(), TimeUnit.MILLISECONDS);
            }
        }

        public synchronized void stop() {
            if(future != null) {
                future.cancel(true);
                future=null;
            }
        }

        public synchronized boolean isRunning() {
            return future != null && !future.isDone() && !future.isCancelled();
        }


        public void findAndNotify() {
            List<PingData> initial_mbrs=findInitialMembers();
            List<View> different_views=detectDifferentViews(initial_mbrs);
            if(different_views.size() > 1) {
                Collection<Address> coords=Util.determineCoords(different_views);
                if(coords.size() == 1) {
                    if(num_inconsistent_views < inconsistent_view_threshold) {
                        if(log.isDebugEnabled())
                            log.debug("dropping MERGE for inconsistent views " + Util.print(different_views) +
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
                    sb.append(local_addr + " found different views : " + Util.print(different_views) + "; sending up MERGE event.\n");
                    sb.append("Discovery results:\n");
                    for(PingData data: initial_mbrs)
                        sb.append("[" + data.getAddress() + "]: " + data.getView().getViewId()).append("\n");
                    log.debug(sb.toString());
                }
                Event evt=new Event(Event.MERGE, different_views);
                try {
                    up_prot.up(evt);
                }
                catch(Throwable t) {
                    log.error("failed sending up MERGE event", t);
                }
            }
            else
                num_inconsistent_views=0;
        }

        /**
         * Returns a random value within [min_interval - max_interval]
         */
        long computeInterval() {
            return min_interval + Util.random(max_interval - min_interval);
        }

        /** Returns a list of PingData pairs */
        @SuppressWarnings("unchecked")
        List<PingData> findInitialMembers() {
            PingData tmp=new PingData(local_addr, view, true);
            List<PingData> retval=(List<PingData>)down_prot.down(new Event(Event.FIND_ALL_INITIAL_MBRS));
            if(retval == null) return Collections.emptyList();
            if(is_coord && local_addr != null) {
                //let's make sure that we add ourself as a coordinator
                if(!retval.contains(tmp))
                    retval.add(tmp);
            } 
            return retval;
        }

        /**
         * Finds out if there is more than 1 coordinator in the initial_mbrs vector (contains PingData elements).
         * @param initial_mbrs A list of PingData pairs
         * @return Vector A list of the coordinators (Addresses) found. Will contain just 1 element for a correct
         *         membership, and more than 1 for multiple coordinators
         */
        List<Address> detectMultipleCoordinators(List<PingData> initial_mbrs) {
            Vector<Address> ret=new Vector<Address>();
             for(PingData response:initial_mbrs) {
                 if(response.isServer()) {
                     Address coord=response.getCoordAddress();
                     if(!ret.contains(coord))
                         ret.add(coord);
                 }
             }            
            return ret;
        }


        List<View> detectDifferentViews(List<PingData> initial_mbrs) {
            List<View> ret=new ArrayList<View>();
            for(PingData response: initial_mbrs) {
                if(!response.isServer())
                    continue;
                View view=response.getView();
                if(view == null)
                    continue;
                ViewId vid=view.getVid();
                if(!containsViewId(ret, vid))
                    ret.add(view);
            }
            return ret;
        }

        


        boolean containsViewId(Collection<View> views, ViewId vid) {
            for(View view: views) {
                ViewId tmp=view.getVid();
                if(Util.sameViewId(vid, tmp))
                    return true;
            }
            return false;
        }
        
    }
}
