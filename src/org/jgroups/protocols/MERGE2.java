// $Id: MERGE2.java,v 1.42.2.5 2009/02/02 16:26:25 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.List;
import java.util.Properties;
import java.util.Vector;
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
 */
public class MERGE2 extends Protocol {
    private Address					local_addr=null;   
    private final FindSubgroupsTask	task=new FindSubgroupsTask();             // task periodically executing as long as we are coordinator    
    private long					min_interval=5000;     // minimum time between executions of the FindSubgroups task
    private long					max_interval=20000;    // maximum time between executions of the FindSubgroups task
    private volatile boolean		is_coord=false;  
    private volatile boolean		use_separate_thread=false; // Use a new thread to send the MERGE event up the stack    
    private TimeScheduler			timer;
    
    public void init() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved from protocol stack");
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


    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("min_interval");
        if(str != null) {
            min_interval=Long.parseLong(str);
            props.remove("min_interval");
        }

        str=props.getProperty("max_interval");
        if(str != null) {
            max_interval=Long.parseLong(str);
            props.remove("max_interval");
        }

        if(min_interval <= 0 || max_interval <= 0) {
            if(log.isErrorEnabled()) log.error("min_interval and max_interval have to be > 0");
            return false;
        }
        if(max_interval <= min_interval) {
            if(log.isErrorEnabled()) log.error("max_interval has to be greater than min_interval");
            return false;
        }

        str=props.getProperty("use_separate_thread");
        if(str != null) {
            use_separate_thread=Boolean.valueOf(str).booleanValue();
            props.remove("use_separate_thread");
        }

        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    public Vector<Integer> requiredDownServices() {
        Vector<Integer> retval=new Vector<Integer>(1);
        retval.addElement(new Integer(Event.FIND_INITIAL_MBRS));
        return retval;
    }


    public void stop() {
        is_coord=false;
        task.stop();
    }



    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                return up_prot.up(evt);

            default:
                return up_prot.up(evt);            // Pass up to the layer above us
        }
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
        
            case Event.VIEW_CHANGE:
                Object ret=down_prot.down(evt);
                Vector<Address> mbrs=((View)evt.getArg()).getMembers();
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
                }, 0, computeInterval(), TimeUnit.MILLISECONDS);
            }
        }

        public synchronized void stop() {
            if(future != null) {
                future.cancel(true);
                future=null;
            }
        }

        public void findAndNotify() {
            List<PingRsp> initial_mbrs=findInitialMembers();
            
            Vector<Address> coords=detectMultipleCoordinators(initial_mbrs);
            if(coords.size() > 1) {
                if(log.isDebugEnabled())
                    log.debug(local_addr + " found multiple coordinators: " + coords + "; sending up MERGE event");
                final Event evt=new Event(Event.MERGE, coords);
                if(use_separate_thread) {
                    Thread merge_notifier=new Thread() {
                        public void run() {
                            up_prot.up(evt);
                        }
                    };
                    merge_notifier.setDaemon(true);
                    merge_notifier.setName("merge notifier thread");
                    merge_notifier.start();
                }
                else {
                    up_prot.up(evt);
                }
            }
            if(log.isTraceEnabled())
                log.trace("MERGE2.FindSubgroups thread terminated (local_addr=" + local_addr + ")");
        }

        /**
         * Returns a random value within [min_interval - max_interval]
         */
        long computeInterval() {
            return min_interval + Util.random(max_interval - min_interval);
        }

        /**
         * Returns a list of PingRsp pairs.
         */
        List<PingRsp> findInitialMembers() {
            PingRsp tmp=new PingRsp(local_addr, local_addr, true);
            List<PingRsp> retval=(List<PingRsp>)down_prot.down(new Event(Event.FIND_INITIAL_MBRS));
            if(retval != null && is_coord && local_addr != null && !retval.contains(tmp))
                retval.add(tmp);
            return retval;
        }

        /**
         * Finds out if there is more than 1 coordinator in the initial_mbrs vector (contains PingRsp elements).
         * @param initial_mbrs A list of PingRsp pairs
         * @return Vector A list of the coordinators (Addresses) found. Will contain just 1 element for a correct
         *         membership, and more than 1 for multiple coordinators
         */
        Vector<Address> detectMultipleCoordinators(List<PingRsp> initial_mbrs) {
            Vector<Address> ret=new Vector<Address>(11);
            if(initial_mbrs != null) {
                for(PingRsp response:initial_mbrs) {
                    if(response.isServer()) {
                        Address coord=response.getCoordAddress();
                        if(!ret.contains(coord))
                            ret.add(coord);
                    }
                }
            }
            return ret;
        }
    }
}
