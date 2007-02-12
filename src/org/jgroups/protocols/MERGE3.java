// $Id: MERGE3.java,v 1.17 2007/02/12 13:28:11 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.Future;


/**
 * Protocol to discover subgroups; e.g., existing due to a network partition (that healed). Example: group
 * {p,q,r,s,t,u,v,w} is split into 3 subgroups {p,q}, {r,s,t,u} and {v,w}. This protocol will eventually send
 * a MERGE event with the coordinators of each subgroup up the stack: {p,r,v}. Note that - depending on the time
 * of subgroup discovery - there could also be 2 MERGE events, which first join 2 of the subgroups, and then the
 * resulting group to the last subgroup. The real work of merging the subgroups into one larger group is done
 * somewhere above this protocol (typically in the GMS protocol).<p>
 * This protocol works as follows:
 * <ul>
 * <li>If coordinator: periodically broadcast a "I'm the coordinator" message. If a coordinator receives such
 * a message, it immediately initiates a merge by sending up a MERGE event
 * <p>
 *
 * Provides: sends MERGE event with list of coordinators up the stack<br>
 * @author Bela Ban, Oct 16 2001
 */
public class MERGE3 extends Protocol {
    Address local_addr=null;
    long min_interval=5000;     // minimum time between executions of the FindSubgroups task
    long max_interval=20000;    // maximum time between executions of the FindSubgroups task
    boolean is_coord=false;
    final Vector  mbrs=new Vector();
    TimeScheduler timer=null;
    Future announcer_task_future=null;
    CoordinatorAnnouncer announcer_task=null;
    final Set announcements=Collections.synchronizedSet(new HashSet());

    /** Use a new thread to send the MERGE event up the stack */
    boolean use_separate_thread=false;




    public String getName() {
        return "MERGE3";
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
            log.error("MERGE2.setProperties(): the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }

    public void init() throws Exception {
        timer=stack.timer;
    }



    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                CoordAnnouncement hdr=(CoordAnnouncement)msg.getHeader(getName());
                if(hdr != null) {
                    if(hdr.coord_addr != null && is_coord) {
                        boolean contains;
                        contains=announcements.contains(hdr.coord_addr);
                        announcements.add(hdr.coord_addr);
                        if(log.isDebugEnabled()) {
                            if(contains)
                                log.debug("discarded duplicate announcement: " + hdr.coord_addr +
                                        ", announcements=" + announcements);
                            else
                                log.debug("received announcement: " + hdr.coord_addr + ", announcements=" + announcements);
                        }

                        if(announcements.size() > 1 && is_coord) {
                            processAnnouncements();
                        }
                    }
                    return null;
                }
                else
                    return up_prot.up(evt);

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return up_prot.up(evt);
    }


    public Object down(Event evt) {
        Vector tmp;
        Address coord;

        switch(evt.getType()) {

            case Event.VIEW_CHANGE:
                down_prot.down(evt);
                tmp=((View)evt.getArg()).getMembers();
                mbrs.clear();
                mbrs.addAll(tmp);
                coord=(Address)mbrs.elementAt(0);
                if(coord.equals(local_addr)) {
                    if(is_coord == false) {
                        is_coord=true;
                        startCoordAnnouncerTask();
                    }
                }
                else {
                    if(is_coord == true) {
                        is_coord=false;
                        stopCoordAnnouncerTask();
                    }
                }
                break;
        }
        return down_prot.down(evt);
    }


    void startCoordAnnouncerTask() {
        if(announcer_task_future == null || announcer_task_future.isDone()) {
            announcements.add(local_addr);
            announcer_task=new CoordinatorAnnouncer();
            announcer_task_future=timer.scheduleWithDynamicInterval(announcer_task);
            if(log.isDebugEnabled())
                log.debug("coordinator announcement task started, announcements=" + announcements);
        }
    }

    void stopCoordAnnouncerTask() {
        if(announcer_task_future != null) {
            announcer_task_future.cancel(false);
            announcer_task_future=null;
        }
        announcer_task=null;
        announcements.clear();
        if(log.isDebugEnabled())
            log.debug("coordinator announcement task stopped");
    }



    /**
     * Returns a random value within [min_interval - max_interval]
     */
    long computeInterval() {
        return min_interval + Util.random(max_interval - min_interval);
    }



    void sendCoordinatorAnnouncement(Address coord) {
        Message coord_announcement=new Message(); // multicast to all
        CoordAnnouncement hdr=new CoordAnnouncement(coord);
        coord_announcement.putHeader(getName(), hdr);
        down_prot.down(new Event(Event.MSG, coord_announcement));
    }

    void processAnnouncements() {
        if(announcements.size() > 1) {
            Vector coords=new Vector(announcements);  // create a clone
            if(coords.size() > 1) {
                if(log.isDebugEnabled())
                    log.debug("passing up MERGE event, coords=" + coords);
                final Event evt=new Event(Event.MERGE, coords);
                if(use_separate_thread) {
                    Thread merge_notifier=new Thread(Util.getGlobalThreadGroup(), "merge notifier thread") {
                        public void run() {
                            up_prot.up(evt);
                        }
                    };
                    merge_notifier.setDaemon(true);
                    merge_notifier.start();
                }
                else {
                    up_prot.up(evt);
                }
            }
            announcements.clear();
            announcements.add(local_addr);
        }
    }


    class CoordinatorAnnouncer implements TimeScheduler.Task {
        public long nextInterval() {
            return computeInterval();
        }

        public void run() {
            if(is_coord)
                sendCoordinatorAnnouncement(local_addr);
        }
    }



    public static class CoordAnnouncement extends Header {
        Address coord_addr=null;

        public CoordAnnouncement() {
        }

        public CoordAnnouncement(Address coord) {
            this.coord_addr=coord;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            coord_addr=(Address)in.readObject();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(coord_addr);
        }
    }

}
