
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
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
 * <li>If coordinator: periodically broadcast its view. If another coordinator receives such a message, and its own
 * view differs from the received view, it immediately initiates a merge by sending up a MERGE event,
 * containing the received view and its own view
 * <p>
 *
 * Provides: sends MERGE event with list of different views up the stack<br>
 * @author Bela Ban, Oct 16 2001
 */
@Experimental @Unsupported
@DeprecatedProperty(names={"use_separate_thread"})
public class MERGE3 extends Protocol {
    Address local_addr=null;
    View view;

    @Property
    long min_interval=5000;     // minimum time between executions of the FindSubgroups task
    @Property
    long max_interval=20000;    // maximum time between executions of the FindSubgroups task
    boolean is_coord=false;
    final Vector<Address>  mbrs=new Vector<Address>();
    TimeScheduler timer=null;
    Future<?> announcer_task_future=null;


    public void init() throws Exception {
        timer=getTransport().getTimer();
                
        if(min_interval <= 0 || max_interval <= 0) {
            throw new Exception("min_interval and max_interval have to be > 0");            
        }
        
        if(max_interval <= min_interval) {
            throw new Exception ("max_interval has to be greater than min_interval");        
        }  
    }



    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                CoordAnnouncement hdr=(CoordAnnouncement)msg.getHeader(this.id);
                if(hdr != null) {
                    if(is_coord) {
                        ViewId other=hdr.view.getViewId();
                        if(!Util.sameViewId(other, view.getViewId())) {
                            Map<Address,View> views=new HashMap<Address,View>();
                            views.put(local_addr, view);
                            views.put(msg.getSrc(), hdr.view);
                            if(log.isDebugEnabled())
                                log.debug("detected different views (" + Util.printViews(views.values()) + "), sending up MERGE event");
                            up_prot.up(new Event(Event.MERGE, views));
                        }
                    }
                    return null;
                }
                else
                    return up_prot.up(evt);
        }

        return up_prot.up(evt);
    }


    public Object down(Event evt) {
        Vector<Address> tmp;
        Address coord;

        switch(evt.getType()) {

            case Event.VIEW_CHANGE:
                down_prot.down(evt);
                view=(View)evt.getArg();
                tmp=view.getMembers();
                mbrs.clear();
                mbrs.addAll(tmp);
                coord=mbrs.elementAt(0);
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

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    void startCoordAnnouncerTask() {
        if(announcer_task_future == null || announcer_task_future.isDone()) {
            announcer_task_future=timer.scheduleWithDynamicInterval(new CoordinatorAnnouncer());
        }
    }

    void stopCoordAnnouncerTask() {
        if(announcer_task_future != null) {
            announcer_task_future.cancel(false);
            announcer_task_future=null;
        }
    }



    /**
     * Returns a random value within [min_interval - max_interval]
     */
    long computeInterval() {
        return min_interval + Util.random(max_interval - min_interval);
    }



    void sendView() {
        Message view_announcement=new Message(); // multicast to all
        CoordAnnouncement hdr=new CoordAnnouncement(view);
        view_announcement.putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG, view_announcement));
    }



    class CoordinatorAnnouncer implements TimeScheduler.Task {
        public long nextInterval() {
            return computeInterval();
        }

        public void run() {
            if(is_coord)
                sendView();
        }
    }



    public static class CoordAnnouncement extends Header {
        private View view;

        public CoordAnnouncement() {
        }

        public CoordAnnouncement(View view) {
            this.view=view;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            Util.writeView(view, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            view=Util.readView(in);
        }

        public int size() {
            return Util.size(view);
        }
    }

}
