package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Periodically sends the view to the group. When a view is received which is greater than the current view, we
 * install it. Otherwise we simply discard it. This is used to solve the problem for unreliable view
 * dissemination outlined in JGroups/doc/ReliableViewInstallation.txt. This protocol is supposed to be just below GMS.
 * @author Bela Ban
 * @version $Id: VIEW_SYNC.java,v 1.25 2008/03/08 09:46:46 vlada Exp $
 */
@MBean(description="Periodically sends the view to the group")
public class VIEW_SYNC extends Protocol {
    Address               local_addr=null;
    final Vector<Address> mbrs=new Vector<Address>();
    View                  my_view=null;
    ViewId                my_vid=null;

    /** Sends a VIEW_SYNC message to the group every 20 seconds on average. 0 disables sending of VIEW_SYNC messages */
    @ManagedAttribute(description="Sends a VIEW_SYNC message to the group every 20 seconds on average",readable=true,writable=true)
    long                 avg_send_interval=60000;

    private int          num_views_sent=0;
    private int          num_views_adjusted=0;

    @GuardedBy("view_task_lock")
    private Future<?>       view_send_task_future=null;       // bcasts periodic view sync message (added to timer below)

    private final Lock   view_task_lock=new ReentrantLock();

    TimeScheduler        timer=null;
    static final String  name="VIEW_SYNC";



    public String getName() {
        return name;
    }

    public long getAverageSendInterval() {
        return avg_send_interval;
    }

    public void setAverageSendInterval(long gossip_interval) {
        avg_send_interval=gossip_interval;
    }

    @ManagedAttribute
    public int getNumViewsSent() {
        return num_views_sent;
    }

    @ManagedAttribute
    public int getNumViewsAdjusted() {
        return num_views_adjusted;
    }

    @ManagedOperation
    public void resetStats() {
        super.resetStats();
        num_views_adjusted=num_views_sent=0;
    }



    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);

        str=props.getProperty("avg_send_interval");
        if(str != null) {
            avg_send_interval=Long.parseLong(str);
            props.remove("avg_send_interval");
        }

        if(!props.isEmpty()) {
            log.error("these properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    public void start() throws Exception {
        if(stack != null && stack.timer != null)
            timer=stack.timer;
        else
            throw new Exception("timer cannot be retrieved from protocol stack");
    }

    public void stop() {
        stopViewSender();
    }

    /** Sends a VIEW_SYNC_REQ to all members, every member replies with a VIEW multicast */
    @ManagedOperation(description="Sends a VIEW_SYNC_REQ to all members")
    public void sendViewRequest() {
        Message msg=new Message(null);
        msg.setFlag(Message.OOB);
        ViewSyncHeader hdr=new ViewSyncHeader(ViewSyncHeader.VIEW_SYNC_REQ, null);
        msg.putHeader(name, hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }

//    public void sendFakeViewForTestingOnly() {
//        ViewId fake_vid=new ViewId(local_addr, my_vid.getId() +2);
//        View fake_view=new View(fake_vid, new Vector(my_view.getMembers()));
//        System.out.println("sending fake view " + fake_view);
//        my_view=fake_view;
//        my_vid=fake_vid;
//        sendView();
//    }


    public Object up(Event evt) {
        Message msg;
        ViewSyncHeader hdr;
        int type=evt.getType();

        switch(type) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                hdr=(ViewSyncHeader)msg.getHeader(name);
                if(hdr == null)
                    break;
                Address sender=msg.getSrc();
                switch(hdr.type) {
                    case ViewSyncHeader.VIEW_SYNC:
                        handleView(hdr.view, sender);
                        break;
                    case ViewSyncHeader.VIEW_SYNC_REQ:
                        if(!sender.equals(local_addr))
                            sendView();
                        break;
                    default:
                        if(log.isErrorEnabled()) log.error("ViewSyncHeader type " + hdr.type + " not known");
                }
                return null;

            case Event.VIEW_CHANGE:
                View view=(View)evt.getArg();
                handleViewChange(view);
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return up_prot.up(evt);
    }



    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View v=(View)evt.getArg();
                handleViewChange(v);
                break;
        }
        return down_prot.down(evt);
    }



    /* --------------------------------------- Private Methods ---------------------------------------- */

    private void handleView(View v, Address sender) {
        Vector<Address> members=v.getMembers();
        if(!members.contains(local_addr)) {
            if(log.isWarnEnabled())
                log.warn("discarding view as I (" + local_addr + ") am not member of view (" + v + ")");
            return;
        }

        ViewId vid=v.getVid();
        int rc=vid.compareTo(my_vid);
        if(rc > 0) { // foreign view is greater than my own view; update my own view !
            if(log.isTraceEnabled())
                log.trace("view from " + sender + " (" + vid + ") is greater than my own view (" + my_vid + ");" +
                " will update my own view");

            Message view_change=new Message(local_addr, local_addr, null);
            org.jgroups.protocols.pbcast.GMS.GmsHeader hdr;
            hdr=new org.jgroups.protocols.pbcast.GMS.GmsHeader(org.jgroups.protocols.pbcast.GMS.GmsHeader.VIEW, v);
            view_change.putHeader(GMS.name, hdr);
            up_prot.up(new Event(Event.MSG, view_change));
            num_views_adjusted++;
        }
    }

    private void handleViewChange(View view) {
        Vector<Address> tmp=view.getMembers();
        if(tmp != null) {
            mbrs.clear();
            mbrs.addAll(tmp);
        }
        my_view=(View)view.clone();
        my_vid=my_view.getVid();
        if(my_view.size() > 1) {
            startViewSender();
        }
        else {
            stopViewSender();
        }
    }

    private void sendView() {
        View tmp=(View)(my_view != null? my_view.clone() : null);
        if(tmp == null) return;
        Message msg=new Message(null); // send to the group
        msg.setFlag(Message.OOB);
        ViewSyncHeader hdr=new ViewSyncHeader(ViewSyncHeader.VIEW_SYNC, tmp);
        msg.putHeader(name, hdr);
        down_prot.down(new Event(Event.MSG, msg));
        num_views_sent++;
    }

    /** Starts with view_task_lock held, no need to acquire it again */
    void startViewSender() {                   
        try {
        	view_task_lock.lock();
        	if(view_send_task_future == null || view_send_task_future.isDone()) {
	            ViewSendTask view_send_task=new ViewSendTask();
	            view_send_task_future=timer.scheduleWithDynamicInterval(view_send_task, true); // fixed-rate scheduling
	            if(log.isTraceEnabled())
	                log.trace("view send task started");
        	}
        }
        finally {
            view_task_lock.unlock();
        }
    }


    void stopViewSender() {        
        try {
        	view_task_lock.lock();
            if(view_send_task_future != null) {
                view_send_task_future.cancel(false);
                view_send_task_future=null;
                if(log.isTraceEnabled())
                    log.trace("view send task stopped");
            }
        }
        finally {
            view_task_lock.unlock();
        }
    }






    /* ------------------------------------End of Private Methods ------------------------------------- */







    public static class ViewSyncHeader extends Header implements Streamable {
        public static final int VIEW_SYNC     = 1; // contains a view
        public static final int VIEW_SYNC_REQ = 2; // request to all members to send their views

        int   type=0;
        View  view=null;

        public ViewSyncHeader() {
        }


        public ViewSyncHeader(int type, View view) {
            this.type=type;
            this.view=view;
        }

        public int getType() {
            return type;
        }

        public View getView() {
            return view;
        }

        static String type2String(int t) {
            switch(t) {
                case VIEW_SYNC:
                    return "VIEW_SYNC";
                case VIEW_SYNC_REQ:
                    return "VIEW_SYNC_REQ";
                default:
                    return "<unknown>";
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder("[").append(type2String(type)).append("]");
            if(view != null)
                sb.append(", view= ").append(view);
            return sb.toString();
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
            if(view == null) {
                out.writeBoolean(false);
                return;
            }
            out.writeBoolean(true);
            view.writeExternal(out);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
            boolean available=in.readBoolean();
            if(available) {
                view=new View();
                view.readExternal(in);
            }
        }

        public int size() {
            int retval=Global.INT_SIZE + Global.BYTE_SIZE + Global.BYTE_SIZE; // type + view type + presence for digest
            if(view != null)
                retval+=view.serializedSize();
            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(type);
            // 0 == null, 1 == View, 2 == MergeView
            byte b=(byte)(view == null? 0 : (view instanceof MergeView? 2 : 1));
            out.writeByte(b);
            Util.writeStreamable(view, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readInt();
            byte b=in.readByte();
            Class<?> clazz=b == 2? MergeView.class : View.class;
            view=(View)Util.readStreamable(clazz, in);
        }


    }




    /**
     Periodically multicasts a View_SYNC message
     */
    private class ViewSendTask implements TimeScheduler.Task {

        public long nextInterval() {
            long interval=computeSleepTime();
            if(interval <= 0)
                return 10000;
            else
                return interval;
        }


        public void run() {
            sendView();
        }

        long computeSleepTime() {
            int num_mbrs=Math.max(mbrs.size(), 1);
            return getRandom((num_mbrs * avg_send_interval * 2));
        }

        long getRandom(long range) {
            return (long)((Math.random() * range) % range);
        }
    }



}
