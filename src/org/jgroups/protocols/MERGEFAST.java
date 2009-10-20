package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.jgroups.annotations.Experimental;
import org.jgroups.stack.Protocol;

import java.io.*;
import java.util.*;

/**
 * The coordinator attaches a small header with its view to each (or every nth) message. If another coordinator <em>in
 * the same group</em> sees the message, it will initiate the merge protocol immediately by sending a MERGE
 * event up the stack.
 * @author Bela Ban, Aug 25 2003
 */
@Experimental
public class MERGEFAST extends Protocol {
    Address       local_addr=null;
    View          view;
    boolean       is_coord=false;

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                if(is_coord && view != null) {
                    Message msg=(Message)evt.getArg();
                    Address dest=msg.getDest();
                    if(dest == null || dest.isMulticastAddress()) {
                        msg.putHeader(getName(), new MergefastHeader(view));
                    }
                }
                break;
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }



    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                if(is_coord == false) // only handle message if we are coordinator
                    break;
                Message msg=(Message)evt.getArg();
                MergefastHeader hdr=(MergefastHeader)msg.getHeader(getName());
                up_prot.up(evt);
                if(hdr != null && view != null) {
                    if(!Util.sameViewId(view.getViewId(), hdr.view.getViewId())) {
                        Map<Address,View> views=new HashMap<Address,View>();
                        views.put(local_addr, view);
                        views.put(msg.getSrc(), hdr.view);
                        if(log.isDebugEnabled())
                            log.debug("detected different views (" + Util.printViews(views.values()) + "), sending up MERGE event");
                        up_prot.up(new Event(Event.MERGE, views));
                    }
                }
                return null; // event was already passed up
        }
        return up_prot.up(evt);
    }


    protected void handleViewChange(View v) {
        Vector<Address> mbrs=v.getMembers();
        view=v;
        is_coord=mbrs != null && !mbrs.isEmpty() && local_addr.equals(mbrs.firstElement());
    }

    


    public static class MergefastHeader extends Header implements Streamable {
        private View view=null;
        private static final long serialVersionUID=-1017265470273262787L;

        public MergefastHeader() {
        }

        public MergefastHeader(View view) {
            this.view=view;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(view);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            view=(View)in.readObject();
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
