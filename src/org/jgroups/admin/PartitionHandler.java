package org.jgroups.admin;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Injects partitions into a cluster (via injectView()). The syntax is string based, and partitions are separated
 * by '||'. Example: if we have member A,B,C,D,E, we can inject a partition with injectView("A,B,C || D,E"). To
 * heal this, injectView("A,B,C,D") can be used. Note that view IDs will be incremented by 1, but no MergeViews
 * will be installed.
 * @author Bela Ban
 * @since  5.6.0
 */
public class PartitionHandler extends AdminHandler {
    public static final String KEY="partition";
    public static final String INJECT_VIEW="inject-view"; // "A,B,C || D,E"
    public static final String INJECT_MERGE_VIEW="inject-merge-view";
    public static final String INJECT_MERGE_EVENT="inject-merge-event"; // Map<Address,View> injected into GMS
    public static final String GET_VIEW="get-view"; // fetches the views of all members, e.g. "A,B,C,D,E"
    public static final String GET_VIEW_RSP="get-view-rsp"; // JoinRsp

    protected final ResponseCollector<View> rsp_coll=new ResponseCollector<>(); // collects view:digest tuples
    protected long                          timeout=5000; // ms to wait for all GET_VIEW_RSP responses

    public PartitionHandler(JChannel ch) {
        super(ch);
    }

    public long             timeout()       {return timeout;}
    public PartitionHandler timeout(long t) {this.timeout=t; return this;}

    @Override
    public String executeRequest(String req) {
        String command=Util.parse(req, 0);
        String rest=req.substring(command.length());
        switch(command) {
            case INJECT_VIEW:
                injectView(rest);
                break;
            case INJECT_MERGE_VIEW:
                injectMergeView(rest);
                break;
            default:
                ch.log().error("%s: command %s not known", ch.address(), command);
        }
        return null;
    }

    /** Injects non-overlapping views into members. Example: "A,B,C || D,E" injects view {A,B,C} into A, B and C, and
     * {D,E} into D and E. "A,B,C,D,E" injects view {A,B,C,D,E} into all members, healing the partition.
     * @param views The partition to be injected
     */
    public void injectView(String views) {
        List<String> tmp=Util.parseStringList(views, "||");
        for(String view: tmp) {
            View v=createView(view);
            if(v == null)
                continue;
            for(Address target: v)
                sendRequest(KEY, new ObjectMessage(target, v), INJECT_VIEW);
        }
    }

    /**
     * Fetches the views from all members (GET_VIEW), collects all responses (GET_VIEW_RSP), creates a map of addresses
     * and views and sends that map to all members, which inject that as a MERGE event into GMS
     */
    public void injectMergeView(String view) {
        final List<Address> members=getMembers(view);
        rsp_coll.reset(members);
        for(Address mbr: members)
            sendRequest(KEY, new ObjectMessage(mbr), GET_VIEW);
        boolean success=rsp_coll.waitForAllResponses(timeout);
        if(!success) {
            ch.log().error("%s failed fetching all responses from %s: rsps:\n%s\n", ch.address(), view, rsp_coll);
            return;
        }
        Map<Address,View> views=rsp_coll.getResults();
        for(Address mbr: members)
            sendRequest(KEY, new ObjectMessage(mbr, new ViewInfo(views)), INJECT_MERGE_EVENT);
    }

    @Override
    public void handleRequest(Message msg, AdminHeader hdr) {
        String cmd=hdr.command();
        switch(cmd) {
            case INJECT_VIEW:
                View v=msg.getObject();
                GMS gms=ch.stack().findProtocol(GMS.class);
                gms.installView(v, null);
                break;
            case GET_VIEW:
                v=ch.view();
                sendResponse(KEY, new ObjectMessage(msg.src(), v), GET_VIEW_RSP);
                break;
            case INJECT_MERGE_EVENT:
                ViewInfo merge_event=msg.getObject();
                gms=ch.stack().findProtocol(GMS.class);
                gms.up(new Event(Event.MERGE, merge_event.views));
                break;
            default:
                ch.log().error("%s: command %s not known", ch.address(), cmd);
        }
    }

    @Override
    public void handleResponse(Message msg, AdminHeader hdr) {
        switch(hdr.command) {
            case GET_VIEW_RSP:
                View v=msg.getObject();
                rsp_coll.add(msg.src(), v);
                break;
            default:
                ch.log().error("%s: command %s is unknown", ch.address(), hdr.command());
        }
    }

    protected List<Address> getMembers(String view) {
        Map<Address, String> cache=NameCache.getContents(true), all_cache=NameCache.getContents(false);
        List<String> addrs=Util.parseCommaDelimitedStrings(view.trim());
        List<Address> retval=new FastArray<>();
        for(String addr: addrs) {
            String a=addr.trim();
            Address target=getByValue(cache, a);
            // Find a non-removed address first, before finding a removed address, e.g. in a split cluster
            if(target == null && (target=getByValue(all_cache, a)) == null) {
                ch.log().warn("%s: failed to find address for \"%s\"; cannot send %s", ch.address(), a, INJECT_VIEW);
                continue;
            }
            retval.add(target);
        }
        return retval;
    }

    protected View createView(String mbrs) { // mbrs: A,B,C
        Map<Address,String> cache=NameCache.getContents(true), all_cache=NameCache.getContents(false);
        List<Address> l=new FastArray<>();
        List<String> addrs=Util.parseCommaDelimitedStrings(mbrs);
        for(String addr: addrs) {
            String a=addr.trim();
            Address target=getByValue(cache, a);
            // Find a non-removed address first, before finding a removed address, e.g. in a split cluster
            if(target == null && (target=getByValue(all_cache, a)) == null) {
                ch.log().warn("%s: failed to find address for \"%s\"", ch.address(), a);
                continue; // return instead?
            }
            l.add(target);
        }
        if(l.isEmpty())
            return null;
        long new_vid=ch.view().getViewId().getId()+1;
        return View.create(l.get(0), new_vid, l);
    }

    protected static Address getByValue(Map<Address,String> cache, String name) {
        for(Map.Entry<Address,String> e: cache.entrySet()) {
            String s=e.getValue();
            if(s.equals(name))
                return e.getKey();
        }
        return null;
    }

    protected static class ViewInfo implements Streamable {
        protected Map<Address,View> views=new HashMap<>();

        public ViewInfo() {
        }

        public ViewInfo(Map<Address,View> views) {
            this.views=views;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(views.size());
            for(Map.Entry<Address,View> e: views.entrySet()) {
                Util.writeAddress(e.getKey(), out);
                Util.writeView(e.getValue(), out);
            }
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            int size=in.readInt();
            for(int i=0; i < size; i++) {
                Address addr=Util.readAddress(in);
                View v=Util.readView(in);
                views.put(addr, v);
            }
        }
    }
}
