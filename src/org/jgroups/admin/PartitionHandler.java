package org.jgroups.admin;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.FastArray;
import org.jgroups.util.NameCache;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Util;

import java.util.*;
import java.util.function.BiConsumer;

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
    public static final String GET_VIEW="get-view"; // fetches the views of all members, e.g. "A,B,C,D,E"
    public static final String GET_VIEW_RSP="get-view-rsp"; // View

    protected final ResponseCollector<View> rsp_coll=new ResponseCollector<>();
    protected long                          timeout=5000; // ms to wait for all GET_VIEW_RSP responses

    public PartitionHandler(JChannel ch) {
        super(ch);
    }

    public long             timeout()       {return timeout;}
    public PartitionHandler timeout(long t) {this.timeout=t; return this;}

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
     * Fetches the view from all members (GET_VIEW) of 'view', collects all responses (GET_VIEW_RSP), creates a
     * {@link MergeView} and installs it in all members of 'view' (INSTALL_MERGE_VIEW).
     * @param view
     */
    public void injectMergeView(String view) {
        final List<Address> members=new FastArray<>();
        forEachMemberDo(view, (addr,name) -> members.add(addr));
        rsp_coll.reset(members);
        for(Address mbr: members)
            sendRequest(KEY, new ObjectMessage(mbr), GET_VIEW);
        boolean success=rsp_coll.waitForAllResponses(timeout);
        if(!success) {
            ch.log().error("%s failed fetching all responses from %s: rsps:\n%s\n", ch.address(), view, rsp_coll);
            return;
        }
        MergeView mv=createMergeView(rsp_coll.getResults());
        for(Address target: mv)
            sendRequest(KEY, new ObjectMessage(target, mv), INJECT_VIEW);
    }

    @Override
    public void handleRequest(Message msg, AdminHeader hdr) {
        String cmd=hdr.command();
        switch(cmd) {
            case INJECT_VIEW:
                View v=msg.getObject();
                handleInjectViewRequest(v);
                break;
            case GET_VIEW:
                v=ch.view();
                sendResponse(KEY, new ObjectMessage(msg.src(), v), GET_VIEW_RSP);
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

    protected void forEachMemberDo(String view, BiConsumer<Address,String> c) {
        List<String> views=Util.parseStringList(view, "||");
        Map<Address, String> cache=NameCache.getContents(true), all_cache=NameCache.getContents(false);
        for(String v: views) {
            List<String> addrs=Util.parseCommaDelimitedStrings(v);
            for(String addr: addrs) {
                String a=addr.trim();
                Address target=getByValue(cache, a);
                // Find a non-removed address first, before finding a removed address, e.g. in a split cluster
                if(target == null && (target=getByValue(all_cache, a)) == null) {
                    ch.log().warn("%s: failed to find address for \"%s\"; cannot send %s", ch.address(), a, INJECT_VIEW);
                    continue;
                }
                c.accept(target, v);
            }
        }
    }

    protected void handleInjectViewRequest(View view) {
        GMS gms=ch.stack().findProtocol(GMS.class);
        gms.installView(view);
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

    protected MergeView createMergeView(Map<Address,View> m) {
        Set<View> tmp=new HashSet<>(m.values()); // remove duplicate views (based on view id)
        List<Collection<Address>> views=new FastArray<>();
        for(View view: tmp)
            views.add(view.getMembers());
        GMS gms=ch.stack().findProtocol(GMS.class);
        List<Address> new_mbrship=gms.computeNewMembership(views);
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        long highest_id=m.values().stream().map(v -> v.getViewId().getId()).max(Long::compare).get();
        return new MergeView(new_mbrship.get(0), ++highest_id, new_mbrship, new FastArray<>(tmp));
    }

    protected static Address getByValue(Map<Address,String> cache, String name) {
        for(Map.Entry<Address,String> e: cache.entrySet()) {
            String s=e.getValue();
            if(s.equals(name))
                return e.getKey();
        }
        return null;
    }
}
