package org.jgroups.admin;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Injects partitions into a cluster (via injectViews()). The syntax is string based, and partitions are separated
 * by '||'. Example: if we have member A,B,C,D,E, we can inject a partition with injectViews("A,B,C || D,E"). To
 * heal this, injectViews("A,B,C,D") can be used. Note that view IDs will be incremented by 1, but no MergeViews
 * will be installed.
 * @author Bela Ban
 * @since  5.6.0
 */
public class PartitionHandler extends AdminHandler {
    public static final String KEY="partition";
    public static final String INJECT_VIEW="inject-view"; // "A,B,C || D,E"
    public static final String INJECT_VIEW_INTO = "inject-view-into"; // "D D,E": installs {D,E} only to D
    public static final String DISCARD_FROM = "discard-from"; // "A,B": this node drops every message from A and B
    public static final String STOP_DISCARD = "stop-discard"; // This node accepts messages from every member again
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
                injectViews(rest);
                break;
            case INJECT_VIEW_INTO:
                String[] args = rest.trim().split("\\s+", 2);
                if (args.length < 2) {
                    ch.log().error("%s: %s needs a target and the view's members, e.g., \"D D,E\"",
                          ch.address(), INJECT_VIEW_INTO);
                    break;
                }
                injectView(args[0], args[1]);
                break;
            case INJECT_MERGE_VIEW:
                injectMergeView(rest);
                break;
            case DISCARD_FROM:
                discardFrom(rest);
                break;
            case STOP_DISCARD:
                stopDiscard();
                break;
            default:
                ch.log().error("%s: command %s not known", ch.address(), command);
        }
        return null;
    }

    /**
     * Drops every message this member receives from the given members.
     *
     * <p>
     * Simulates a one-way partition. This member stops hearing from the given members, while they still receive
     * messages from this member. Repeated calls add to the set of dropped members. The effect lasts until
     * {@link #stopDiscard()} is called.
     * </p>
     *
     * <p>
     * Names that do not resolve to a known member are skipped. If the {@link DISCARD} protocol cannot be added to the
     * stack, an error is logged and no messages are dropped.
     * </p>
     *
     * @param members comma-separated logical names of the members to drop messages from, e.g. {@code "A,B"}.
     */
    public void discardFrom(String members) {
        DISCARD discard = ch.stack().findProtocol(DISCARD.class);
        if (discard == null) {
            discard = new DISCARD();
            try {
                ch.stack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
            } catch (Exception e) {
                ch.log().error("%s: failed inserting %s: %s", ch.address(), DISCARD.class, e);
                return;
            }
        }

        List<Address> mbrs = getMembers(members);
        discard.addIgnoredMembers(mbrs.toArray(Address[]::new));
    }

    /**
     * Makes this member accept messages from every member again.
     *
     * <p>
     * Reverts all previous {@link #discardFrom(String)} calls, so this member hears from every member again.
     * Does nothing if this member was never set to drop messages.
     * </p>
     */
    public void stopDiscard() {
        DISCARD discard = ch.stack().findProtocol(DISCARD.class);
        if (discard == null)
            return;

        discard.resetIgnoredMembers();
    }

    /**
     * Installs a view in a single member, leaving the other members of that view untouched.
     *
     * <p>
     * Creates views that differ between members. For example, target {@code "D"} with members {@code "D,E"} installs
     * {@code {D,E}} in D only, while E keeps its current view.
     * </p>
     *
     * <p>
     * Nothing is sent if the target or any of the view's members cannot be resolved.
     * </p>
     *
     * @param target logical name of the member that installs the view.
     * @param members comma-separated logical names of the view's members, which must include the target.
     */
    public void injectView(String target, String members) {
        View view = createView(members);
        List<Address> t = getMembers(target);
        if (view != null && !t.isEmpty())
            injectView(t.get(0), view);
    }

    /**
     * Installs the given view in a single member.
     *
     * <p>
     * The request is sent without waiting for confirmation. If the target is not a member of the view, nothing is sent.
     * </p>
     *
     * @param target the member that installs the view.
     * @param view the view to install, which must contain the target.
     */
    public void injectView(Address target, View view) {
        if (!view.containsMember(target)) {
            // GMS would drop it anyways.
            // A member never install a view it is not part of.
            ch.log().warn("%s: %s is not part of %s; not injecting it", ch.address(), target, view);
            return;
        }
        sendRequest(KEY, new ObjectMessage(target, view), INJECT_VIEW);
    }

    /** Injects non-overlapping views into members. Example: "A,B,C || D,E" injects view {A,B,C} into A, B and C, and
     * {D,E} into D and E. "A,B,C,D,E" injects view {A,B,C,D,E} into all members, healing the partition.
     * @param views The partition to be injected
     */
    public void injectViews(String views) {
        List<String> tmp=Util.parseStringList(views, "||");
        List<View> l=tmp.stream().map(this::createView).filter(Objects::nonNull).toList();
        if(!l.isEmpty())
            injectViews(l);
    }

    public void injectViews(List<View> views) {
        for(View view: views) {
            for(Address target: view)
                sendRequest(KEY, new ObjectMessage(target, view), INJECT_VIEW);
        }
    }

    /**
     * Fetches the views from all members (GET_VIEW), collects all responses (GET_VIEW_RSP), creates a map of addresses
     * and views and sends that map to all members, which inject that as a MERGE event into GMS
     */
    public void injectMergeView(String view) {
        final List<Address> members=getMembers(view);
        injectMergeView(members);
    }

    public void injectMergeView(List<Address> members) {
        rsp_coll.reset(members);
        for(Address mbr: members)
            sendRequest(KEY, new ObjectMessage(mbr), GET_VIEW);
        boolean success=rsp_coll.waitForAllResponses(timeout);
        if(!success) {
            ch.log().error("%s failed fetching all responses from %s: rsps:\n%s\n", ch.address(), members, rsp_coll);
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
        Map<Address,String> cache=NameCache.getContents(true), all_cache=NameCache.getContents(false);
        List<String> addrs=Util.parseCommaDelimitedStrings(view.trim());
        List<Address> retval=new FastArray<>();
        for(String addr: addrs) {
            String a=addr.trim();
            Address target = Objects.equals(a, ch.name()) ? ch.address() : getByValue(cache, a);
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
            Address target = Objects.equals(a, ch.name()) ? ch.address() : getByValue(cache, a);
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
