package org.jgroups.admin;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.FastArray;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

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
    public static final String INJECT_VIEW="inject-view"; // list of strings, e.g. "A,B,C" "D,E"

    public PartitionHandler(JChannel ch) {
        super(ch);
    }

    /** Injects non-overlapping views into members. Example: "A,B,C || D,E" injects view {A,B,C} into A, B and C, and
     * {D,E} into D and E. "A,B,C,D,E" injects view {A,B,C,D,E} into all members, healing the partition.
     * @param view The partition to be injected
     */
    public void injectView(String view) {
        List<String> views=Util.parseStringList(view, "||");
        Map<Address, String> cache=NameCache.getContents();
        for(String v: views) {
            List<String> addrs=Util.parseCommaDelimitedStrings(v);
            for(String addr: addrs) {
                String a=addr.trim();
                Address target=getByValue(cache, a);
                if(target == null) {
                    ch.log().warn("%s: failed to find address for \"%s\"; cannot send %s", ch.address(), a, INJECT_VIEW);
                    continue;
                }
                Message msg=new ObjectMessage(target, String.format("%s %s", INJECT_VIEW, v));
                sendRequest(KEY, msg);
            }
        }
    }

    @Override
    public void handleRequest(Message msg, AdminHeader hdr) {
        String s=msg.getObject();
        String cmd=Util.parse(s, 0);

        switch(cmd) {
            case INJECT_VIEW:
                String tmp=s.substring(cmd.length()).trim();
                handleInjectViewRequest(tmp);
                break;
            default:
                ch.log().error("%s: command %s not known", ch.address(), cmd);
        }
    }

    @Override
    public void handleResponse(Message msg, AdminHeader hdr) {

    }

    protected void handleInjectViewRequest(String view) {
        List<String> l=Util.parseCommaDelimitedStrings(view);
        Map<Address,String> cache=NameCache.getContents();
        List<Address> members=new FastArray<>(l.size());
        for(String mbr: l) {
            Address m=getByValue(cache, mbr);
            if(m == null) {
                ch.log().warn("%s: failed to find address for \"%s\"; name cache: %s", ch.address(), mbr, cache);
                continue;
            }
            members.add(m);
        }
        if(members.isEmpty())
            return;
        long new_id=ch.view().getViewId().getId()+1;
        View v=new View(members.get(0), new_id, members);
        GMS gms=ch.stack().findProtocol(GMS.class);
        gms.installView(v);
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
