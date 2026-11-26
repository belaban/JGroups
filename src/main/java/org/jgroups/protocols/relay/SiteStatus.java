package org.jgroups.protocols.relay;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Maintains the status of sites (up, down, undefined). This weeds out duplicate up or down notifications
 * @author Bela Ban
 * @since  5.2.17
 */
public class SiteStatus {
    public enum Status {up,down};
    protected final Map<String,Status> sites=new HashMap<>();

    /**
     * Adds a set of sites to the cache. Returns a set of sites for which notifications should be emitted. For each
     * site S, the following happens:
     * <pre>
     *  - S is not present: add a new entry with the given status for S and add S to the return value
     *  - S is present: if S != status: change the status and add S to the return value, else no-op
     * </pre>
     * @param sites
     * @param status
     * @return
     */
    public synchronized Set<String> add(Set<String> sites, Status status) {
        Set<String> retval=new HashSet<>();
        for(String site: sites) {
            Status s=this.sites.get(site);
            if(s == null) {
                this.sites.put(site, status);
                retval.add(site);
            }
            else {
                if(s != status) {
                    this.sites.put(site, status);
                    retval.add(site);
                }
            }
        }
        return retval;
    }

    public synchronized Status get(String site) {
        return this.sites.get(site);
    }

    public synchronized SiteStatus clear() {
        sites.clear();
        return this;
    }

    public String toString() {
        return sites.entrySet().stream()
          .map(e -> String.format("%s: %s", e.getKey(), e.getValue())).collect(Collectors.joining("\n"));
    }
}
