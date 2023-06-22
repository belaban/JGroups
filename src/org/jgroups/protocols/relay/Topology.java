package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BiConsumer;

/**
 * Provides a cache of all sites and their members (addresses, IP addresses, site masters etc) in a network of
 * autonomous sites. The cache is an approximation, and is refreshed on reception of {@link Relay2Header#SITES_UP}
 * or {@link Relay2Header#SITES_DOWN} notifications. A refresh can also be triggered programmatically.
 * <br/>
 * Used as a component in {@link RELAY2}.
 * @author Bela Ban
 * @since  5.2.15
 */
public class Topology {
    protected final RELAY2                      relay;
    protected final Map<String,Set<MemberInfo>> cache=new ConcurrentHashMap<>(); // cache of sites and members
    protected BiConsumer<String,MemberInfo>     rsp_handler;


    public Topology(RELAY2 relay) {
        this.relay=Objects.requireNonNull(relay);
    }

    public Map<String,Set<MemberInfo>> cache() {return cache;}

    /**
     * Sets a response handler
     * @param c The response handler. Arguments are the site and MemberInfo of the member from which we received the rsp
     */
    public Topology setResponseHandler(BiConsumer<String,MemberInfo> c) {rsp_handler=c; return this;}


    @ManagedOperation(description="Fetches information (site, address, IP address) from all members")
    public Topology refresh() {
        return refresh(null);
    }

    /**
     * Refreshes the topology for a given site.
     * @param site The site. If null, all sites will be refreshed.
     */
    @ManagedOperation(description="Fetches information (site, address, IP address) from all members of a given site")
    public Topology refresh(String site) {
        Address dest=new SiteMaster(site); // sent to all site masters if site == null
        Message topo_req=new EmptyMessage(dest).putHeader(relay.getId(), new Relay2Header(Relay2Header.TOPO_REQ));
        relay.down(topo_req);
        return this;
    }

    @ManagedOperation(description="Prints the cache information about all members")
    public String print() {
        return print(null);
    }

    /**
     * Dumps the members for a given site, or all sites
     * @param site The site name. Dumps all sites if null
     * @return A string of all sites and their members
     */
    @ManagedOperation(description="Prints the cache information about all members")
    public String print(String site) {
        if(site != null)
            return dumpSite(site);
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,Set<MemberInfo>> e: cache.entrySet()) {
            sb.append(dumpSite(e.getKey()));
        }
        return sb.toString();
    }

    public Topology removeAll(Collection<String> sites) {
        if(sites == null)
            cache.keySet().clear();
        else
            cache.keySet().removeAll(sites);
        return this;
    }

    public Topology adjust(String site, Collection<Address> mbrs) {
        Set<MemberInfo> list=cache.get(site);
        if(list != null && mbrs != null)
            list.removeIf(mi -> !mbrs.contains(mi.addr));
        return this;
    }

    @Override
    public String toString() {
        return String.format("%d sites", cache.size());
    }

    protected String dumpSite(String site) {
        Set<MemberInfo> members=cache.get(site);
        if(members == null)
            return String.format("%s: no members found", site);
        StringBuilder sb=new StringBuilder(site).append("\n");
        for(MemberInfo mi: members) {
            sb.append("  ").append(mi.toStringNoSite()).append("\n");
        }
        return sb.toString();
    }

    /** Called when a response has been received. Updates the internal cache */
    protected void handleResponse(Members rsp) {
        String site=rsp.site;
        Set<MemberInfo> infos=cache.computeIfAbsent(site,s -> new ConcurrentSkipListSet<>());
        if(rsp.joined != null) {
            infos.addAll(rsp.joined);
            if(rsp_handler != null)
                for(MemberInfo mi: rsp.joined)
                    rsp_handler.accept(site, mi);
        }

        if(rsp.left != null) {
            // todo: implement
        }
    }

    @FunctionalInterface
    public interface ResponseHandler {
        void handle(String site, MemberInfo rsp);
    }

    /** Contains information about joined and left members for a given site */
    public static class Members implements SizeStreamable {
        protected String           site;
        protected List<MemberInfo> joined;
        protected List<Address>    left;

        public Members() {}
        public Members(String site) {this.site=site;}

        public Members addJoined(MemberInfo mi) {
            if(this.joined == null)
                this.joined=new ArrayList<>();
            this.joined.add(Objects.requireNonNull(mi));
            return this;
        }

        public Members addLeft(Address left) {
            if(this.left == null)
                this.left=new ArrayList<>();
            this.left.add(Objects.requireNonNull(left));
            return this;
        }

        public int serializedSize() {
            int size=Util.size(site) + Short.BYTES * 2;
            if(joined != null && !joined.isEmpty()) {
                for(MemberInfo mi: joined)
                    size+=mi.serializedSize();
            }
            if(left != null && !left.isEmpty()) {
                for(Address addr: left)
                    size+=Util.size(addr);
            }
            return size;
        }

        public void writeTo(DataOutput out) throws IOException {
            Bits.writeString(site, out);
            if(joined == null || joined.isEmpty())
                out.writeShort(0);
            else {
                out.writeShort(joined.size());
                for(MemberInfo mi: joined)
                    mi.writeTo(out);
            }
            if(left == null || left.isEmpty())
                out.writeShort(0);
            else {
                out.writeShort(left.size());
                for(Address addr: left)
                    Util.writeAddress(addr, out);
            }
        }

        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            site=Bits.readString(in);
            short len=in.readShort();
            if(len > 0) {
                joined=new ArrayList<>(len);
                for(int i=0; i < len; i++) {
                    MemberInfo mi=new MemberInfo();
                    mi.readFrom(in);
                    joined.add(mi);
                }
            }
            len=in.readShort();
            if(len > 0) {
                left=new ArrayList<>(len);
                Address addr=Util.readAddress(in);
                left.add(addr);
            }
        }

        public String toString() {
            return String.format("site %s: %d members joined %d left", site, joined == null? 0 : joined.size(),
                                 left == null? 0 : left.size());
        }
    }

    public static class MemberInfo implements SizeStreamable, Comparable<MemberInfo> {
        protected String    site;
        protected Address   addr;
        protected IpAddress ip_addr;
        protected boolean   site_master;

        public MemberInfo() {
        }

        public MemberInfo(String site, Address addr, IpAddress ip_addr, boolean site_master) {
            this.site=site;
            this.addr=Objects.requireNonNull(addr);
            this.ip_addr=ip_addr;
            this.site_master=site_master;
        }

        @Override
        public int hashCode() {
            return addr.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof MemberInfo && this.compareTo((MemberInfo)obj) == 0;
        }

        @Override
        public int compareTo(MemberInfo o) {
            return addr.compareTo(o.addr);
        }

        @Override
        public int serializedSize() {
            return Util.size(site) + Util.size(addr) + Util.size(ip_addr) + Global.BYTE_SIZE;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeString(site, out);
            Util.writeAddress(addr, out);
            Util.writeAddress(ip_addr, out);
            out.writeBoolean(site_master);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            site=Bits.readString(in);
            addr=Util.readAddress(in);
            ip_addr=(IpAddress)Util.readAddress(in);
            site_master=in.readBoolean();
        }

        @Override
        public String toString() {
            return String.format("site=%s, addr=%s (ip=%s%s)", site, addr, ip_addr, site_master? ", sm" : "");
        }

        public String toStringNoSite() {
            return String.format("%s (ip=%s%s)", addr, ip_addr, site_master? ", sm" : "");
        }
    }
}
