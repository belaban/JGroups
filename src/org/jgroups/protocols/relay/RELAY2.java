package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;

/**
 *
 * Design: ./doc/design/RELAY2.txt and at https://github.com/belaban/JGroups/blob/master/doc/design/RELAY2.txt.<p/>
 * JIRA: https://issues.jboss.org/browse/JGRP-1433
 * @author Bela Ban
 * @since 3.2
 */
@Experimental
@MBean(description="RELAY2 protocol")
public class RELAY2 extends Protocol {

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Name of the site (needs to be defined in the configuration)",writable=false)
    protected String site;

    @Property(description="Name of the relay configuration",writable=false)
    protected String config;

    @Property(description="Whether or not to relay multicast messages (dest=null)")
    protected boolean relay_multicasts=false;

    

    /* ---------------------------------------------    Fields    ------------------------------------------------ */
    @ManagedAttribute(description="My site-ID")
    protected short                              site_id=-1;

    protected Map<String,RelayConfig.SiteConfig> sites;

    protected RelayConfig.SiteConfig             site_config;

    @ManagedAttribute(description="Whether this member is the coordinator")
    protected volatile boolean                   is_coord=false;

    protected volatile Address                   coord;

    protected Relayer                            relayer;

    protected volatile Address                   local_addr;


    public void init() throws Exception {
        super.init();
        if(site == null)
            throw new IllegalArgumentException("site cannot be null");
        if(config == null)
            throw new IllegalArgumentException("config cannot be null");
        parseSiteConfiguration();

        // Sanity check
        Collection<Short> site_ids=new TreeSet<Short>();
        for(RelayConfig.SiteConfig site_config: sites.values()) {
            site_ids.add(site_config.getId());
            if(site.equals(site_config.getName()))
                site_id=site_config.getId();
        }

        int index=0;
        for(short id: site_ids) {
            if(id != index)
                throw new Exception("site IDs need to start with 0 and are required to increase monotonically; " +
                                      "site IDs=" + site_ids);
            index++;
        }

        if(site_id < 0)
            throw new IllegalArgumentException("site_id could not be determined from site \"" + site + "\"");
    }

    public void stop() {
        super.stop();
        is_coord=false;
        if(log.isTraceEnabled())
            log.trace("I ceased to be site master; closing bridges");
        if(relayer != null)
            relayer.stop();
    }

    /**
     * Parses the configuration by reading the config file. Can be used to re-read a configuration.
     * @throws Exception
     */
    @ManagedOperation(description="Reads the configuration file and build the internal config information.")
    public void parseSiteConfiguration() throws Exception {
        InputStream input=null;
        try {
            input=ConfiguratorFactory.getConfigStream(config);
            sites=RelayConfig.parse(input);
            for(RelayConfig.SiteConfig site_config: sites.values())
                SiteUUID.addToCache(site_config.getId(), site_config.getName());
            site_config=sites.get(site);
            if(site_config == null)
                throw new Exception("site configuration for \"" + site + "\" not found in " + config);
            if(log.isTraceEnabled())
                log.trace("site configuration:\n" + site_config);
        }
        finally {
            Util.close(input);
        }
    }

    @ManagedOperation(description="Prints the contents of the routing table. " +
      "Only available if we're the current coordinator (site master)")
    public String printRoutes() {
        return relayer != null? relayer.printRoutes() : "n/a (not site master)";
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest == null || !(dest instanceof SiteAddress))
                    break;
                SiteAddress target=(SiteAddress)dest;

                byte[] buf=marshal(msg);
                if(buf == null)
                    return null; // don't pass down

                Address src=msg.getSrc();
                SiteAddress sender=null;
                if(src instanceof SiteMaster)
                    sender=new SiteMaster(((SiteMaster)src).getSite());
                else
                    sender=new SiteUUID((UUID)local_addr, UUID.get(local_addr), site_id);

                // target is in the same site; we can deliver the message locally
                if(target.getSite() == site_id ) {
                    if(local_addr.equals(target) || ((target instanceof SiteMaster) && is_coord))
                        deliver(target, sender, buf);
                    else
                        deliverLocally(target, sender, buf);
                    return null;
                }

                // forward to the coordinator unless we're the coord (then route the message directly)
                if(!is_coord)
                    forwardTo(coord, target, sender, buf);
                else
                    route(target, sender, buf);
                return null;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Relay2Header hdr=(Relay2Header)msg.getHeader(id);
                if(hdr == null)
                    break;

                System.out.println("[" + local_addr + "] received message with Relay2Header " + hdr);
                handleMessage(hdr, msg);
                return null;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    /** Called to handle a received relay message */
    protected void handleMessage(Relay2Header hdr, Message msg) {
        System.out.println("**** handleMessage(): to=" + hdr.final_dest + ", from=" + hdr.original_sender + ", " + msg.getLength() + " bytes " +
                             "(forwarder=" + msg.getSrc() + ")");

        route((SiteAddress)hdr.final_dest, (SiteAddress)hdr.original_sender, msg.getBuffer());
    }



    /**
     * Routes the message to the target destination, used by a site master (coordinator)
     * @param dest
     * @param buf
     */
    protected void route(SiteAddress dest, SiteAddress sender, byte[] buf) {
        short target_site=dest.getSite();
        if(target_site == site_id) {
            if(local_addr.equals(dest) || ((dest instanceof SiteMaster) && is_coord)) {
                deliver(dest, sender, buf);
            }
            else
                deliverLocally(dest, sender, buf); // send to member in same local site
            return;
        }
        Relayer tmp=relayer;
        if(tmp == null) {
            log.warn("not site master; dropping message");
            return;
        }
        Relayer.Route route=relayer.getRoute(target_site);
        if(route == null) {
            log.warn("route for " + SiteUUID.getSiteName(target_site) + " (" + target_site + ") not found, dropping message to " + dest + " from " + sender);
            return;
        }
        relay(dest, sender, route, buf);
    }

    protected void forwardTo(Address next_dest, SiteAddress final_dest, Address original_sender, byte[] buf) {
        Message msg=new Message(next_dest, buf);
        Relay2Header hdr=new Relay2Header(Relay2Header.DATA, final_dest, original_sender);
        msg.putHeader(id, hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }

    
    protected void deliverLocally(SiteAddress dest, SiteAddress sender, byte[] buf) {
        Address local_dest;
        if(dest instanceof SiteUUID) {
            SiteUUID tmp=(SiteUUID)dest;
            local_dest=new UUID(tmp.getMostSignificantBits(), tmp.getLeastSignificantBits());
        }
        else
            local_dest=dest;

        forwardTo(local_dest, dest, sender, buf);
    }

    protected void deliver(Address dest, Address sender, byte[] buf) {
        try {
            Message original_msg=(Message)Util.streamableFromByteBuffer(Message.class, buf);
            original_msg.setSrc(sender);
            original_msg.setDest(dest);
            up_prot.up(new Event(Event.MSG, original_msg));
        }
        catch(Exception e) {
            log.error("failed unmarshalling message", e);
        }
    }

    protected void relay(SiteAddress to, SiteAddress from, Relayer.Route route, byte[] buf) {
        if(route == null) {
            log.warn("route for site" + to.getSite() + " not found; dropping message");
            return;
        }
        RELAY2.Relay2Header hdr=new RELAY2.Relay2Header(RELAY2.Relay2Header.DATA, to, from);
        Message msg=new Message(route.site_master, buf);
        msg.putHeader(id, hdr);
        try {
            route.bridge.send(msg);
        }
        catch(Exception e) {
            log.error("failure relaying message", e);
        }
    }


    protected byte[] marshal(Message msg) {
        Message tmp=msg.copy(true, Global.BLOCKS_START_ID); // // we only copy headers from building blocks
        // setting dest and src to null reduces the serialized size of the message; we'll set dest/src from the header later
        tmp.setDest(null);
        tmp.setSrc(null);

        try {
            return Util.streamableToByteBuffer(tmp);
        }
        catch(Exception e) {
            log.error("marshalling failure", e);
            return null;
        }
    }



    protected void handleView(View view) {
        Address old_coord=coord, new_coord=Util.getCoordinator(view);
        boolean become_coord=new_coord.equals(local_addr) && (old_coord == null || !old_coord.equals(local_addr));
        boolean cease_coord=old_coord != null && old_coord.equals(local_addr) && !new_coord.equals(local_addr);

        coord=new_coord;

        // This member is the new coordinator: start the Relayer
        if(become_coord) {
            is_coord=true;
            String bridge_name="_" + UUID.get(local_addr);
            relayer=new Relayer(site_config, bridge_name, log, this);
            try {
                if(log.isTraceEnabled())
                    log.trace("I became site master; starting bridges");
                relayer.start();
            }
            catch(Throwable t) {
                log.error("failed starting relayer", t);
            }
        }
        else {
            if(cease_coord) { // ceased being the coordinator (site master): stop the Relayer
                is_coord=false;
                if(log.isTraceEnabled())
                    log.trace("I ceased to be site master; closing bridges");
                if(relayer != null)
                    relayer.stop();
            }
        }
    }


    public static class Relay2Header extends Header {
        public static final byte DATA = 1;

        protected byte    type;
        protected Address final_dest;
        protected Address original_sender;


        public Relay2Header() {
        }

        public Relay2Header(byte type, Address final_dest, Address original_sender) {
            this.type=type;
            this.final_dest=final_dest;
            this.original_sender=original_sender;
        }

        public int size() {
            return Global.BYTE_SIZE + Util.size(final_dest) + Util.size(original_sender);
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeAddress(final_dest, out);
            Util.writeAddress(original_sender, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            final_dest=Util.readAddress(in);
            original_sender=Util.readAddress(in);
        }

        public String toString() {
            return typeToString(type) + " [dest=" + final_dest + ", sender=" + original_sender + "]";
        }

        protected static String typeToString(byte type) {
            switch(type) {
                case DATA: return "DATA";
                default:   return "<unknown>";
            }
        }
    }
}
