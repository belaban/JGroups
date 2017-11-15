package org.jgroups.protocols.relay;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.util.Util;


/**
 * Class which maintains the destination address for sending messages to a given site, and the bridge channel to do so.
 * @author Bela Ban
 * @since  3.x
 */
public class Route implements Comparable<Route> {
    /** SiteUUID: address of the site master */
    protected final Address  site_master;
    protected final JChannel bridge;
    protected final RELAY2   relay;
    protected final Log      log;
    protected boolean        stats=true;

    public Route(Address site_master, JChannel bridge, RELAY2 relay, Log log) {
        this.site_master=site_master;
        this.bridge=bridge;
        this.relay=relay;
        this.log=log;
    }

    public JChannel bridge()         {return bridge;}
    public Address  siteMaster()     {return site_master;}
    public boolean  stats()          {return stats;}
    public Route    stats(boolean f) {stats=f; return this;}

    public void send(Address final_destination, Address original_sender, final Message msg) {
        if(log.isTraceEnabled())
            log.trace("routing message to " + final_destination + " via " + site_master);
        long start=stats? System.nanoTime() : 0;
        try {
            Message copy=createMessage(site_master, final_destination, original_sender, msg);
            bridge.send(copy);
            if(stats) {
                relay.addToRelayedTime(System.nanoTime() - start);
                relay.incrementRelayed();
            }
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailureRelayingMessage"), e);
        }
    }

    public int compareTo(Route o) {
        return site_master.compareTo(o.siteMaster());
    }

    public boolean equals(Object obj) {
        return compareTo((Route)obj) == 0;
    }

    public int hashCode() {
        return site_master.hashCode();
    }

    public String toString() {
        return (site_master != null? site_master.toString() : "");
    }

    protected Message createMessage(Address target, Address final_destination, Address original_sender, final Message msg) {
        Message copy=relay.copy(msg).setDest(target).setSrc(null);
        RELAY2.Relay2Header hdr=new RELAY2.Relay2Header(RELAY2.Relay2Header.DATA, final_destination, original_sender);
        copy.putHeader(relay.getId(), hdr);
        return copy;
    }
}
