package org.jgroups.protocols.relay;


import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.util.Headers;
import org.jgroups.util.Util;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import static org.jgroups.protocols.relay.RelayHeader.DATA;


/**
 * Class which maintains the destination address for sending messages to a given site, and the bridge channel to do so.
 * @author Bela Ban
 * @since  3.x
 */
public class Route implements Comparable<Route> {
    /** SiteUUID: address of the site master */
    protected final Address  site_master;
    protected final JChannel bridge;
    protected final RELAY    relay;
    protected final Log      log;
    protected boolean        stats=true;
    protected final boolean  relay3;

    public Route(Address site_master, JChannel bridge, RELAY relay, Log log) {
        this.site_master=site_master;
        this.bridge=bridge;
        this.relay=relay;
        this.log=log;
        this.relay3=Objects.requireNonNull(relay) instanceof RELAY3;
    }

    public JChannel bridge()         {return bridge;}
    public Address  siteMaster()     {return site_master;}
    public boolean  stats()          {return stats;}
    public Route    stats(boolean f) {stats=f; return this;}

    public void send(Address final_destination, Address original_sender, final Message msg) {
        send(final_destination, original_sender, msg, null);
    }

    public void send(Address final_destination, Address original_sender, final Message msg, Collection<String> visited_sites) {
        if(log.isTraceEnabled())
            log.trace("%s: routing message to %s via %s", bridge.address(), final_destination, site_master);
        long start=stats? System.nanoTime() : 0;
        try {
            Message copy=createMessage(site_master, final_destination, original_sender, msg, visited_sites);
            if(relay3) {
                Address dest=copy.dest();
                boolean multicast=dest == null || dest.isMulticast();
                // we can skip UNICAST3 in the bridge cluster because the local cluster's UNICAST3 will retransmit
                // unicast messages anyway
                if(!multicast)
                    copy.setFlag(Message.Flag.NO_RELIABILITY);
            }
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

    protected Message createMessage(Address target, Address final_destination, Address original_sender,
                                    final Message msg, Collection<String> visited_sites) throws IOException {
        Message copy=relay.copy(msg).setDest(target).setSrc(null);
        RelayHeader tmp=msg.getHeader(relay.getId());
        RelayHeader hdr=tmp != null? tmp.copy().setFinalDestination(final_destination).setOriginalSender(original_sender)
          : new RelayHeader(DATA, final_destination, original_sender);
          hdr.addToVisitedSites(visited_sites)
            .originalFlags(copy.getFlags()); // store the original flags, will be restored at the receiver
        if(relay3) {
            // to prevent local headers getting mixed up with bridge headers: https://issues.redhat.com/browse/JGRP-2729
            Header[] original_hdrs=((BaseMessage)copy).headers();
            if(Headers.size(original_hdrs) > 0) {
                hdr.originalHeaders(original_hdrs);
                copy.clearHeaders();
            }
        }
        copy.putHeader(relay.getId(), hdr);
        return copy;
    }
}
