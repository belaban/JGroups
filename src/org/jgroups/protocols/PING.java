package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.util.Responses;
import org.jgroups.util.UUID;

import java.io.InterruptedIOException;
import java.util.List;


/**
 * The PING protocol retrieves the initial membership by mcasting a discovery request (via the multicast capable
 * transport) to all current cluster members<p/>
 * The responses should allow us to determine the coordinator which we have to contact, e.g. in case we want to join
 * the group.  When we are a server (after having received the BECOME_SERVER event), we'll respond to discovery requests
 * with a discovery response.
 * @author Bela Ban
 */
public class PING extends Discovery {

    public boolean isDynamic() {
        return true;
    }


    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        try {
            sendDiscoveryRequest(cluster_name, members);
        }
        catch(InterruptedIOException ie) {
            ;
        }
        catch(InterruptedException ex) {
            ;
        }
        catch(Throwable ex) {
            log.error("failed sending discovery request", ex);
        }
    }


    protected void sendDiscoveryRequest(String cluster_name, List<Address> members_to_find) throws Exception {
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));

        // https://issues.jboss.org/browse/JGRP-1670
        PingData data=new PingData(local_addr, false, UUID.get(local_addr), physical_addr);
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName(cluster_name);

        if(members_to_find != null && members_to_find.size() <= max_members_in_discovery_request)
            data.mbrs(members_to_find);

        // message needs to have DONT_BUNDLE flag: if A sends message M to B, and we need to fetch B's physical
        // address, then the bundler thread blocks until the discovery request has returned. However, we cannot send
        // the discovery *request* until the bundler thread has returned from sending M
        Message msg=new Message(null).putHeader(getId(),hdr).setBuffer(marshal(data))
          .setFlag(Message.Flag.INTERNAL,Message.Flag.DONT_BUNDLE,Message.Flag.OOB)
          .setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
        sendMcastDiscoveryRequest(msg);
    }

    protected void sendMcastDiscoveryRequest(Message msg) {
        down_prot.down(new Event(Event.MSG, msg));
    }
}