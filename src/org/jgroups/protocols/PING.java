package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.Property;
import org.jgroups.util.Promise;
import org.jgroups.util.UUID;

import java.util.Arrays;
import java.util.List;


/**
 * The PING protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack). We do this by mcasting PING
 * requests to an IP MCAST address.
 * The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to PING requests with a PING
 * response.
 * @author Bela Ban
 * @version $Id: PING.java,v 1.62 2009/09/06 13:51:07 belaban Exp $
 */
@DeprecatedProperty(names={"gossip_host", "gossip_port", "gossip_refresh", "port_range", "socket_conn_timeout",
        "socket_read_timeout", "discovery_timeout"})
public class PING extends Discovery {
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Time (in ms) to wait for our own discovery message to be received. 0 means don't wait. If the " +
            "discovery message is not received within discovery_timeout ms, a warning will be logged")
    private long discovery_timeout=0L;

    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    protected final Promise<Boolean>   discovery_reception=new Promise<Boolean>();


    public void stop() {
        super.stop();
        discovery_reception.reset();
    }
    

    public void sendGetMembersRequest(String cluster_name, Promise promise, boolean return_views_only) throws Exception{
        //  Mcast GET_MBRS_REQ message
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
        PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, data, cluster_name);
        hdr.return_view_only=return_views_only;
        Message msg=new Message(null);  // mcast msg
        msg.setFlag(Message.OOB);
        msg.putHeader(getName(), hdr); // needs to be getName(), so we might get "MPING" !
        sendMcastDiscoveryRequest(msg);
    }


    public Object up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            PingHeader hdr=(PingHeader)msg.getHeader(getName());
            if(hdr != null && hdr.type == PingHeader.GET_MBRS_REQ && msg.getSrc().equals(local_addr)) {
                discovery_reception.setResult(true);
            }
        }
        return super.up(evt);
    }

    void sendMcastDiscoveryRequest(Message discovery_request) {
        discovery_reception.reset();
        down_prot.down(new Event(Event.MSG, discovery_request));
        waitForDiscoveryRequestReception();
    }


    protected void waitForDiscoveryRequestReception() {
        if(discovery_timeout > 0) {
            try {
                discovery_reception.getResultWithTimeout(discovery_timeout);
            }
            catch(TimeoutException e) {
                if(log.isWarnEnabled())
                    log.warn("didn't receive my own discovery request - multicast socket might not be configured correctly");
            }
        }
    }
}