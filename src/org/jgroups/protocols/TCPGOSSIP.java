// $Id: TCPGOSSIP.java,v 1.21 2006/12/11 15:38:56 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;

import java.util.*;
import java.net.UnknownHostException;


/**
 * The TCPGOSSIP protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack).
 * We do this by contacting one or more GossipRouters, which must be running at well-known
 * addresses:ports. The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to TCPGOSSIP requests with a TCPGOSSIP
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 *
 * @author Bela Ban
 */
public class TCPGOSSIP extends Discovery {
    Vector initial_hosts=null;  // (list of IpAddresses) hosts to be contacted for the initial membership
    GossipClient gossip_client=null;  // accesses the GossipRouter(s) to find initial mbrship

    // we need to refresh the registration with the GossipRouter(s) periodically,
    // so that our entries are not purged from the cache
    long gossip_refresh_rate=20000;

    final static Vector EMPTY_VECTOR=new Vector();
    final static String name="TCPGOSSIP";


    public String getName() {
        return name;
    }



    public boolean setProperties(Properties props) {
        String str;
        str=props.getProperty("gossip_refresh_rate");  // wait for at most n members
        if(str != null) {
            gossip_refresh_rate=Integer.parseInt(str);
            props.remove("gossip_refresh_rate");
        }

        str=props.getProperty("initial_hosts");
        if(str != null) {
            props.remove("initial_hosts");
            try {
                initial_hosts=createInitialHosts(str);
            }
            catch(UnknownHostException ex) {
                if(log.isErrorEnabled())
                    log.error("failed creating initial hosts", ex);
                return false;
            }
        }

        if(initial_hosts == null || initial_hosts.size() == 0) {
            if(log.isErrorEnabled()) log.error("initial_hosts must contain the address of at least one GossipRouter");
            return false;
        }
        return super.setProperties(props);
    }



    public void start() throws Exception {
        super.start();
        if(gossip_client == null)
            gossip_client=new GossipClient(initial_hosts, gossip_refresh_rate);
    }

    public void stop() {
        super.stop();
        if(gossip_client != null) {
            gossip_client.stop();
            gossip_client=null;
        }
    }

    public void destroy() {
        if(gossip_client != null) {
            gossip_client.destroy();
            gossip_client=null;
        }
    }


    public void handleConnectOK() {
        if(group_addr == null || local_addr == null) {
            if(log.isErrorEnabled())
                log.error("[CONNECT_OK]: group_addr or local_addr is null. " +
                          "cannot register with GossipRouter(s)");
        }
        else {
            if(trace)
                log.trace("[CONNECT_OK]: registering " + local_addr +
                          " under " + group_addr + " with GossipRouter");
            gossip_client.register(group_addr, local_addr);
        }
    }

    public void sendGetMembersRequest() {
        Message msg, copy;
        PingHeader hdr;
        List tmp_mbrs;
        Address mbr_addr;

        if(group_addr == null) {
            if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: group_addr is null, cannot get mbrship");
            passUp(new Event(Event.FIND_INITIAL_MBRS_OK, EMPTY_VECTOR));
            return;
        }
        if(trace) log.trace("fetching members from GossipRouter(s)");
        tmp_mbrs=gossip_client.getMembers(group_addr);
        if(tmp_mbrs == null || tmp_mbrs.size() == 0) {
            if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: gossip client found no members");
            passUp(new Event(Event.FIND_INITIAL_MBRS_OK, EMPTY_VECTOR));
            return;
        }
        if(trace) log.trace("consolidated mbrs from GossipRouter(s) are " + tmp_mbrs);

        // 1. 'Mcast' GET_MBRS_REQ message
        hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
        msg=new Message(null);
        msg.setFlag(Message.OOB);
        msg.putHeader(name, hdr);

        for(Iterator it=tmp_mbrs.iterator(); it.hasNext();) {
            mbr_addr=(Address)it.next();
            copy=msg.copy();
            copy.setDest(mbr_addr);
            if(trace) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + copy.getDest());
            passDown(new Event(Event.MSG, copy));
        }
    }



    /* -------------------------- Private methods ---------------------------- */


    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return list of IpAddresses
     */
    private Vector createInitialHosts(String l) throws UnknownHostException {
        Vector tmp=new Vector();
        String host;
        int port;
        IpAddress addr;
        StringTokenizer tok=new StringTokenizer(l, ",");
        String t;

        while(tok.hasMoreTokens()) {
            try {
                t=tok.nextToken();
                host=t.substring(0, t.indexOf('['));
                port=Integer.parseInt(t.substring(t.indexOf('[') + 1, t.indexOf(']')));
                addr=new IpAddress(host, port);
                tmp.addElement(addr);
            }
            catch(NumberFormatException e) {
                if(log.isErrorEnabled()) log.error("exeption is " + e);
            }
        }

        return tmp;
    }


}

