// $Id: PING.java,v 1.19 2004/12/31 14:10:38 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.List;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.util.*;


/**
 * The PING protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack). We do this by mcasting PING
 * requests to an IP MCAST address (or, if gossiping is enabled, by contacting the GossipServer).
 * The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to PING requests with a PING
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * The following properties are available
 * property: timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
 * property: num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
 * property: gossip_host - if you are using GOSSIP then this defines the host of the GossipServer, default is null
 * property: gossip_port - if you are using GOSSIP then this defines the port of the GossipServer, default is null
 */
public class PING extends Protocol {
    final Vector members=new Vector(11);
    final Vector initial_members=new Vector(11);
    final Set members_set=new HashSet(11); //copy of the members vector for fast random access
    Address local_addr=null;
    String group_addr=null;
    long timeout=3000;
    long num_initial_members=2;
    String gossip_host=null;
    int gossip_port=0;
    long gossip_refresh=20000; // time in msecs after which the entry in GossipServer will be refreshed
    GossipClient client;
    boolean is_server=false;
    int port_range=1;        // number of ports to be probed for initial membership
    List initial_hosts=null;  // hosts to be contacted for the initial membership

    public String getName() {
        return "PING";
    }


    public Vector providedUpServices() {
        Vector ret=new Vector(1);
        ret.addElement(new Integer(Event.FIND_INITIAL_MBRS));
        return ret;
    }

    /**
     * sets the properties of the PING protocol.
     * The following properties are available
     * property: timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
     * property: num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
     * property: gossip_host - if you are using GOSSIP then this defines the host of the GossipServer, default is null
     * property: gossip_port - if you are using GOSSIP then this defines the port of the GossipServer, default is null
     *
     * @param props - a property set containing only PING properties
     * @return returns true if all properties were parsed properly
     *         returns false if there are unrecnogized properties in the property set
     */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("timeout");              // max time to wait for initial members
        if(str != null) {
            timeout=Long.parseLong(str);
            if(timeout <= 0)
                if(log.isErrorEnabled()) log.error("timeout must be > 0");
            props.remove("timeout");
        }

        str=props.getProperty("num_initial_members");  // wait for at most n members
        if(str != null) {
            num_initial_members=Integer.parseInt(str);
            props.remove("num_initial_members");
        }

        str=props.getProperty("gossip_host");
        if(str != null) {
            gossip_host=str;
            props.remove("gossip_host");
        }

        str=props.getProperty("gossip_port");
        if(str != null) {
            gossip_port=Integer.parseInt(str);
            props.remove("gossip_port");
        }

        str=props.getProperty("gossip_refresh");
        if(str != null) {
            gossip_refresh=Long.parseLong(str);
            props.remove("gossip_refresh");
        }

        if(gossip_host != null && gossip_port != 0) {
            try {
                client=new GossipClient(new IpAddress(InetAddress.getByName(gossip_host), gossip_port), gossip_refresh);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("creation of GossipClient failed, exception=" + e);
                return false; // will cause stack creation to abort
            }
        }

        str=props.getProperty("port_range");           // if member cannot be contacted on base port,
        if(str != null) {                              // how many times can we increment the port
            port_range=Integer.parseInt(str);
            if(port_range < 1) {
                port_range=1;
            }
            props.remove("port_range");
        }

        str=props.getProperty("initial_hosts");
        if(str != null) {
            props.remove("initial_hosts");
            initial_hosts=createInitialHosts(str);
        }

        if(props.size() > 0) {
            StringBuffer sb=new StringBuffer();
            for(Enumeration e=props.propertyNames(); e.hasMoreElements();) {
                sb.append(e.nextElement().toString());
                if(e.hasMoreElements()) {
                    sb.append(", ");
                }
            }
            if(log.isErrorEnabled()) log.error("The following properties are not recognized: " + sb);
            return false;
        }
        return true;
    }


    public void stop() {
        is_server=false;
        // System.out.println("### PING.stop(): is_server=" + is_server);
        // ovidiu, Dec 10 2002
        if(client != null) {
            client.stop();
        }
    }


    /**
     * An event was received from the layer below. Usually the current layer will want to examine
     * the event type and - depending on its type - perform some computation
     * (e.g. removing headers from a MSG event type, or updating the internal membership list
     * when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down
     * the stack using <code>PassDown</code> or c) the event (or another event) is sent up
     * the stack using <code>PassUp</code>.
     * <p/>
     * For the PING protocol, the Up operation does the following things.
     * 1. If the event is a Event.MSG then PING will inspect the message header.
     * If the header is null, PING simply passes up the event
     * If the header is PingHeader.GET_MBRS_REQ then the PING protocol
     * will PassDown a PingRequest message
     * If the header is PingHeader.GET_MBRS_RSP we will add the message to the initial members
     * vector and wake up any waiting threads.
     * 2. If the event is Event.SET_LOCAL_ADDR we will simple set the local address of this protocol
     * 3. For all other messages we simple pass it up to the protocol above
     *
     * @param evt - the event that has been sent from the layer below
     */

    public void up(Event evt) {
        Message msg, rsp_msg;
        Object obj;
        PingHeader hdr, rsp_hdr;
        PingRsp rsp;
        Address coord;

        switch(evt.getType()) {

        case Event.MSG:
            msg=(Message)evt.getArg();
            obj=msg.getHeader(getName());
            if(obj == null || !(obj instanceof PingHeader)) {
                passUp(evt);
                return;
            }
            hdr=(PingHeader)msg.removeHeader(getName());

            switch(hdr.type) {

            case PingHeader.GET_MBRS_REQ:   // return Rsp(local_addr, coord)
                if(!is_server) {
                    return;
                }
                synchronized(members) {
                    coord=members.size() > 0 ? (Address)members.firstElement() : local_addr;
                }

                PingRsp ping_rsp=new PingRsp(local_addr, coord);
                rsp_msg=new Message(msg.getSrc(), null, null);
                rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, ping_rsp);
                rsp_msg.putHeader(getName(), rsp_hdr);
                if(log.isTraceEnabled())
                    log.trace("received GET_MBRS_REQ from " + msg.getSrc() + ", returning " + rsp_hdr);
                passDown(new Event(Event.MSG, rsp_msg));
                return;

            case PingHeader.GET_MBRS_RSP:   // add response to vector and notify waiting thread
                rsp=hdr.arg;

                if(log.isTraceEnabled())
                    log.trace("received FIND_INITAL_MBRS_RSP, rsp=" + rsp);
                synchronized(initial_members) {
                    initial_members.addElement(rsp);
                    initial_members.notifyAll();
                }
                return;

            default:
                if(log.isWarnEnabled()) log.warn("got PING header with unknown type (" + hdr.type + ')');
                return;
            }


        case Event.SET_LOCAL_ADDRESS:
            passUp(evt);
            local_addr=(Address)evt.getArg();
            // Add own address to initial_hosts if not present: we must always be able to ping ourself !
            if(initial_hosts != null && local_addr != null) {
                List hlist;
                boolean inInitialHosts=false;
                for(Enumeration en=initial_hosts.elements(); en.hasMoreElements() && !inInitialHosts;) {
                    hlist=(List)en.nextElement();
                    if(hlist.contains(local_addr)) {
                        inInitialHosts=true;
                    }
                }
                if(!inInitialHosts) {
                    hlist=new List();
                    hlist.add(local_addr);
                    initial_hosts.add(hlist);
                    if(log.isDebugEnabled())
                        log.debug("[SET_LOCAL_ADDRESS]: adding my own address (" + local_addr +
                                  ") to initial_hosts; initial_hosts=" + initial_hosts);
                }
            }
            break;

        default:
            passUp(evt);            // Pass up to the layer above us
            break;
        }
    }


    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using <code>PassDown</code>. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using <code>PassUp</code>.
     * The PING protocol is interested in several different down events,
     * Event.FIND_INITIAL_MBRS - sent by the GMS layer and expecting a GET_MBRS_OK
     * Event.TMP_VIEW and Event.VIEW_CHANGE - a view change event
     * Event.BECOME_SERVER - called after client has joined and is fully working group member
     * Event.CONNECT, Event.DISCONNECT.
     */
    public void down(Event evt) {
        Message msg;
        PingHeader hdr;
        long time_to_wait, start_time;
        Vector gossip_rsps=null;


        switch(evt.getType()) {

        case Event.FIND_INITIAL_MBRS:   // sent by GMS layer, pass up a GET_MBRS_OK event
            initial_members.removeAllElements();

            if(client != null) {
                gossip_rsps=client.getMembers(group_addr);
                if(gossip_rsps != null && gossip_rsps.size() > 0) {
                    // Set a temporary membership in the UDP layer, so that the following multicast
                    // will be sent to all of them
                    Event view_event=new Event(Event.TMP_VIEW, makeView(gossip_rsps));
                    passDown(view_event); // needed e.g. by failure detector or UDP
                }
                else {
                    passUp(new Event(Event.FIND_INITIAL_MBRS_OK, null));
                    return;
                }


                if(gossip_rsps != null && gossip_rsps.size() > 0) {
                    for(int i=0; i < gossip_rsps.size(); i++) {
                        Address dest=(Address)gossip_rsps.elementAt(i);
                        msg=new Message(dest, null, null);  // mcast msg
                        msg.putHeader(getName(), new PingHeader(PingHeader.GET_MBRS_REQ, null));
                        passDown(new Event(Event.MSG, msg));
                    }
                }

                Util.sleep(500);
            }
            else {
                if(initial_hosts != null && initial_hosts.size() > 0) {
                    IpAddress h;
                    List hlist;
                    msg=new Message(null, null, null);
                    msg.putHeader(getName(), new PingHeader(PingHeader.GET_MBRS_REQ, null));

                    synchronized(members) {
                        int numMembers=members.size();
                        int numMemberInitialHosts=0;
                        Address coord=numMembers > 0 ? (Address)members.firstElement() : local_addr;
                        for(Enumeration en=initial_hosts.elements(); en.hasMoreElements();) {
                            hlist=(List)en.nextElement();
                            boolean isMember=false;

                            for(Enumeration hen=hlist.elements(); hen.hasMoreElements() && !isMember && numMemberInitialHosts < numMembers;) {
                                h=(IpAddress)hen.nextElement();
                                if(members_set.contains(h)) {
                                    //update the initial_members list for this already connected member
                                    initial_members.add(new PingRsp(h, coord));
                                    isMember=true;
                                    numMemberInitialHosts++;
                                    if(log.isDebugEnabled())
                                        log.debug("[FIND_INITIAL_MBRS] " + h + " is already a member");
                                }
                            }
                            for(Enumeration hen=hlist.elements(); hen.hasMoreElements() && !isMember;) {
                                h=(IpAddress)hen.nextElement();
                                msg.setDest(h);
                                if(log.isTraceEnabled())
                                    log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
                                passDown(new Event(Event.MSG, msg.copy()));
                            }
                        }
                    }
                }
                else {
                    // 1. Mcast GET_MBRS_REQ message
                    if(log.isTraceEnabled()) log.trace("FIND_INITIAL_MBRS");
                    hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
                    msg=new Message(null, null, null);  // mcast msg
                    msg.putHeader(getName(), hdr);
                    passDown(new Event(Event.MSG, msg));
                }
            }



            // 2. Wait 'timeout' ms or until 'num_initial_members' have been retrieved
            synchronized(initial_members) {
                start_time=System.currentTimeMillis();
                time_to_wait=timeout;

                while(initial_members.size() < num_initial_members && time_to_wait > 0) {
                    if(log.isTraceEnabled()) // +++ remove
                        log.trace("waiting for initial members: time_to_wait=" + time_to_wait +
                                  ", got " + initial_members.size() + " rsps");

                    try {
                        initial_members.wait(time_to_wait);
                    }
                    catch(Exception e) {
                    }
                    time_to_wait=timeout - (System.currentTimeMillis() - start_time);
                }
            }

            // 3. Send response
            if(log.isDebugEnabled())
                log.debug("initial mbrs are " + initial_members);
            passUp(new Event(Event.FIND_INITIAL_MBRS_OK, initial_members));
            break;


        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            Vector tmp;
            if((tmp=((View)evt.getArg()).getMembers()) != null) {
                synchronized(members) {
                    members.clear();
                    members.addAll(tmp);
                    members_set.clear();
                    members_set.addAll(tmp);
                }
            }
            passDown(evt);
            break;

        case Event.BECOME_SERVER: // called after client has joined and is fully working group member
            passDown(evt);
            is_server=true;
            // System.out.println("## down(BECOME_SERVER): is_server=" + is_server);
            break;

        case Event.CONNECT:
            group_addr=(String)evt.getArg();
            passDown(evt);
            if(client != null)
                client.register(group_addr, local_addr);
            break;

        case Event.DISCONNECT:
            if(client != null)
                client.stop();
            passDown(evt);
            break;

        default:
            passDown(evt);          // Pass on to the layer below us
            break;
        }
    }



    /* -------------------------- Private methods ---------------------------- */


    private View makeView(Vector mbrs) {
        Address coord=null;
        long id=0;
        ViewId view_id=new ViewId(local_addr);

        coord=view_id.getCoordAddress();
        id=view_id.getId();

        return new View(coord, id, mbrs);
    }

    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return List of IpAddresses
     */
    private List createInitialHosts(String l) {
        List tmp=new List();
        StringTokenizer tok=new StringTokenizer(l, ",");
        String t;

        while(tok.hasMoreTokens()) {
            try {
                t=tok.nextToken();
                String host=t.substring(0, t.indexOf('['));
                int port=Integer.parseInt(t.substring(t.indexOf('[') + 1, t.indexOf(']')));
                List hosts=new List();
                for(int i=port; i < port + port_range; i++) {
                    hosts.add(new IpAddress(host, i));
                }
                tmp.add(hosts);
            }
            catch(NumberFormatException e) {
                if(log.isErrorEnabled()) log.error("exeption is " + e);
            }
        }
        return tmp;
    }


}
