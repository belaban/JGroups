// $Id: TCPGOSSIP.java,v 1.10 2004/12/31 14:10:38 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;

import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;


/**
 * The TCPGOSSIP protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack).
 * We do this by contacting one or more GossipServers, which must be running at well-known
 * addresses:ports. The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to TCPGOSSIP requests with a TCPGOSSIP
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 *
 * @author Bela Ban
 */
public class TCPGOSSIP extends Protocol {
    final Vector members=new Vector();
    final Vector initial_members=new Vector();
    Address local_addr=null;
    String group_addr=null;
    final String groupname=null;
    long timeout=3000;
    long num_initial_members=2;
    Vector initial_hosts=null;  // (list of IpAddresses) hosts to be contacted for the initial membership
    boolean is_server=false;
    GossipClient gossip_client=null;  // accesses the GossipServer(s) to find initial mbrship

    // we need to refresh the registration with the GossipServer(s) periodically,
    // so that our entries are not purged from the cache
    long gossip_refresh_rate=20000;


    public String getName() {
        return "TCPGOSSIP";
    }


    public Vector providedUpServices() {
        Vector ret=new Vector();
        ret.addElement(new Integer(Event.FIND_INITIAL_MBRS));
        return ret;
    }


    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("timeout");              // max time to wait for initial members
        if(str != null) {
            timeout=Long.parseLong(str);
            props.remove("timeout");
        }

        str=props.getProperty("num_initial_members");  // wait for at most n members
        if(str != null) {
            num_initial_members=Integer.parseInt(str);
            props.remove("num_initial_members");
        }

        str=props.getProperty("gossip_refresh_rate");  // wait for at most n members
        if(str != null) {
            gossip_refresh_rate=Integer.parseInt(str);
            props.remove("gossip_refresh_rate");
        }

        str=props.getProperty("initial_hosts");
        if(str != null) {
            props.remove("initial_hosts");
            initial_hosts=createInitialHosts(str);
        }

        if(props.size() > 0) {
            System.err.println("TCPGOSSIP.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }

        if(initial_hosts == null || initial_hosts.size() == 0) {
            if(log.isErrorEnabled()) log.error("initial_hosts must contain the address of at least one GossipServer");
            return false;
        }

        return true;
    }


    public void init() throws Exception {

    }

    public void start() throws Exception {
        if(gossip_client == null)
            gossip_client=new GossipClient(initial_hosts, gossip_refresh_rate);
    }

    public void stop() {
        if(gossip_client != null) {
            gossip_client.stop();
            gossip_client=null;
        }
    }


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
                    //System.err.println("TCPGOSSIP.up(GET_MBRS_REQ): did not return a response " +
                    //	       "as I'm not a server yet !");
                    return;
                }
                synchronized(members) {
                    coord=members.size() > 0 ? (Address)members.firstElement() : local_addr;
                }
                rsp_msg=new Message(msg.getSrc(), null, null);
                rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, new PingRsp(local_addr, coord));
                rsp_msg.putHeader(getName(), rsp_hdr);
                passDown(new Event(Event.MSG, rsp_msg));
                return;

            case PingHeader.GET_MBRS_RSP:   // add response to vector and notify waiting thread
                rsp=hdr.arg;
                synchronized(initial_members) {
                    initial_members.addElement(rsp);
                    initial_members.notifyAll();
                }
                return;

            default:
                if(log.isWarnEnabled()) log.warn("got TCPGOSSIP header with unknown type (" + hdr.type + ')');
                return;
            }


        case Event.CONNECT_OK:
            // removed May 8 2003 (bela) -- generated by GMS
            //passUp(evt);

            if(group_addr == null || local_addr == null) {
                if(log.isErrorEnabled())
                    log.error("[CONNECT_OK]: group_addr or local_addr is null. " +
                              "cannot register with GossipServer(s)");
            }
            else {
                if(log.isTraceEnabled())
                    log.trace("[CONNECT_OK]: registering " + local_addr +
                              " under " + group_addr + " with GossipServer");
                gossip_client.register(group_addr, local_addr);
            }
            break;

        case Event.SET_LOCAL_ADDRESS:
            passUp(evt);
            local_addr=(Address)evt.getArg();
            break;

        default:
            passUp(evt);            // Pass up to the layer above us
            break;
        }
    }


    public void down(Event evt) {
        Message msg, copy;
        PingHeader hdr;
        long time_to_wait, start_time;
        Vector tmp_mbrs;
        Address mbr_addr;

        switch(evt.getType()) {

        case Event.FIND_INITIAL_MBRS:   // sent by GMS layer, pass up a GET_MBRS_OK event

            initial_members.removeAllElements();
            if(group_addr == null) {
                if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: group_addr is null, cannot get mbrship");
                passUp(new Event(Event.FIND_INITIAL_MBRS_OK, initial_members));
                break;
            }
            if(log.isTraceEnabled()) log.trace("fetching members from GossipServer(s)");
            tmp_mbrs=gossip_client.getMembers(group_addr);
            if(tmp_mbrs == null || tmp_mbrs.size() == 0) {
                if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: gossip client found no members");
                passUp(new Event(Event.FIND_INITIAL_MBRS_OK, initial_members));
            }
            if(log.isTraceEnabled()) log.trace("consolidated mbrs from GossipServer(s) are " + tmp_mbrs);

            // 1. 'Mcast' GET_MBRS_REQ message
            hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
            msg=new Message(null, null, null);
            msg.putHeader(getName(), hdr);

            for(int i=0; i < tmp_mbrs.size(); i++) {
                mbr_addr=(IpAddress)tmp_mbrs.elementAt(i);
                copy=msg.copy();
                copy.setDest(mbr_addr);
                if(log.isTraceEnabled()) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + copy.getDest());
                passDown(new Event(Event.MSG, copy));
            }


            // 2. Wait 'timeout' ms or until 'num_initial_members' have been retrieved
            synchronized(initial_members) {
                start_time=System.currentTimeMillis();
                time_to_wait=timeout;

                while(initial_members.size() < num_initial_members && time_to_wait > 0) {
                    try {
                        initial_members.wait(time_to_wait);
                    }
                    catch(Exception e) {
                    }
                    time_to_wait=timeout - (System.currentTimeMillis() - start_time);
                }
            }
            if(log.isTraceEnabled()) log.trace("[FIND_INITIAL_MBRS] initial members are " + initial_members);

            // 3. Send response
            passUp(new Event(Event.FIND_INITIAL_MBRS_OK, initial_members));
            break;

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            Vector tmp;
            if((tmp=((View)evt.getArg()).getMembers()) != null) {
                synchronized(members) {
                    members.removeAllElements();
                    for(int i=0; i < tmp.size(); i++)
                        members.addElement(tmp.elementAt(i));
                }
            }
            passDown(evt);
            break;

        case Event.BECOME_SERVER: // called after client has joined and is fully working group member
            passDown(evt);
            is_server=true;
            break;

        case Event.CONNECT:
            group_addr=(String)evt.getArg();
            passDown(evt);
            break;

        case Event.DISCONNECT:
            passDown(evt);
            break;

        default:
            passDown(evt);          // Pass on to the layer below us
            break;
        }
    }



    /* -------------------------- Private methods ---------------------------- */


    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return list of IpAddresses
     */
    private Vector createInitialHosts(String l) {
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

