
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;

import java.util.*;


/**
 * The Discovery protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack). We do this by specific subclasses, e.g. by mcasting PING
 * requests to an IP MCAST address or, if gossiping is enabled, by contacting the GossipServer.
 * The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to PING requests with a PING
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * The following properties are available
 * property: timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
 * property: num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
 * @author Bela Ban
 * @version $Id: Discovery.java,v 1.1 2005/01/04 08:17:06 belaban Exp $
 */
public abstract class Discovery extends Protocol {
    final Vector  members=new Vector(11);
    // final Vector  initial_members=new Vector(11);
    final Set     members_set=new HashSet(11); // copy of the members vector for fast random access
    Address       local_addr=null;
    String        group_addr=null;
    long          timeout=3000;
    int           num_initial_members=2;
    boolean       is_server=false;
    PingWaiter    waiter;


    public abstract String getName();

    /** Called after local_addr was set */
    public void localAddressSet(Address addr) {
        ;
    }

    public abstract void sendGetMembersRequest();


    /** Called when CONNECT_OK has been received */
    public void handleConnectOK() {
        ;
    }

    public void handleDisconnect() {
        ;
    }

    public void handleConnect() {
        ;
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
     * @param props - a property set
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

    public void start() throws Exception {
        super.start();
        if(waiter == null)
            waiter=new PingWaiter(timeout, num_initial_members, this);
    }

    public void stop() {
        is_server=false;
        if(waiter != null)
            waiter.stop();
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
                if(!is_server)
                    return;

                synchronized(members) {
                    coord=members.size() > 0 ? (Address)members.firstElement() : local_addr;
                }

                PingRsp ping_rsp=new PingRsp(local_addr, coord);
                rsp_msg=new Message(msg.getSrc(), null, null);
                rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, ping_rsp);
                rsp_msg.putHeader(getName(), rsp_hdr);
                if(log.isTraceEnabled())
                    log.trace("[GET_MBRS_REQ] from " + msg.getSrc() + ", sending response " + rsp_hdr);
                passDown(new Event(Event.MSG, rsp_msg));
                return;

            case PingHeader.GET_MBRS_RSP:   // add response to vector and notify waiting thread
                rsp=hdr.arg;

                if(log.isTraceEnabled())
                    log.trace("received FIND_INITAL_MBRS_RSP, rsp=" + rsp);


                // synchronized(initial_members) {
                   // initial_members.addElement(rsp);
                    //initial_members.notifyAll();
                //}
                waiter.addResponse(rsp);

                return;

            default:
                if(log.isWarnEnabled()) log.warn("got PING header with unknown type (" + hdr.type + ')');
                return;
            }


        case Event.SET_LOCAL_ADDRESS:
            passUp(evt);
            local_addr=(Address)evt.getArg();
            localAddressSet(local_addr);
            break;

        case Event.CONNECT_OK:
            handleConnectOK();
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

        switch(evt.getType()) {

        case Event.FIND_INITIAL_MBRS:   // sent by GMS layer, pass up a GET_MBRS_OK event


            //synchronized(initial_members) {
              //  initial_members.removeAllElements();
            //}
            waiter.clearResponses();

            // 1. Send the GET_MBRS_REQ to all members (to be overridden by subclasses)
            sendGetMembersRequest();



            // 2. Wait 'timeout' ms or until 'num_initial_members' have been retrieved
            waiter.start();

            /*synchronized(initial_members) {
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
            passUp(new Event(Event.FIND_INITIAL_MBRS_OK, initial_members));*/


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
            break;

        case Event.CONNECT:
            group_addr=(String)evt.getArg();
            passDown(evt);
            handleConnect();
            break;

        case Event.DISCONNECT:
            handleDisconnect();
            passDown(evt);
            break;

        default:
            passDown(evt);          // Pass on to the layer below us
            break;
        }
    }



    /* -------------------------- Private methods ---------------------------- */


    protected View makeView(Vector mbrs) {
        Address coord=null;
        long id=0;
        ViewId view_id=new ViewId(local_addr);

        coord=view_id.getCoordAddress();
        id=view_id.getId();

        return new View(coord, id, mbrs);
    }



}
