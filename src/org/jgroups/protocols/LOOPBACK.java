// $Id: LOOPBACK.java,v 1.4 2004/04/23 19:36:13 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.Vector;


/**
 Makes copies of outgoing messages, swaps sender and receiver and sends the message back up the stack.
 */
public class LOOPBACK extends Protocol {
    private Address local_addr=null;
    private String group_addr=null;
    private Vector members=new Vector();


    public LOOPBACK() {
        ;
    }


    public String toString() {
        return "Protocol LOOPBACK(local address: " + local_addr + ")";
    }




    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "LOOPBACK";
    }



    public void init() throws Exception {
        local_addr=new org.jgroups.stack.IpAddress("localhost", 10000); // fake address
    }

    public void start() throws Exception {
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }


    /**
     * Caller by the layer above this layer. Usually we just put this Message
     * into the send queue and let one or more worker threads handle it. A worker thread
     * then removes the Message from the send queue, performs a conversion and adds the
     * modified Message to the send queue of the layer below it, by calling Down).
     */
    public void down(Event evt) {
        Message msg, rsp;
        Address dest_addr;


            if(log.isInfoEnabled()) log.info("event is " + evt + ", group_addr=" + group_addr +
                                          ", time is " + System.currentTimeMillis() + ", hdrs: " + Util.printEvent(evt));


        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                rsp=msg.copy();
                dest_addr=msg.getDest();
                rsp.setDest(local_addr);
                rsp.setSrc(dest_addr != null ? dest_addr : local_addr);
                passUp(new Event(Event.MSG, rsp));
                break;

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                synchronized(members) {
                    members.removeAllElements();
                    Vector tmpvec=((View)evt.getArg()).getMembers();
                    for(int i=0; i < tmpvec.size(); i++)
                        members.addElement(tmpvec.elementAt(i));
                }
                break;

            case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
                passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
                break;

            case Event.CONNECT:
                group_addr=(String)evt.getArg();
                passUp(new Event(Event.CONNECT_OK));
                break;

            case Event.DISCONNECT:
                passUp(new Event(Event.DISCONNECT_OK));
                break;

            case Event.PERF:
                passUp(evt);
                break;

            case Event.ACK:
                passUp(new Event(Event.ACK_OK));
                break;                
        }


    }



    /*--------------------------- End of Protocol interface -------------------------- */






    /* ------------------------------ Private methods -------------------------------- */

    /**
     If the sender is null, set our own address. We cannot just go ahead and set the address
     anyway, as we might be sending a message on behalf of someone else ! E.g. in case of
     retransmission, when the original sender has crashed, or in a FLUSH protocol when we
     have to return all unstable messages with the FLUSH_OK response.
     */
    private void setSourceAddress(Message msg) {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
    }


    /* ------------------------------------------------------------------------------- */


}
