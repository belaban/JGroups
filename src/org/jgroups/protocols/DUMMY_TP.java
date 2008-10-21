// $Id: DUMMY_TP.java,v 1.8 2008/10/21 12:10:30 vlada Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;


/**
 * Dummy transport, returns a fake local address and responds to CONNECT.
 * Compared to LOOPBACK, this discards everything
 * @author Bela Ban
 * @version $Id: DUMMY_TP.java,v 1.8 2008/10/21 12:10:30 vlada Exp $
 */
@Unsupported
public class DUMMY_TP extends Protocol {
    private Address local_addr=null;

    public DUMMY_TP() {
    }


    public String toString() {
        return "Protocol DUMMY_TP (local address: " + local_addr + ')';
    }


    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "DUMMY_TP";
    }




    public void init() throws Exception {
        local_addr=new org.jgroups.stack.IpAddress("localhost", 10000); // fake address
    }

    public void start() throws Exception {
        up_prot.up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }


    /**
     * Caller by the layer above this layer. Usually we just put this Message
     * into the send queue and let one or more worker threads handle it. A worker thread
     * then removes the Message from the send queue, performs a conversion and adds the
     * modified Message to the send queue of the layer below it, by calling Down).
     */
    public Object down(Event evt) {

        switch(evt.getType()) {

        case Event.CONNECT:
            return null;

        case Event.DISCONNECT:
            return null;
        }
        return down_prot.down(evt);
    }



    /*--------------------------- End of Protocol interface -------------------------- */




}
