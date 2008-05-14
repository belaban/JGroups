// $Id: LOOPBACK.java,v 1.26 2008/05/14 13:26:33 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;


/**
 Makes copies of outgoing messages, swaps sender and receiver and sends the message back up the stack.
 */
public class LOOPBACK extends Protocol {
    private Address local_addr=null;
    private String group_addr=null;

    public LOOPBACK() {
    }


    public String toString() {
        return "LOOPBACK(local address: " + local_addr + ')';
    }




    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "LOOPBACK";
    }



    public void init() throws Exception {
//        local_addr=new IpAddress("localhost", 10000) { // fake address
//            public String toString() {
//                return "<fake>";
//            }
//        };

          //local_addr=new org.jgroups.stack.IpAddress("localhost", 10000); // fake address
       local_addr = new IpAddress(12345);
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
        if(log.isTraceEnabled())
            log.trace("event is " + evt + ", group_addr=" + group_addr +
                      ", time is " + System.currentTimeMillis() + ", hdrs: " + Util.printEvent(evt));

        switch(evt.getType()) {

        case Event.MSG:
            Message msg=(Message)evt.getArg();
            Message rsp=msg.copy();
            if(rsp.getSrc() == null)
                rsp.setSrc(local_addr);

            //dest_addr=msg.getDest();
            //rsp.setDest(local_addr);
            //rsp.setSrc(dest_addr != null ? dest_addr : local_addr);
            up(new Event(Event.MSG, rsp));
            break;

        case Event.CONNECT:
        case Event.CONNECT_WITH_STATE_TRANSFER:    
            group_addr=(String)evt.getArg();
            break;
        }
        return null;
    }



    /*--------------------------- End of Protocol interface -------------------------- */


}
