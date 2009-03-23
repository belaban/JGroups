// $Id: LOOPBACK.java,v 1.29 2009/03/23 19:40:40 vlada Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.jgroups.util.TimeScheduler;


/**
 Makes copies of outgoing messages, swaps sender and receiver and sends the message back up the stack.
 */
public class LOOPBACK extends TP {
    private String group_addr=null;

    public LOOPBACK() {
    }


    public String toString() {
        return "LOOPBACK(local address: " + local_addr + ')';
    }

    public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
    }

    public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
    }

    public String getInfo() {
        return null;
    }

    public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {
    }

    public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
    }

    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "LOOPBACK";
    }



    public void init() throws Exception {
        super.init();
//        local_addr=new IpAddress("localhost", 10000) { // fake address
//            public String toString() {
//                return "<fake>";
//            }
//        };

          //local_addr=new org.jgroups.stack.IpAddress("localhost", 10000); // fake address
       local_addr = new IpAddress(12345);
    }

    public void destroy() {
        System.out.println("destroy();");
        try {
            timer.stop();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
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
        case Event.CONNECT_USE_FLUSH:
        case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH: 	
            group_addr=(String)evt.getArg();
            break;
        }
        return null;
    }



    /*--------------------------- End of Protocol interface -------------------------- */


}
