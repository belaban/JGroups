// $Id: LOOPBACK.java,v 1.32 2009/09/10 21:28:46 rachmatowicz Exp $

package org.jgroups.protocols;


import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;


/**
 Makes copies of outgoing messages, swaps sender and receiver and sends the message back up the stack.
 */
public class LOOPBACK extends TP {
    private String group_addr=null;
    private final PhysicalAddress physical_addr=new IpAddress(12345);
    private Address local_addr=null;

    public LOOPBACK() {
    }


    public String toString() {
        return "LOOPBACK(local address: " + local_addr + ')';
    }

    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
    }

    public String getInfo() {
        return null;
    }

    protected PhysicalAddress getPhysicalAddress() {
        return physical_addr;
    }

    /*------------------------------ Protocol interface ------------------------------ */

    public void init() throws Exception {
        super.init();
        
        // the bind address determination moved from TP
        Properties props = new Properties() ;
        if (bind_addr_str != null)
        	props.put("bind_addr", bind_addr_str) ;
        if (bind_interface_str != null)
        props.put("bind_interface", bind_interface_str) ;
        bind_addr = Util.getBindAddress(props) ;

        // the diagnostics determination moved from TP
        diagnostics_addr_str = DEFAULT_IPV4_DIAGNOSTICS_ADDR_STR ;        
                
        if(bind_addr != null) {
            Map<String, Object> m=new HashMap<String, Object>(1);
            m.put("bind_addr", bind_addr);
            up(new Event(Event.CONFIG, m));
        }

    }    
    
    public void destroy() {
        try {
            timer.stop();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
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

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
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