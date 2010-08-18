package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Simple relaying protocol: RELAY is added to the top of the stack, creates a channel to a bridge cluster,
 * and - if coordinator - relays all multicast messages via the bridge cluster to the remote relay.  
 * @author Bela Ban
 * @version $Id: RELAY.java,v 1.1 2010/08/18 09:46:12 belaban Exp $
 */
@Experimental @Unsupported
public class RELAY extends Protocol {

    @Property(description="Properties of the bridge cluster (e.g. tcp.xml)")
    protected String props=null;

    @Property(description="Name of the bridge cluster")
    protected String cluster_name="bridge-cluster";

    protected Address local_addr;

    protected volatile boolean is_coord=false;

    protected JChannel ch;


    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                if(is_coord && ch != null) {
                    Message msg=(Message)evt.getArg();
                    Address dest=msg.getDest();
                    if(dest == null || dest.isMulticastAddress()) {
                        Message copy=msg.copy(true, false);
                        copy.setSrc(local_addr);
                        try {
                            if(log.isTraceEnabled())
                                log.trace("down(): relaying message from " + copy.getSrc());
                            ch.send(copy);
                        }
                        catch(Throwable e) {
                            log.error("failed forwarding message " + copy, e);
                        }
                    }
                }
                break;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;

            case Event.DISCONNECT:
                Util.close(ch);
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }



        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                if(is_coord && ch != null) {
                    Message msg=(Message)evt.getArg();
                    Address dest=msg.getDest();
                    if((dest == null || dest.isMulticastAddress()) && !local_addr.equals(msg.getSrc())) {
                        Message copy=msg.copy(true, false);
                        try {
                            if(log.isTraceEnabled())
                                log.trace("up(): relaying message from " + copy.getSrc());
                            ch.send(copy);
                        }
                        catch(Throwable e) {
                            log.error("failed forwarding message " + copy, e);
                        }
                    }
                }
                break;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }


        return up_prot.up(evt);
    }

    public void stop() {
        Util.close(ch);
    }


    protected void handleView(View view) {
        if(is_coord) {
            if(!Util.isCoordinator(view, local_addr)) {
                if(log.isTraceEnabled())
                    log.trace("I'm not coordinator anymore, closing the channel");
                Util.close(ch);
                ch=null;
            }
        }
        else {
            if(Util.isCoordinator(view, local_addr)) {
                is_coord=true;
                try {
                    if(log.isTraceEnabled())
                        log.trace("I'm the coordinator, creating a channel (props=" + props + ", cluster_name=" + cluster_name + ")");
                    ch=new JChannel(props);
                    ch.setOpt(Channel.LOCAL, false); // don't receive my own messages
                    ch.connect(cluster_name);
                    ch.setReceiver(new Receiver());
                }
                catch(ChannelException e) {
                    log.error("failed creating channel (props=" + props + ")", e);
                }
            }

        }
    }


    protected class Receiver extends ReceiverAdapter {
        public void receive(Message msg) {
            Message copy=msg.copy(true, false); // copy the payload and everything else but the headers
            copy.setSrc(local_addr);
            if(log.isTraceEnabled())
                log.trace("received msg from " + msg.getSrc() + ", passing down the stack with dest=" +
                        copy.getDest() + " and src=" + local_addr);
            down_prot.down(new Event(Event.MSG, copy));
        }
    }


    public static class RelayHeader extends Header {

        public RelayHeader() {
        }

        public int size() {
            return 0;
        }

        public void writeTo(DataOutputStream out) throws IOException {
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        }
    }
}
