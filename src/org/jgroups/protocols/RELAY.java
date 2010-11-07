package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Simple relaying protocol: RELAY is added to the top of the stack, creates a channel to a bridge cluster,
 * and - if coordinator - relays all multicast messages via the bridge cluster to the remote cluster.<p/>
 *
 * This is <em>not</em> a big virtual cluster, e.g. consisting of {A,B,C,X,Y,Z}, but 2 clusters {A,B,C} and {X,Y,Z},
 * bridged together by RELAY. For example, when B multicasts a message M, A (if it happens to be the coord) relays M
 * to X (which happens to be the other cluster's coordinator). X then re-broadcasts M, but under its own address, so
 * M.src = X (<em>not</em> B !)<p/>
 *
 * See [1] and [2] for details.<p/>
 * [1] https://jira.jboss.org/browse/JGRP-747<p/>
 * [2] doc/design/DataCenterReplication.txt
 *
 * @author Bela Ban
 * @version $Id: RELAY.java,v 1.4 2010/08/18 13:07:03 belaban Exp $
 */
@Experimental @Unsupported
@MBean(description="RELAY protocol")
public class RELAY extends Protocol {

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Properties of the bridge cluster (e.g. tcp.xml)")
    protected String props=null;

    @Property(description="Name of the bridge cluster")
    protected String cluster_name="bridge-cluster";

    @Property(description="If true, messages are relayed asynchronously, ie. via submission of a task to the timer thread pool")
    protected boolean async=false;

    @Property(description="If set to false, don't perform relaying. Used e.g. for backup clusters; " +
            "unidirectional replication from one cluster to another, but not back. Can be changed at runtime")
    protected boolean relay=true;


    /* ---------------------------------------------    Fields    ------------------------------------------------ */
    protected Address local_addr;

    @ManagedAttribute
    protected volatile boolean is_coord=false;

    protected JChannel ch;

    protected TimeScheduler timer;




    @ManagedOperation
    public void setRelay(boolean relay) {
        this.relay=relay;
    }

    public void init() throws Exception {
        timer=getTransport().getTimer();
    }

    public void stop() {
        Util.close(ch);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
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
                if(is_coord && relay && ch != null) {
                    Message msg=(Message)evt.getArg();
                    Address dest=msg.getDest();
                    RelayHeader hdr=(RelayHeader)msg.getHeader(getId());
                    boolean multicast=dest == null || dest.isMulticastAddress();
                    if(multicast && hdr == null)
                        relay(msg);
                }
                break;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
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

    protected void relay(Message msg) {
        final Message copy=msg.copy(true, false);
        if(async) {
            timer.execute(new Runnable() {
                public void run() {
                    _relay(copy);
                }
            });
        }
        else {
            _relay(copy);
        }
    }


    protected void _relay(Message msg) {
        try {
            if(log.isTraceEnabled())
                log.trace("relaying message from " + msg.getSrc());
            ch.send(msg);
        }
        catch(Throwable e) {
            log.error("failed relaying message " + msg, e);
        }
    }


    protected class Receiver extends ReceiverAdapter {
        public void receive(Message msg) {
            Message copy=msg.copy(true, false); // copy the payload and everything else but the headers
            copy.putHeader(getId(), new RelayHeader());
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
