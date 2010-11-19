package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ProxyAddress;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple relaying protocol: RELAY is added to the top of the stack, creates a channel to a bridge cluster,
 * and - if coordinator - relays all multicast messages via the bridge cluster to the remote cluster.<p/>
 *
 * This is <em>not</em> a big virtual cluster, e.g. consisting of {A,B,C,X,Y,Z}, but 2 <em>autonomous</em> clusters
 * {A,B,C} and {X,Y,Z}, bridged together by RELAY. For example, when B multicasts a message M, A (if it happens to be
 * the coord) relays M to X (which happens to be the other cluster's coordinator). X then re-broadcasts M, with M.src
 * being a ProxyUUID(X,B). This means that the sender of M in the {X,Y,Z} cluster will be X for all practical purposes,
 * but the original sender B is also recorded, for sending back a response.<p/>
 *
 * See [1] and [2] for details.<p/>
 * [1] https://jira.jboss.org/browse/JGRP-747<p/>
 * [2] doc/design/RELAY.txt
 *
 * @author Bela Ban
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
    protected Address          local_addr;
    @ManagedAttribute
    protected volatile boolean is_coord=false;
    protected volatile Address coord=null;
    protected JChannel         bridge;
    protected View             local_view, remote_view;





    @ManagedOperation
    public void setRelay(boolean relay) {
        this.relay=relay;
    }

    @ManagedAttribute
    public String getLocalView() {
        return local_view != null? local_view.toString() : "n/a";
    }

    @ManagedAttribute
    public String getRemoteView() {
        return remote_view != null? remote_view.toString() : "n/a";
    }

    @ManagedAttribute
    public String getGlobalView() {
        return "n/a";
    }


    public void stop() {
        Util.close(bridge);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.getDest() instanceof ProxyAddress) {
                    forwardToCoord(msg);
                    return null;
                }
                break;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;

            case Event.DISCONNECT:
                Util.close(bridge);
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
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                RelayHeader hdr=(RelayHeader)msg.getHeader(getId());
                if(hdr != null) {
                    switch(hdr.type) {
                        case DISSEMINATE:
                            Message copy=msg.copy();
                            if(hdr.original_sender != null)
                                copy.setSrc(hdr.original_sender);
                            return up_prot.up(new Event(Event.MSG, copy));

                        case FORWARD:
                            if(is_coord) {
                                try {
                                    forward(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                                }
                                catch(Exception e) {
                                    log.error("failed forwarding message", e);
                                }
                            }
                            else
                                log.warn("Cannot forward message as I'm not coordinator");
                            break;
                        case VIEW:
                            // todo: handle RELAY view, probably needs to invoke viewAccepted on the app...
                            break;
                        case UUIDS:
                            try {
                                Map<Address,String> uuids=uuidsFromBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                                UUID.add(uuids);
                            }
                            catch(Exception e) {
                                log.error("failed reading UUID information from message", e);
                            }

                            break;
                        default:
                            throw new IllegalArgumentException(hdr.type + " is not a valid type");
                    }
                    return null;
                }

                if(is_coord && relay && (dest == null || dest.isMulticastAddress())) {
                    Message tmp=msg.copy(true, false);
                    try {
                        byte[] buf=Util.streamableToByteBuffer(tmp);
                        forward(buf, 0, buf.length);
                    }
                    catch(Exception e) {
                        log.warn("failed relaying message", e);
                    }
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
                Util.close(bridge);
                bridge=null;
            }
        }
        else {
            if(Util.isCoordinator(view, local_addr)) {
                is_coord=true;
                try {
                    if(log.isTraceEnabled())
                        log.trace("I'm the coordinator, creating a channel (props=" + props + ", cluster_name=" + cluster_name + ")");
                    bridge=new JChannel(props);
                    bridge.setOpt(Channel.LOCAL, false); // don't receive my own messages
                    bridge.connect(cluster_name);
                    bridge.setReceiver(new Receiver());
                    sendUUIDs(true);
                }
                catch(ChannelException e) {
                    log.error("failed creating channel (props=" + props + ")", e);
                }
            }
        }

        coord=view.getMembers().firstElement();

        // Set view
        if(local_view == null || !local_view.getVid().equals(view.getViewId())) {
            local_view=view;
            if(is_coord) {
                sendUUIDs(true);
            }
        }
    }


    /** Forwards the message across the TCP link to the other local cluster */
    protected void forward(byte[] buffer, int offset, int length) throws Exception {
        Message msg=new Message(null, null, buffer, offset, length);
        msg.putHeader(id, new RelayHeader(RelayHeader.Type.FORWARD));
        if(bridge != null)
            bridge.send(msg);
    }

    /** Wraps the message annd sends it to the current coordinator */
    protected void forwardToCoord(Message msg) {
        Message tmp=msg.copy(true, false); // don't copy headers
        if(tmp.getSrc() == null)
            tmp.setSrc(local_addr);
        ProxyAddress dst=(ProxyAddress)tmp.getDest();
        tmp.setDest(dst.getOriginalAddress());
        try {
            byte[] buf=Util.streamableToByteBuffer(tmp);
            if(coord != null) {
                tmp=new Message(coord, null, buf, 0, buf.length); // reusing tmp is OK here ...
                tmp.putHeader(id, new RelayHeader(RelayHeader.Type.FORWARD));
                down_prot.down(new Event(Event.MSG, tmp));
            }
        }
        catch(Exception e) {
            log.error("failed forwarding unicast message to coord", e);
        }
    }


    protected void sendUUIDs(boolean remote) {
        Map<Address,String> contents=UUID.getContents();
        RelayHeader hdr=RelayHeader.create(RelayHeader.Type.UUIDS);
        Message msg=new Message();
        msg.putHeader(id, hdr);
        try {
            byte[] buf=uuidsToBuffer(contents);
            msg.setBuffer(buf);
            if(remote) {
                if(bridge != null) {
                    bridge.send(msg);
                }
            }
            else {
                down_prot.down(new Event(Event.MSG, msg));
            }
        }
        catch(Exception e) {
            log.error("failed sending UUID information", e);
        }
    }

    protected static byte[] uuidsToBuffer(Map<Address,String> uuids) throws IOException {
        ByteArrayOutputStream output=null;
        DataOutputStream out=null;

        output=new ByteArrayOutputStream();
        out=new DataOutputStream(output);
        if(uuids == null)
            out.writeInt(0);
        else {
            out.writeInt(uuids.size());
            for(Map.Entry<Address,String> entry: uuids.entrySet()) {
                Util.writeAddress(entry.getKey(), out);
                out.writeUTF(entry.getValue());
            }
        }
        return output.toByteArray();
    }

    protected static Map<Address,String> uuidsFromBuffer(byte[] buf, int offset, int length) throws Exception {
        ByteArrayInputStream input=new ByteArrayInputStream(buf, offset, length);
        DataInputStream in=new DataInputStream(input);
        Map<Address,String> retval=new HashMap<Address,String>();

        int size=in.readInt();
        if(size == 0)
            return retval;

        for(int i=0; i < size; i++) {
            Address addr=Util.readAddress(in);
            String name=in.readUTF();
            retval.put(addr, name);
        }
        return retval;
    }


    protected class Receiver extends ReceiverAdapter {

        public void receive(Message msg) {
            Address sender=msg.getSrc();
            if(bridge.getAddress().equals(sender)) // discard my own messages
                return;
            RelayHeader hdr=(RelayHeader)msg.getHeader(id);
            switch(hdr.type) {
                case DISSEMINATE:
                    // should not occur here, but we'll ignore it anyway
                    break;
                case FORWARD:
                    try {
                        Message tmp=(Message)Util.streamableFromByteBuffer(Message.class, msg.getRawBuffer(),
                                                                           msg.getOffset(), msg.getLength());
                        putOnLocalCluster(tmp);
                    }
                    catch(Exception e) {
                        log.error("failed unserializing forwarded message", e);
                    }
                    break;
                case VIEW:
                    // todo: somehow send the view around in this local cluster
                    break;
                case UUIDS:
                    Message uuid_msg=new Message(null, null, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    uuid_msg.putHeader(id, hdr);
                    down_prot.down(new Event(Event.MSG, uuid_msg));
                    break;
                default:
                    throw new IllegalArgumentException(hdr.type + " is not a valid type");
            }
        }

        public void viewAccepted(View view) {
            if(remote_view == null || !remote_view.getVid().equals(view.getViewId())) {
                remote_view=view;
                sendUUIDs(true);
                sendUUIDs(false);
            }
        }
    }

    protected void putOnLocalCluster(Message msg) {
        Address sender=msg.getSrc();
        ProxyAddress proxy_sender=new ProxyAddress(local_addr, sender);
        msg.setSrc(proxy_sender);
        msg.putHeader(id, RelayHeader.createDisseminateHeader(proxy_sender));

        if(log.isTraceEnabled())
            log.trace("received msg from " + sender + ", passing down the stack with dest=" +
                    msg.getDest() + " and src=" + msg.getSrc());

        down_prot.down(new Event(Event.MSG, msg));
    }

    

    public static class RelayHeader extends Header {
        public static enum Type {DISSEMINATE, FORWARD, VIEW, UUIDS};
        protected Type                type;
        protected Address             original_sender; // with DISSEMINATE


        public RelayHeader() {
        }

        private RelayHeader(Type type) {
            this.type=type;
        }


        public static RelayHeader create(Type type) {
            return new RelayHeader(type);
        }

        public static RelayHeader createDisseminateHeader(Address original_sender) {
            RelayHeader retval=new RelayHeader(Type.DISSEMINATE);
            retval.original_sender=original_sender;
            return retval;
        }



        public int size() {
            int retval=Global.BYTE_SIZE; // type
            switch(type) {
                case DISSEMINATE:
                    retval+=Util.size(original_sender);
                    break;
                case FORWARD:
                    break;
                case VIEW:
                    break;
                case UUIDS:
                    break;
            }
            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type.ordinal());
            switch(type) {
                case DISSEMINATE:
                    Util.writeAddress(original_sender, out);
                    break;
                case FORWARD:
                case VIEW:
                    break;
                case UUIDS:
                    break;
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=Type.values()[in.readByte()];
            switch(type) {
                case DISSEMINATE:
                    original_sender=Util.readAddress(in);
                    break;
                case FORWARD:
                case VIEW:
                    break;
                case UUIDS:
                    break;
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(type.toString());
            switch(type) {
                case DISSEMINATE:
                    sb.append(" (original sender=" + original_sender + ")");
                    break;
                case FORWARD:
                case VIEW:
                    break;
                case UUIDS:
                    break;
            }
            return sb.toString();
        }
    }


    
}
