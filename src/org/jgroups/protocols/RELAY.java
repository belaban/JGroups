package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

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

    @Property(description="Drops views received from below and instead generates global views and passes them up. " +
            "A global view consists of the local view and the remote view, ordered by view ID. If true, no protocol" +
            "which requires (local) views can sit on top of RELAY")
    protected boolean present_global_views=true;


    /* ---------------------------------------------    Fields    ------------------------------------------------ */
    protected Address          local_addr;
    @ManagedAttribute
    protected volatile boolean is_coord=false;
    protected volatile Address coord=null;

    /** The bridge between the two local clusters, usually based on a TCP config */
    protected JChannel         bridge;

    /** The view of the local cluster */
    protected View             local_view;

    /** The view of the bridge cluster, usually consists of max 2 nodes */
    protected View             bridge_view;

    /** The view of the remote cluster, typically all members are ProxyAddresses */
    protected View             remote_view;

    /** The combined view of local and remote cluster */
    protected View             global_view;

    protected TimeScheduler    timer;





    @ManagedOperation
    public void setRelay(boolean relay) {
        this.relay=relay;
    }

    @ManagedAttribute
    public String getLocalView() {
        return local_view != null? local_view.toString() : "n/a";
    }

    @ManagedAttribute
    public String getBridgeView() {
        return bridge_view != null? bridge_view.toString() : "n/a";
    }

    @ManagedAttribute
    public String getRemoteView() {
        return remote_view != null? remote_view.toString() : "n/a";
    }

    @ManagedAttribute
    public String getGlobalView() {
        return global_view != null? global_view.toString() : "n/a";
    }


    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
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

            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                Object retval=down_prot.down(evt);

                if(coord != null) {
                    Message broadcast_view_req=new Message(coord, null, null);
                    broadcast_view_req.putHeader(id, RelayHeader.create(RelayHeader.Type.BROADCAST_VIEW));
                    down_prot.down(new Event(Event.MSG, broadcast_view_req));
                }
                return retval;

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
                            if(is_coord)
                                forward(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                            else
                                log.warn("Cannot forward message as I'm not coordinator");
                            break;
                        case VIEW:
                            try {
                                ViewData view_data=(ViewData)Util.streamableFromByteBuffer(ViewData.class, msg.getRawBuffer(),
                                                                                           msg.getOffset(), msg.getLength());
                                // add UUID info:
                                if(view_data.uuids != null)
                                    UUID.add(view_data.uuids);

                                boolean generate_global_view=false;
                                if(view_data.local_view != null && (local_view == null || !local_view.equals(view_data.local_view))) {
                                    generate_global_view=true;
                                    local_view=view_data.local_view;
                                }

                                if(view_data.remote_view != null && (remote_view == null || !remote_view.equals(view_data.remote_view))) {
                                    remote_view=view_data.remote_view;
                                    generate_global_view=true;
                                }

                                if(generate_global_view) {
                                    global_view=generateGlobalView();
                                    if(present_global_views)
                                        return up_prot.up(new Event(Event.VIEW_CHANGE, global_view));
                                }
                            }
                            catch(Exception e) {
                                log.error("failed unmarshalling VIEW", e);
                            }
                            break;

                        case BROADCAST_VIEW:
                            ViewData view_data=null;
                            final Address sender=msg.getSrc();
                            try {
                                view_data=ViewData.create(local_view, remote_view);
                                final Message view_msg=new Message(sender, null, Util.streamableToByteBuffer(view_data));
                                view_msg.putHeader(id, RelayHeader.create(RelayHeader.Type.VIEW));
                                down_prot.down(new Event(Event.MSG, view_msg));
                            }
                            catch(Throwable e) {
                                log.error("failed sending view data to " + sender, e);
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
                handleView((View)evt.getArg()); // already sends up new view if needed
                if(present_global_views)
                    return null;
                else
                    break;
        }
        return up_prot.up(evt);
    }




    protected void handleView(final View view) {
        coord=view.getMembers().firstElement();

        // Set view
        if(local_view == null || !local_view.getVid().equals(view.getViewId())) {
            // local_view=view; // set later

            boolean create_bridge=false;

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
                    create_bridge=true;
                }
            }


            if(is_coord) {
                ViewData view_data=null;
                try {
                    view_data=ViewData.create(view, remote_view);
                    final Message view_msg=new Message(null, null, Util.streamableToByteBuffer(view_data));
                    view_msg.putHeader(id, RelayHeader.create(RelayHeader.Type.VIEW));
                    timer.execute(new Runnable() {
                        public void run() {
                            down_prot.down(new Event(Event.MSG, view_msg));
                        }
                    });
                }
                catch(Throwable e) {
                    log.error("failed sending view data to local cluster", e);
                }

                if(create_bridge) {
                    try {
                        if(log.isTraceEnabled())
                            log.trace("I'm the coordinator, creating a channel (props=" + props + ", cluster_name=" + cluster_name + ")");
                        bridge=new JChannel(props);
                        bridge.setOpt(Channel.LOCAL, false); // don't receive my own messages
                        bridge.setReceiver(new Receiver());
                        bridge.connect(cluster_name);
                        if(view_data != null)
                            sendViewToRemote(view_data);
                    }
                    catch(ChannelException e) {
                        log.error("failed creating channel (props=" + props + ")", e);
                    }
                }
            }
        }
    }


    /** Forwards the message across the TCP link to the other local cluster */
    protected void forward(byte[] buffer, int offset, int length) {
        Message msg=new Message(null, null, buffer, offset, length);
        msg.putHeader(id, new RelayHeader(RelayHeader.Type.FORWARD));
        if(bridge != null) {
            try {
                bridge.send(msg);
            }
            catch(Throwable t) {
                log.error("failed forwarding message over bridge", t);
            }
        }
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



    protected void sendViewToRemote(ViewData view_data) {
        try {
            byte[] buf=Util.streamableToByteBuffer(view_data);
            final Message msg=new Message(null, null, buf);
            msg.putHeader(id, RelayHeader.create(RelayHeader.Type.VIEW));
            if(bridge != null && bridge.isConnected()) {
                bridge.send(msg);
            }
        }
        catch(Exception e) {
            log.error("failed sending view to remote", e);
        }
    }



    protected View generateGlobalView() {
        List<View> views=new ArrayList<View>(2);
        if(local_view != null) views.add(local_view);
        if(remote_view != null) views.add(remote_view);
        Collections.sort(views, new Comparator<View>() {
            public int compare(View v1, View v2) {
                ViewId vid1=v1.getViewId(), vid2=v2.getViewId();
                Address creator1=vid1.getCoordAddress(), creator2=vid2.getCoordAddress();
                int rc=creator1.compareTo(creator2);
                if(rc != 0)
                    return rc;
                long id1=vid1.getId(), id2=vid2.getId();
                return id1 > id2 ? 1 : id1 < id2? -1 : 0;
            }
        });

        Collection<Address> combined_members=new LinkedList<Address>();
        for(View view: views)
            combined_members.addAll(view.getMembers());
        return new View(views.get(0).getViewId(), combined_members);
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
                    try {
                        ViewData data=(ViewData)Util.streamableFromByteBuffer(ViewData.class, msg.getRawBuffer(),
                                                                              msg.getOffset(), msg.getLength());
                        // swap local and remote views and null remote view
                        data.remote_view=data.local_view;
                        data.local_view=null;

                        UUID.add(data.uuids); // todo: remove
                        System.out.println("received view from remote: " + data);

                        Message view_msg=new Message(null, null, Util.streamableToByteBuffer(data));
                        view_msg.putHeader(id, RelayHeader.create(RelayHeader.Type.VIEW));
                        down_prot.down(new Event(Event.MSG, view_msg));
                    }
                    catch(Exception e) {
                        log.error("failed unmarshalling view from remote cluster", e);
                    }
                    break;
                case BROADCAST_VIEW: // no-op
                    break;
                default:
                    throw new IllegalArgumentException(hdr.type + " is not a valid type");
            }
        }

        public void viewAccepted(View view) {
            if(bridge_view == null)
                bridge_view=view;
            else {

                System.out.println("-- bridge_view: " + bridge_view + ", view: " + view);

                if(!bridge_view.getVid().equals(view.getViewId()) && view.size() > 1) {
                    bridge_view=view;
                    sendViewToRemote(ViewData.create(local_view, remote_view));
                }
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
        public static enum Type {DISSEMINATE, FORWARD, VIEW, BROADCAST_VIEW};
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
                case VIEW:
                case BROADCAST_VIEW:
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
                case BROADCAST_VIEW:
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
                case BROADCAST_VIEW:
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
                case BROADCAST_VIEW:
                    break;
            }
            return sb.toString();
        }
    }

    /** Contains local and remote views, and UUID information */
    protected static class ViewData implements Streamable {
        protected View                local_view;
        protected View                remote_view;
        protected Map<Address,String> uuids;

        public ViewData() {
        }

        private ViewData(View local_view, View remote_view, Map<Address,String> uuids) {
            this.local_view=local_view;
            this.remote_view=remote_view;
            this.uuids=uuids;
        }

        public static ViewData create(View local_view, View remote_view) {
            Map<Address,String> tmp=UUID.getContents();
            View lv=local_view != null? local_view.copy() : null;
            View rv=remote_view != null? remote_view.copy() : null;
            return new ViewData(lv, rv, tmp);
        }


        public void writeTo(DataOutputStream out) throws IOException {
            Util.writeStreamable(local_view, out);
            Util.writeStreamable(remote_view, out);
            out.writeInt(uuids.size());
            for(Map.Entry<Address,String> entry: uuids.entrySet()) {
                Util.writeAddress(entry.getKey(), out);
                out.writeUTF(entry.getValue());
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            local_view=(View)Util.readStreamable(View.class, in);
            remote_view=(View)Util.readStreamable(View.class, in);
            int size=in.readInt();
            uuids=new HashMap<Address,String>();
            for(int i=0; i < size; i++) {
                Address addr=Util.readAddress(in);
                String name=in.readUTF();
                uuids.put(addr, name);
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("local view: " + local_view).append(", remote_view: ").append(remote_view);
            return sb.toString();
        }
    }



}
