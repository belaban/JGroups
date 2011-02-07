package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.IpAddress;
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
 * @since 2.12
 */
@Experimental
@MBean(description="RELAY protocol")
public class RELAY extends Protocol {

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Properties of the bridge cluster (e.g. tcp.xml)")
    protected String bridge_props=null;

    @Property(description="Name of the bridge cluster")
    protected String bridge_name="bridge-cluster";

    // @Property(description="If true, messages are relayed asynchronously, ie. via submission of a task to the timer thread pool")
    // protected boolean async=false;

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

    /** To generate new global views */
    protected long             global_view_id=0;

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

            case Event.GET_PHYSICAL_ADDRESS:
                // fix to prevent exception by JBossAS, which checks whether a physical
                // address is present and throw an ex if not
                PhysicalAddress addr=(PhysicalAddress)down_prot.down(evt);
                if(addr == null)
                    addr=new IpAddress(6666);
                return addr;
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
                            break;

                        case VIEW:
                            return installView(msg.getRawBuffer(), msg.getOffset(), msg.getLength());

                        case BROADCAST_VIEW:
                            sendViewOnLocalCluster(msg.getSrc(), remote_view, global_view, false);
                            break;

                        default:
                            throw new IllegalArgumentException(hdr.type + " is not a valid type");
                    }
                    return null;
                }

                if(is_coord && relay && (dest == null || dest.isMulticastAddress()) && !msg.isFlagSet(Message.NO_RELAY)) {
                    Message tmp=msg.copy(true, Global.BLOCKS_START_ID); // we only copy headers from building blocks
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
        local_view=view;
        coord=view.getMembers().firstElement();
        boolean create_bridge=false;

        boolean is_new_coord=Util.isCoordinator(view, local_addr);

        if(is_coord) {
            if(!is_new_coord) {
                if(log.isTraceEnabled())
                    log.trace("I'm not coordinator anymore, closing the channel");
                Util.close(bridge);
                is_coord=false;
                bridge=null;
            }
        }
        else {
            if(is_new_coord)
                is_coord=create_bridge=true;
        }


        if(is_coord) {
            sendViewOnLocalCluster(null, remote_view, generateGlobalView(view, remote_view), true);

            if(create_bridge) {
                createBridge();
                Message msg=new Message();
                msg.putHeader(id, RelayHeader.create(RELAY.RelayHeader.Type.BROADCAST_VIEW));
                try {
                    bridge.send(msg);
                }
                catch(Exception e) {
                }
            }
            
            sendViewToRemote(ViewData.create(view, null), false);
        }
    }


    protected Object installView(byte[] buf, int offset, int length) {
        try {
            ViewData data=(ViewData)Util.streamableFromByteBuffer(ViewData.class, buf, offset, length);
            if(data.uuids != null)
                UUID.add(data.uuids);

            remote_view=data.remote_view;
            if(global_view == null || (data.global_view != null &&!global_view.equals(data.global_view))) {
                global_view=data.global_view;
                synchronized(this) {
                    if(data.global_view.getVid().getId() > global_view_id)
                        global_view_id=data.global_view.getViewId().getId();
                }
                if(present_global_views)
                    return up_prot.up(new Event(Event.VIEW_CHANGE, global_view));
            }
        }
        catch(Exception e) {
            log.error("failed installing view", e);
        }
        return null;
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
        Message tmp=msg.copy(true, Global.BLOCKS_START_ID); // // we only copy headers from building blocks
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



    protected void sendViewToRemote(ViewData view_data, boolean use_seperate_thread) {
        try {
            byte[] buf=Util.streamableToByteBuffer(view_data);
            final Message msg=new Message(null, null, buf);
            msg.putHeader(id, RelayHeader.create(RelayHeader.Type.VIEW));
            if(bridge != null && bridge.isConnected()) {
                if(use_seperate_thread) {
                    timer.execute(new Runnable() {
                        public void run() {
                            try {
                                bridge.send(msg);
                            }
                            catch(Exception e) {
                                log.error("failed sending view to remote", e);
                            }
                        }
                    });
                }
                else
                    bridge.send(msg);
            }
        }
        catch(Exception e) {
            log.error("failed sending view to remote", e);
        }
    }



    protected View generateGlobalView(View local_view, View remote_view) {
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

        long new_view_id;
        synchronized(this) {
            new_view_id=global_view_id++;
        }

        return new View(local_addr, new_view_id, combined_members);
    }

    protected void createBridge() {
        try {
            if(log.isTraceEnabled())
                log.trace("I'm the coordinator, creating a channel (props=" + bridge_props + ", cluster_name=" + bridge_name + ")");
            bridge=new JChannel(bridge_props);
            bridge.setOpt(Channel.LOCAL, false); // don't receive my own messages
            bridge.setReceiver(new Receiver());
            bridge.connect(bridge_name);

        }
        catch(ChannelException e) {
            log.error("failed creating bridge channel (props=" + bridge_props + ")", e);
        }
    }


    protected class Receiver extends ReceiverAdapter {

        public void receive(Message msg) {
            Address sender=msg.getSrc();
            if(bridge.getAddress().equals(sender)) // discard my own messages
                return;
            RelayHeader hdr=(RelayHeader)msg.getHeader(id);
            switch(hdr.type) {
                case DISSEMINATE: // should not occur here, but we'll ignore it anyway
                    break;
                case FORWARD:
                    sendOnLocalCluster(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    break;
                case VIEW:
                    try {
                        ViewData data=(ViewData)Util.streamableFromByteBuffer(ViewData.class, msg.getRawBuffer(),
                                                                              msg.getOffset(), msg.getLength());
                        // replace addrs with proxies
                        if(data.remote_view != null) {
                            List<Address> mbrs=new LinkedList<Address>();
                            for(Address mbr: data.remote_view.getMembers()) {
                                mbrs.add(new ProxyAddress(local_addr, mbr));
                            }
                            data.remote_view=new View(data.remote_view.getViewId(), mbrs);
                        }
                        data.global_view=generateGlobalView(local_view, data.remote_view);
                        sendViewOnLocalCluster(null, data, false);
                    }
                    catch(Exception e) {
                        log.error("failed unmarshalling view from remote cluster", e);
                    }
                    break;
                case BROADCAST_VIEW: // no-op
                    // our local view is seen as the remote view on the other side !
                    sendViewToRemote(ViewData.create(local_view, null), true);
                    break;
                default:
                    throw new IllegalArgumentException(hdr.type + " is not a valid type");
            }
        }

        public void viewAccepted(View view) {
            if(bridge_view == null)
                bridge_view=view;
            else {
                if(!bridge_view.getVid().equals(view.getViewId())) {
                    bridge_view=view;

                    if(view.size() == 1 && bridge != null && bridge.isConnected() &&
                      view.getMembers().firstElement().equals(bridge.getAddress())) {
                        remote_view=null;
                        View new_global_view=generateGlobalView(local_view, null);
                        sendViewOnLocalCluster(null, null, new_global_view, false);
                    }
                    else {
                        // our local view is seen as the remote view on the other side !
                        // sendViewToRemote(ViewData.create(local_view, null), true);
                    }
                }
            }
        }
    }



    protected void sendOnLocalCluster(byte[] buf, int offset, int length) {
        try {
            Message msg=(Message)Util.streamableFromByteBuffer(Message.class, buf, offset, length);
            Address sender=msg.getSrc();
            ProxyAddress proxy_sender=new ProxyAddress(local_addr, sender);
            // msg.setSrc(proxy_sender);

            // set myself to be the sender
            msg.setSrc(local_addr);

            // later, in RELAY, we'll take the proxy_sender from the header and make it the sender
            msg.putHeader(id, RelayHeader.createDisseminateHeader(proxy_sender));

            if(log.isTraceEnabled())
                log.trace("received msg from " + sender + ", passing down the stack with dest=" +
                            msg.getDest() + " and src=" + msg.getSrc());
            down_prot.down(new Event(Event.MSG, msg));
        }
        catch(Exception e) {
            log.error("failed sending on local cluster", e);
        }
    }


    protected void sendViewOnLocalCluster(Address dest, View remote_view, View global_view, boolean use_seperate_thread) {
        sendViewOnLocalCluster(dest, ViewData.create(remote_view, global_view), use_seperate_thread);
    }


    protected void sendViewOnLocalCluster(Address dest, ViewData data, boolean use_seperate_thread) {
        try {
            final Message view_msg=new Message(dest, null, Util.streamableToByteBuffer(data));
            view_msg.putHeader(id, RelayHeader.create(RelayHeader.Type.VIEW));
            if(use_seperate_thread) {
                timer.execute(new Runnable() {
                    public void run() {
                        down_prot.down(new Event(Event.MSG, view_msg));
                    }
                });
            }
            else
                down_prot.down(new Event(Event.MSG, view_msg));
        }
        catch(Exception e) {
            log.error("failed sending view to local cluster", e);
        }
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
        protected View                remote_view;
        protected View                global_view;
        protected Map<Address,String> uuids;

        public ViewData() {
        }

        private ViewData(View remote_view, View global_view, Map<Address,String> uuids) {
            this.remote_view=remote_view;
            this.global_view=global_view;
            this.uuids=uuids;
        }

        public static ViewData create(View remote_view, View global_view) {
            Map<Address,String> tmp=UUID.getContents();
            View rv=remote_view != null? remote_view.copy() : null;
            View gv=global_view != null? global_view.copy() : null;
            return new ViewData(rv, gv, tmp);
        }


        public void writeTo(DataOutputStream out) throws IOException {
            Util.writeStreamable(remote_view, out);
            Util.writeStreamable(global_view, out);
            out.writeInt(uuids.size());
            for(Map.Entry<Address,String> entry: uuids.entrySet()) {
                Util.writeAddress(entry.getKey(), out);
                out.writeUTF(entry.getValue());
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            remote_view=(View)Util.readStreamable(View.class, in);
            global_view=(View)Util.readStreamable(View.class, in);
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
            sb.append("global_view: " + global_view).append(", remote_view: ").append(remote_view);
            return sb.toString();
        }
    }



}
