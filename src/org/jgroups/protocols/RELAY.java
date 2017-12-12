package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
@MBean(description="RELAY protocol")
public class RELAY extends Protocol {

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Description of the local cluster, e.g. \"nyc\". This is added to every address, so it" +
      "should be short. This is a mandatory property and must be set",writable=false)
    protected String site;

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

    /** The view of the remote cluster */
    protected View             remote_view;

    /** The combined view of local and remote cluster */
    protected View             global_view;

    /** To generate new global views */
    protected long             global_view_id=0;

    protected TimeScheduler    timer;

    protected Future<?>        remote_view_fetcher_future;

    protected static final byte[] SITE_ID=Util.stringToBytes("site-id");


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
        if(site == null || site.isEmpty())
            throw new IllegalArgumentException("\"site\" must be set");
        timer=getTransport().getTimer();
        JChannel channel=getProtocolStack().getChannel();
        if(channel == null)
            throw new IllegalStateException("channel must be set");
        channel.addAddressGenerator(() -> ExtendedUUID.randomUUID().put(SITE_ID, Util.stringToBytes(site)));
    }

    public void stop() {
        stopRemoteViewFetcher();
        Util.close(bridge);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;

            case Event.DISCONNECT:
                Util.close(bridge);
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;

            case Event.GET_PHYSICAL_ADDRESS:
                // fix to prevent exception by JBossAS, which checks whether a physical
                // address is present and throw an ex if not
                // Remove this when the AS code removes that check
                PhysicalAddress addr=(PhysicalAddress)down_prot.down(evt);
                if(addr == null)
                    addr=new IpAddress(6666);
                return addr;
        }
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        Address dest=msg.getDest();
        if(dest == null)
            return down_prot.down(msg);

        // forward non local destinations to the coordinator, to relay to the remote cluster
        if(!isLocal(dest)) {
            forwardToCoord(msg);
            return null;
        }
        return down_prot.down(msg);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg()); // already sends up new view if needed
                if(present_global_views)
                    return null;
                else
                    break;
        }
        return up_prot.up(evt);
    }


    public Object up(Message msg) {
        Address dest=msg.getDest();
        RelayHeader hdr=msg.getHeader(getId());
        if(hdr != null)
            return handleUpEvent(msg, hdr);

        if(is_coord && relay && dest == null && !msg.isFlagSet(Message.Flag.NO_RELAY)) {
            Message tmp=msg.copy(true, Global.BLOCKS_START_ID); // we only copy headers from building blocks
            try {
                byte[] buf=Util.streamableToByteBuffer(tmp);
                forward(buf, 0, buf.length);
            }
            catch(Exception e) {
                log.warn("failed relaying message", e);
            }
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            RelayHeader hdr=msg.getHeader(getId());
            if(hdr != null) {
                batch.remove(msg);
                try {
                    handleUpEvent(msg, hdr);
                    continue; // fix for https://issues.jboss.org/browse/JGRP-2073
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("FailedProcessingMessage"), t);
                }
            }

            // Leave the messages in the batch: they're going to be forwarded, but we also need to deliver them locally
            if(is_coord && relay && msg.dest() == null && !msg.isFlagSet(Message.Flag.NO_RELAY)) {
                Message tmp=msg.copy(true, Global.BLOCKS_START_ID); // we only copy headers from building blocks
                try {
                    byte[] buf=Util.streamableToByteBuffer(tmp);
                    forward(buf, 0, buf.length);
                }
                catch(Exception e) {
                    log.warn("failed relaying message", e);
                }
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected Object handleUpEvent(Message msg, RelayHeader hdr) {
        switch(hdr.type) {
            case DISSEMINATE:
                Message copy=msg.copy();
                if(hdr.original_sender != null)
                    copy.setSrc(hdr.original_sender);
                return up_prot.up(copy);

            case FORWARD:
                if(is_coord)
                    forward(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                break;

            case VIEW:
                return installView(msg.getRawBuffer(), msg.getOffset(), msg.getLength());

            case BROADCAST_VIEW:
                break;

            default:
                throw new IllegalArgumentException(hdr.type + " is not a valid type");
        }
        return null;
    }


    protected void handleView(final View view) {
        List<Address> new_mbrs=null;
        if(local_view != null)
            new_mbrs=Util.newMembers(local_view.getMembers(), view.getMembers());
        local_view=view;
        coord=view.getMembers().iterator().next();
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
        else if(is_new_coord)
            is_coord=create_bridge=true;

        if(is_coord) {
            // need to have a local view before JChannel.connect() returns; we don't want to change the viewAccepted() semantics
            sendViewOnLocalCluster(remote_view, generateGlobalView(view, remote_view, view instanceof MergeView),
                                   true, new_mbrs);
            if(create_bridge)
                createBridge();
            sendViewToRemote(ViewData.create(view, null), false);
        }
    }


    protected Object installView(byte[] buf, int offset, int length) {
        try {
            ViewData data=Util.streamableFromByteBuffer(ViewData::new, buf, offset, length);
            if(data.uuids != null)
                NameCache.add(data.uuids);

            remote_view=data.remote_view;
            if(global_view == null || (data.global_view != null &&!global_view.equals(data.global_view))) {
                global_view=data.global_view;
                synchronized(this) {
                    if(data.global_view.getViewId().getId() > global_view_id)
                        global_view_id=data.global_view.getViewId().getId();
                }
                if(present_global_views)
                    return up_prot.up(new Event(Event.VIEW_CHANGE, global_view));
            }
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedInstallingView"), e);
        }
        return null;
    }


    /** Forwards the message across the TCP link to the other local cluster */
    protected void forward(byte[] buffer, int offset, int length) {
        Message msg=new Message(null, buffer, offset, length).putHeader(id, new RelayHeader(RelayHeader.Type.FORWARD));
        if(bridge != null) {
            try {
                bridge.send(msg);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedForwardingMessageOverBridge"), t);
            }
        }
    }

    /** Wraps the message annd sends it to the current coordinator */
    protected void forwardToCoord(Message msg) {
        Message tmp=msg.copy(true, Global.BLOCKS_START_ID); // // we only copy headers from building blocks
        if(tmp.getSrc() == null)
            tmp.setSrc(local_addr);
        
        try {
            byte[] buf=Util.streamableToByteBuffer(tmp);
            if(coord != null) {
                // optimization: if I'm the coord, simply relay to the remote cluster via the bridge
                if(coord.equals(local_addr)) {
                    forward(buf, 0, buf.length);
                    return;
                }

                tmp=new Message(coord, buf, 0, buf.length) // reusing tmp is OK here ...
                  .putHeader(id, new RelayHeader(RelayHeader.Type.FORWARD));
                down_prot.down(tmp);
            }
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedForwardingUnicastMessageToCoord"), e);
        }
    }



    protected void sendViewToRemote(ViewData view_data, boolean use_seperate_thread) {
        try {
            if(bridge != null && bridge.isConnected()) {
                byte[] buf=Util.streamableToByteBuffer(view_data);
                final Message msg=new Message(null, buf).putHeader(id, RelayHeader.create(RelayHeader.Type.VIEW));
                if(use_seperate_thread) {
                    timer.execute(() -> {
                        try {
                            bridge.send(msg);
                        }
                        catch(Exception e) {
                            log.error(Util.getMessage("FailedSendingViewToRemote"), e);
                        }
                    });
                }
                else
                    bridge.send(msg);
            }
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedSendingViewToRemote"), e);
        }
    }



    protected View generateGlobalView(View local_view, View remote_view) {
        return generateGlobalView(local_view, remote_view, false);
    }

    protected View generateGlobalView(View local_view, View remote_view, boolean merge) {
        List<View> views=new ArrayList<>(2);
        if(local_view != null) views.add(local_view);
        if(remote_view != null) views.add(remote_view);
        Collections.sort(views, (v1, v2) -> {
            ViewId vid1=v1.getViewId(), vid2=v2.getViewId();
            Address creator1=vid1.getCreator(), creator2=vid2.getCreator();
            int rc=creator1.compareTo(creator2);
            if(rc != 0)
                return rc;
            long id1=vid1.getId(), id2=vid2.getId();
            return id1 > id2 ? 1 : id1 < id2? -1 : 0;
        });

        List<Address> combined_members=new ArrayList<>();
        for(View view: views)
            combined_members.addAll(view.getMembers());

        long new_view_id;
        synchronized(this) {
            new_view_id=global_view_id++;
        }
        Address view_creator=combined_members.isEmpty()? local_addr : combined_members.get(0);
        if(merge)
            return new MergeView(view_creator, new_view_id, combined_members, views);
        else
            return new View(view_creator, new_view_id, combined_members);
    }

    protected void createBridge() {
        try {
            if(log.isTraceEnabled())
                log.trace("I'm the coordinator, creating a channel (props=" + bridge_props + ", cluster_name=" + bridge_name + ")");
            bridge=new JChannel(bridge_props);
            bridge.setDiscardOwnMessages(true); // don't receive my own messages
            bridge.setReceiver(new Receiver());
            bridge.connect(bridge_name);
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedCreatingBridgeChannelProps") + bridge_props + ")", e);
        }
    }


    protected void sendOnLocalCluster(byte[] buf, int offset, int length) {
        try {
            Message msg=Util.streamableFromByteBuffer(Message::new, buf, offset, length);
            Address sender=msg.getSrc();
            Address dest=msg.getDest();

            if(!isLocal(dest)) {
                if(log.isWarnEnabled())
                    log.warn("[" + local_addr + "] dest=" + dest + " is not local (site=" + this.site + "); discarding it");
                return;
            }

            // set myself to be the sender
            msg.setSrc(local_addr);

            // later, in RELAY, we'll take the original sender from the header and make it the sender
            msg.putHeader(id, RelayHeader.createDisseminateHeader(sender));

            if(log.isTraceEnabled())
                log.trace("received msg from " + sender + ", passing down the stack with dest=" +
                            msg.getDest() + " and src=" + msg.getSrc());
            down_prot.down(msg);
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedSendingOnLocalCluster"), e);
        }
    }


    protected void sendViewOnLocalCluster(View remote_view, View global_view,
                                          boolean use_seperate_thread, List<Address> new_mbrs) {
        sendViewOnLocalCluster(ViewData.create(remote_view, global_view), use_seperate_thread, new_mbrs);
    }


    protected void sendViewOnLocalCluster(ViewData data, boolean use_seperate_thread, final List<Address> new_mbrs) {
        try {
            final byte[] buffer=Util.streamableToByteBuffer(data);
            final List<Address> destinations=new ArrayList<>();
            destinations.add(null); // send to all
            if(new_mbrs != null)
                destinations.addAll(new_mbrs);

            if(use_seperate_thread) {
                timer.execute(() -> sendViewOnLocalCluster(destinations, buffer));
            }
            else
                sendViewOnLocalCluster(destinations, buffer);
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedSendingViewToLocalCluster"), e);
        }
    }


    protected void sendViewOnLocalCluster(final List<Address> destinations, final byte[] buffer) {
        for(Address dest: destinations) {
            Message view_msg=new Message(dest, buffer).putHeader(id, RelayHeader.create(RelayHeader.Type.VIEW));
            down_prot.down(view_msg);
        }
    }

    /** Does the payload match the 'site' ID. Checks only unicast destinations (multicast destinations return true) */
    protected boolean isLocal(Address dest) {
        if(dest instanceof ExtendedUUID) {
            byte[] tmp=((ExtendedUUID)dest).get(SITE_ID);
            String str=Util.bytesToString(tmp);
            return Objects.equals(str, site);
        }
        return true;
    }


    protected synchronized void startRemoteViewFetcher() {
        if(remote_view_fetcher_future == null || remote_view_fetcher_future.isDone()) {
            remote_view_fetcher_future=timer.scheduleWithFixedDelay(new RemoteViewFetcher(), 500, 2000, TimeUnit.MILLISECONDS);
        }
    }

    protected synchronized void stopRemoteViewFetcher() {
        if(remote_view_fetcher_future != null) {
            remote_view_fetcher_future.cancel(false);
            remote_view_fetcher_future=null;
        }
    }



    protected class Receiver extends ReceiverAdapter {

        public void receive(Message msg) {
            Address sender=msg.getSrc();
            if(bridge.getAddress().equals(sender)) // discard my own messages
                return;

            RelayHeader hdr=msg.getHeader(id);
            switch(hdr.type) {
                case DISSEMINATE: // should not occur here, but we'll ignore it anyway
                    break;
                case FORWARD:
                    sendOnLocalCluster(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    break;
                case VIEW:
                    try {
                        ViewData data=Util.streamableFromByteBuffer(ViewData::new, msg.getRawBuffer(),
                                                                    msg.getOffset(), msg.getLength());
                        // replace addrs with proxies
                        if(data.remote_view != null) {
                            List<Address> mbrs=new LinkedList<>(data.remote_view.getMembers());
                            data.remote_view=new View(data.remote_view.getViewId(), mbrs);
                        }
                        boolean merge=remote_view == null;
                        stopRemoteViewFetcher();
                        data.global_view=generateGlobalView(local_view, data.remote_view, merge);
                        sendViewOnLocalCluster(data, false, null);
                    }
                    catch(Exception e) {
                        log.error(Util.getMessage("FailedUnmarshallingViewFromRemoteCluster"), e);
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
            if(bridge_view != null && bridge_view.getViewId().equals(view.getViewId()))
                return;
            int prev_members=bridge_view != null? bridge_view.size() : 0;
            bridge_view=view;

            switch(view.size()) {
                case 1:
                    // the remote cluster disappeared, remove all of its addresses from the view
                    if(prev_members > 1 && view.getCoord().equals(bridge.getAddress())) {
                        remote_view=null;
                        View new_global_view=generateGlobalView(local_view, null);
                        sendViewOnLocalCluster(null, new_global_view, false, null);
                    }
                    break;
                case 2:
                    startRemoteViewFetcher();
                    break;
                default:
                    // System.err.println("View size of > 2 is invalid: view=" + view);
                    break;
            }
        }
    }


    protected class RemoteViewFetcher implements Runnable {

        public void run() {
            if(bridge == null || !bridge.isConnected() || remote_view != null)
                return;
            Message msg=new Message().putHeader(id, RelayHeader.create(RELAY.RelayHeader.Type.BROADCAST_VIEW));
            try {
                bridge.send(msg);
            }
            catch(Exception e) {
            }
        }
    }


    public static class RelayHeader extends Header {
        public enum Type {DISSEMINATE, FORWARD, VIEW, BROADCAST_VIEW};
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
        public short getMagicId() {return 70;}
        public Supplier<? extends Header> create() {
            return RelayHeader::new;
        }

        public int serializedSize() {
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

        public void writeTo(DataOutput out) throws Exception {
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

        public void readFrom(DataInput in) throws Exception {
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
            Map<Address,String> tmp=NameCache.getContents();
            return new ViewData(remote_view, global_view, tmp);
        }


        public void writeTo(DataOutput out) throws Exception {
            Util.writeView(remote_view, out);
            Util.writeView(global_view, out);
            out.writeInt(uuids.size());
            for(Map.Entry<Address,String> entry: uuids.entrySet()) {
                Util.writeAddress(entry.getKey(), out);
                out.writeUTF(entry.getValue());
            }
        }

        public void readFrom(DataInput in) throws Exception {
            remote_view=Util.readView(in);
            global_view=Util.readView(in);
            int size=in.readInt();
            uuids=new HashMap<>();
            for(int i=0; i < size; i++) {
                Address addr=Util.readAddress(in);
                String n=in.readUTF();
                uuids.put(addr, n);
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("global_view: " + global_view).append(", remote_view: ").append(remote_view);
            return sb.toString();
        }
    }



}
