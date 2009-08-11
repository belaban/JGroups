package org.jgroups.mux;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.TimeoutException;
import org.jgroups.annotations.Experimental;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.*;
import org.jgroups.util.ThreadFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The multiplexer allows multiple channel interfaces to be associated with one
 * underlying instance of JChannel.
 * 
 * <p>
 * The multiplexer is essentially a building block residing on top of a JChannel
 * providing multiplexing functionality to N instances of MuxChannel. Since
 * MuxChannel extends the JGroups JChannel class, user applications are
 * completely unaware of this change in the underlying plumbing.
 * 
 * <p>
 * Each JGroups application sharing a channel through a multiplexer has to
 * create a MuxChannel with a unique application id. The multiplexer keeps track
 * of all registered applications and tags messages belonging to a specific
 * application with that id for sent messages. When receiving a message from a
 * remote peer, the multiplexer will dispatch a message to the appropriate
 * MuxChannel depending on the id attached to the message.
 * 
 * 
 * @author Bela Ban, Vladimir Blagojevic
 * @see MuxChannel
 * @see Channel
 * @version $Id: Multiplexer.java,v 1.110 2009/08/11 09:44:59 belaban Exp $
 */
@Experimental(comment="because of impedance mismatches between a MuxChannel and JChannel, this might get deprecated " +
        "in the future. The replacement would be a shared transport (see the documentation for details)")
@Deprecated
public class Multiplexer implements UpHandler {

    private static final Log log=LogFactory.getLog(Multiplexer.class);
    private static final String SEPARATOR="::";
    private static final short SEPARATOR_LEN=(short)SEPARATOR.length();
    private static final String NAME="MUX";

    /**
     * Map<String,MuxChannel>. Maintains the mapping between service IDs and
     * their associated MuxChannels
     */
    private final ConcurrentMap<String,MuxChannel> services=new ConcurrentHashMap<String,MuxChannel>();
    private final JChannel channel;

    /** Thread pool to concurrently process messages sent to different services */
    private final ExecutorService thread_pool;

    /**
     * To make sure messages sent to different services are processed
     * concurrently (using the thread pool above), but messages to the same
     * service are processed FIFO
     */
    private final FIFOMessageQueue<String,Runnable> fifo_queue=new FIFOMessageQueue<String,Runnable>();

    /** To collect service acks from Multiplexers */
    private final AckCollector service_ack_collector=new AckCollector();

    protected long service_ack_timeout = 2000;

    /** Cluster view */
    private volatile View view=null;

    /**
     * Map<String,Boolean>. Map of service IDs and booleans that determine
     * whether getState() has already been called
     */
    private final Map<String,Boolean> state_transfer_listeners=new HashMap<String,Boolean>();

    /**
     * Map<String,List<Address>>. A map of services as keys and lists of hosts
     * as values
     */
    private final Map<String,List<Address>> service_state=new HashMap<String,List<Address>>();

    /**
     * Map<Address, Set<String>>. Keys are senders, values are a set of
     * services hosted by that sender. Used to collect responses to
     * LIST_SERVICES_REQ
     */
    private final Map<Address,Set<String>> service_responses=new HashMap<Address,Set<String>>();

    private final List<Address> services_merged_collector=new ArrayList<Address>();

    private AtomicBoolean services_merged=new AtomicBoolean(false);

    private long service_response_timeout=3000;

    public Multiplexer(JChannel channel) {
        if(channel == null || !channel.isOpen())
            throw new IllegalArgumentException("Channel " + channel + " cannot be used for Multiplexer");
        this.channel=channel;
        this.channel.setUpHandler(this);
        this.channel.setOpt(Channel.BLOCK, Boolean.TRUE); // we want to handle BLOCK events ourselves

        //thread pool is enabled by default
        boolean use_thread_pool=Global.getPropertyAsBoolean(Global.MUX_ENABLED, true);
        if(use_thread_pool) {
            thread_pool=createThreadPool();
        }
        else {
            thread_pool=null;
        }
    }

    JChannel getChannel() {
        return channel;
    }

    /**
     * @deprecated Use ${link #getServiceIds()} instead
     * @return The set of service IDs
     */
    public Set getApplicationIds() {
        return getServiceIds();
    }

    public Set<String> getServiceIds() {
        return Collections.unmodifiableSet(services.keySet());
    }

    public long getServicesResponseTimeout() {
        return service_response_timeout;
    }

    public void setServicesResponseTimeout(long services_rsp_timeout) {
        this.service_response_timeout=services_rsp_timeout;
    }


    public long getServiceAckTimeout() {
        return service_ack_timeout;
    }

    public void setServiceAckTimeout(long service_ack_timeout) {
        this.service_ack_timeout=service_ack_timeout;
    }

    /**
     * Returns a copy of the current view <em>minus</em> the nodes on which
     * service service_id is <em>not</em> running
     *
     * @param service_id
     * @return The service view
     */
    View getServiceView(String service_id) {
        List<Address> hosts=service_state.get(service_id);
        if(hosts == null)
            return null;
        return generateServiceView(hosts);
    }

    boolean stateTransferListenersPresent() {
        return !state_transfer_listeners.isEmpty();
    }

    public synchronized void registerForStateTransfer(String appl_id, String substate_id) {
        String key=appl_id;
        if(substate_id != null && substate_id.length() > 0)
            key+=SEPARATOR + substate_id;
        state_transfer_listeners.put(key, Boolean.FALSE);
    }

    synchronized boolean getState(Address target, String id, long timeout) throws ChannelNotConnectedException,
                                                                          ChannelClosedException {
        if(state_transfer_listeners.isEmpty())
            return false;

        for(Iterator<Map.Entry<String,Boolean>> it=state_transfer_listeners.entrySet().iterator();it.hasNext();) {
            Map.Entry<String,Boolean> entry=it.next();
            String key=entry.getKey();
            int index=key.indexOf(SEPARATOR);
            boolean match;
            if(index > -1) {
                String tmp=key.substring(0, index);
                match=id.equals(tmp);
            }
            else {
                match=id.equals(key);
            }
            if(match) {
                entry.setValue(Boolean.TRUE);
                break;
            }
        }

        Collection<Boolean> values=state_transfer_listeners.values();
        boolean all_true=Util.all(values, Boolean.TRUE);
        if(!all_true)
            return true; // pseudo

        boolean rc=false;
        Set<String> keys=new HashSet<String>(state_transfer_listeners.keySet());
        rc=fetchServiceStates(target, keys, timeout);
        state_transfer_listeners.clear();
        return rc;
    }

    protected ThreadPoolExecutor createThreadPool() {
        int min_threads=1, max_threads=4;
        long keep_alive=30000;

        Map<String,Object> m=channel.getInfo();
        min_threads=Global.getPropertyAsInteger(Global.MUX_MIN_THREADS, min_threads);
        max_threads=Global.getPropertyAsInteger(Global.MUX_MAX_THREADS, max_threads);
        keep_alive=Global.getPropertyAsLong(Global.MUX_KEEPALIVE, keep_alive);

        ThreadFactory factory=new DefaultThreadFactory(Util.getGlobalThreadGroup(), "Multiplexer", false, true);
        return new ThreadPoolExecutor(min_threads, max_threads, keep_alive, TimeUnit.MILLISECONDS,
                                      new SynchronousQueue<Runnable>(),
                                      factory,
                                      new ShutdownRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy()));
    }

    protected void shutdownThreadPool() {
        if(thread_pool != null && !thread_pool.isShutdown()) {
            thread_pool.shutdownNow();
            try {
                thread_pool.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME,
                                             TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Fetches the app states for all service IDs in keys. The keys are a
     * duplicate list, so it cannot be modified by the caller of this method
     *
     * @param keys
     */
    private boolean fetchServiceStates(Address target, Set<String> keys, long timeout) throws ChannelClosedException,
                                                                                      ChannelNotConnectedException {
        boolean rc, all_tranfers_ok=false;
        boolean flushStarted=Util.startFlush(channel);
        if(flushStarted) {
            try {
                for(String stateId:keys) {
                    rc=channel.getState(target, stateId, timeout, false);
                    if(!rc)
                        throw new Exception("Failed transfer for state id " + stateId
                                            + ", state provider was "
                                            + target);
                }
                all_tranfers_ok=true;
            }
            catch(Exception e) {
                log.warn("Failed multiple state transfer under one flush phase ", e);
            }
            finally {
                channel.stopFlush();
            }
        }
        return flushStarted && all_tranfers_ok;
    }

    void sendServiceUpMessage(String service) throws Exception {
        // we have to make this service message non OOB since we have
        // to FIFO order service messages and BLOCK/UNBLOCK messages
        sendServiceMessage(true, ServiceInfo.SERVICE_UP, service, null, false);
    }

    void sendServiceDownMessage(String service) throws Exception {
        // we have to make this service message non OOB since we have
        // to FIFO order service messages and BLOCK/UNBLOCK messages
        sendServiceMessage(true, ServiceInfo.SERVICE_DOWN, service, null, false);
    }

    /**
     * Remove mux header and dispatch to correct MuxChannel
     *
     * @param evt
     * @return
     */
    public Object up(final Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                final Message msg=(Message)evt.getArg();
                final MuxHeader hdr=(MuxHeader)msg.getHeader(NAME);
                if(hdr == null) {
                    log.error("MuxHeader not present - discarding message " + msg);
                    return null;
                }

                Address sender=msg.getSrc();
                boolean isServiceMessage=hdr.info != null;
                if(isServiceMessage) {
                    try {
                        handleServiceMessage(hdr.info, sender);
                    }
                    catch(Exception e) {
                        if(log.isErrorEnabled())
                            log.error("failure in handling service message " + hdr.info
                                      + " from sender "
                                      + sender, e);
                    }
                    return null;
                }
                else {
                    //regular message between MuxChannel(s)
                    final MuxChannel mux_ch=services.get(hdr.id);
                    return mux_ch == null? null : passToMuxChannel(mux_ch,
                                                                   evt,
                                                                   fifo_queue,
                                                                   sender,
                                                                   hdr.id,
                                                                   false,
                                                                   msg.isFlagSet(Message.OOB));
                }

            case Event.VIEW_CHANGE:
                Vector<Address> old_members=view != null? view.getMembers() : null;
                view=(View)evt.getArg();
                Vector<Address> new_members=view != null? view.getMembers() : null;
                Vector<Address> left_members=Util.determineLeftMembers(old_members, new_members);

                if(view instanceof MergeView) {
                    final MergeView temp_merge_view=(MergeView)view.clone();
                    if(log.isTraceEnabled())
                        log.trace("received a MergeView: " + temp_merge_view
                                  + ", adjusting the service view");
                    try {
                        handleMergeView(temp_merge_view);
                    }
                    catch(Exception e) {
                        if(log.isErrorEnabled())
                            log.error("failed handling merge view", e);
                    }
                    finally {
                        synchronized(service_responses) {
                            service_responses.clear();
                        }
                        synchronized(services_merged_collector) {
                            services_merged_collector.clear();
                        }
                        services_merged.set(false);
                    }
                }
                else { // regular view
                    HashMap<String,List<Address>> payload=(HashMap<String,List<Address>>)view.getPayload("service_state");
                    if(payload != null) {
                        synchronized(service_state) {
                            service_state.putAll(payload);
                        }
                    }

                }
                service_ack_collector.handleView(view);
                for(Address member:left_members) {
                    try {
                        adjustServiceView(member);
                    }
                    catch(Throwable t) {
                        if(log.isErrorEnabled())
                            log.error("failed adjusting service views", t);
                    }
                }
                break;

            case Event.PREPARE_VIEW:
                View prepare_view=(View)evt.getArg();
                old_members=view != null? view.getMembers() : new Vector<Address>();

                Vector<Address> added_members=new Vector<Address>(prepare_view.getMembers());
                added_members.removeAll(old_members);

                if(!added_members.isEmpty()) {
                    synchronized(service_state) {
                        prepare_view.addPayload("service_state", service_state);
                    }
                }
                break;

            case Event.SUSPECT:
                Address suspected_mbr=(Address)evt.getArg();
                service_ack_collector.suspect(suspected_mbr);
                /*
                 * http://jira.jboss.com/jira/browse/JGRP-665
                 * Intentionally do not update service_response since we might
                 * get false suspect while merging if FD is aggressive enough.
                 * Instead rely on ServiceInfo.SERVICES_MERGED and timeout
                 * mechanism in handleMergeView
                 */
                passToAllMuxChannels(evt);
                break;

            case Event.GET_APPLSTATE:
                return handleStateRequest(evt, true);
            case Event.STATE_TRANSFER_OUTPUTSTREAM:
                handleStateRequest(evt, true);
                break;
            case Event.GET_STATE_OK:
            case Event.STATE_TRANSFER_INPUTSTREAM:
                handleStateResponse(evt, true);
                break;

            case Event.BLOCK:
                passToAllMuxChannels(evt, true, true);
                return null;

            case Event.UNBLOCK: // process queued-up MergeViews
                passToAllMuxChannels(evt);
                break;

            default:
                passToAllMuxChannels(evt);
                break;
        }
        return null;
    }

    public Channel createMuxChannel(String id, String stack_name) throws Exception {
        if(services.containsKey(id)) {
            throw new Exception("service ID \"" + id
                                + "\" is already registered at channel"
                                + getLocalAddress()
                                + ", cannot register service with duplicate ID at the same channel");
        }
        MuxChannel ch=new MuxChannel(id, stack_name, this);
        services.put(id, ch);
        return ch;
    }

    private void passToAllMuxChannels(Event evt) {
        passToAllMuxChannels(evt, false, true);
    }

    private void passToAllMuxChannels(Event evt, boolean block, boolean bypass_thread_pool) {
        String service_name;
        MuxChannel ch;
        for(Map.Entry<String,MuxChannel> entry:services.entrySet()) {
            service_name=entry.getKey();
            ch=entry.getValue();
            // these events are directly delivered, don't get added to any queue
            passToMuxChannel(ch, evt, fifo_queue, null, service_name, block, bypass_thread_pool);
        }
    }

    void addServiceIfNotPresent(String id, MuxChannel ch) {
        services.putIfAbsent(id, ch);
    }

    protected MuxChannel removeService(String id) {
        MuxChannel ch=services.remove(id);
        //http://jira.jboss.com/jira/browse/JGRP-623
        if(ch != null)
            ch.up(new Event(Event.UNBLOCK));
        return ch;
    }

    /** Closes the underlying JChannel if all MuxChannels have been disconnected */
    void disconnect() {
        boolean all_disconnected=true;
        for(MuxChannel mux_ch:services.values()) {
            if(mux_ch.isConnected()) {
                all_disconnected=false;
                break;
            }
        }
        if(all_disconnected) {
            if(log.isTraceEnabled()) {
                log.trace("disconnecting underlying JChannel as all MuxChannels are disconnected");
            }
            channel.disconnect();
        }
    }

    public boolean close() {
        boolean all_closed=true;
        for(MuxChannel mux_ch:services.values()) {
            if(mux_ch.isOpen()) {
                all_closed=false;
                break;
            }
        }
        if(all_closed) {
            if(log.isTraceEnabled()) {
                log.trace("closing underlying JChannel as all MuxChannels are closed");
            }
            channel.close();
            services.clear();
            shutdownThreadPool();
        }
        return all_closed;
    }

    public void closeAll() {
        for(MuxChannel mux_ch:services.values()) {
            mux_ch.setConnected(false);
            mux_ch.setClosed(true);
            mux_ch.closeMessageQueue(true);
        }
    }

    boolean shutdown() {
        boolean all_closed=true;
        for(MuxChannel mux_ch:services.values()) {
            if(mux_ch.isOpen()) {
                all_closed=false;
                break;
            }
        }
        if(all_closed) {
            if(log.isTraceEnabled()) {
                log.trace("shutting down underlying JChannel as all MuxChannels are closed");
            }
            channel.shutdown();
            services.clear();
            shutdownThreadPool();
        }
        return all_closed;
    }

    Address getLocalAddress() {
        return channel.getAddress();
    }

    boolean flushSupported() {
        return channel.flushSupported();
    }

    boolean startFlush(boolean automatic_resume) {
    	boolean result = Util.startFlush(channel);
    	if(automatic_resume)
    		channel.stopFlush();
    	return result;
    }

    void stopFlush() {
        channel.stopFlush();
    }

    boolean isConnected() {
        return channel.isConnected();
    }

    void connect(String cluster_name) throws ChannelException {
        channel.connect(cluster_name);
    }

    /**
     * Determines whether the channel is open; i.e., the protocol stack has been
     * created (may not be connected though).
     */
    boolean isOpen() {
        return channel.isOpen();
    }

    void open() throws ChannelException {
        channel.open();
    }

    /**
     * Returns an Address of a state provider for a given service_id. If
     * preferredTarget is a member of a service view for a given service_id then
     * preferredTarget is returned. Otherwise, service view coordinator is
     * returned if such node exists. If service view is empty for a given
     * service_id null is returned.
     *
     * @param preferredTarget
     * @param service_id
     * @return
     */
    Address getStateProvider(Address preferredTarget, String service_id) {
        Address result=null;
        List<Address> hosts=service_state.get(service_id);
        if(hosts != null && !hosts.isEmpty()) {
            if(hosts.contains(preferredTarget)) {
                result=preferredTarget;
            }
            else {
                result=hosts.get(0);
            }
        }
        return result;
    }

    private void sendServiceMessage(boolean synchronous,
                                    byte type,
                                    String service,
                                    byte[] payload,
                                    boolean oob) throws Exception {

        Address host=getLocalAddress();
        if(host == null) {
            if(log.isWarnEnabled()) {
                log.warn("local_addr is null, cannot send ServiceInfo." + ServiceInfo.typeToString(type)
                         + " message");
            }
            return;
        }

        if(!channel.isOpen() || !channel.isConnected()) {
            if(log.isWarnEnabled()) {
                log.warn("Underlying multiplexer channel " + channel.getAddress()
                         + " is not connected, cannot send ServiceInfo."
                         + ServiceInfo.typeToString(type)
                         + " message");
            }
            return;
        }

        Message service_msg=new Message();
        service_msg.putHeader(NAME, new MuxHeader(new ServiceInfo(type, service, host, payload)));

        if(oob)
            service_msg.setFlag(Message.OOB);

        if(channel.flushSupported())
            service_msg.putHeader(FLUSH.NAME, new FLUSH.FlushHeader(FLUSH.FlushHeader.FLUSH_BYPASS));

        if(synchronous) {
            //for synchronous invocation we need to collect acks
            //the host that is sending this message should also ack
            CopyOnWriteArrayList<Address> muxChannels=new CopyOnWriteArrayList<Address>();
            muxChannels.add(host);
            List<Address> list=service_state.get(service);
            if(list != null && !list.isEmpty()) {
                muxChannels.addAllAbsent(list);
            }

            //initialize collector and ...
            service_ack_collector.reset(muxChannels);
            int size=service_ack_collector.size();

            //then send a message
            channel.send(service_msg);
            long start=System.currentTimeMillis();
            try {
                service_ack_collector.waitForAllAcks(service_ack_timeout);
                if(log.isTraceEnabled())
                    log.trace("received all service ACKs (" + size
                              + ")  in "
                              + (System.currentTimeMillis() - start)
                              + "ms");
            }
            catch(TimeoutException e) {
                log.warn("failed to collect all service ACKs (" + size
                         + ") for "
                         + service_msg
                         + " after "
                         + service_ack_timeout
                         + "ms, missing ACKs from "
                         + service_ack_collector.printMissing()
                         + ", local_addr="
                         + getLocalAddress());
            }
        }
        else {
            //if asynchronous then fire and forget
            channel.send(service_msg);
        }
    }

    private Object handleStateRequest(Event evt, boolean hasReturnValue) {
        StateTransferInfo info=(StateTransferInfo)evt.getArg();
        String id=info.state_id;
        String original_id=id;
        Address requester=info.target; // the sender of the state request

        if(id == null) {
            if(log.isWarnEnabled()) {
                log.warn("Invalid state request " + info + " arrived at Multiplexer, dropping it");
            }
            return new StateTransferInfo(null, original_id, 0L, null);
        }

        try {
            int index=id.indexOf(SEPARATOR);
            if(index > -1) {
                info.state_id=id.substring(index + SEPARATOR_LEN);
                id=id.substring(0, index); // similar reuse as above...
            }
            else {
                info.state_id=null;
            }

            MuxChannel mux_ch=services.get(id);
            //JGRP-616
            if(mux_ch == null) {
                if(log.isWarnEnabled())
                    log.warn("State provider " + channel.getAddress()
                             + " does not have service with id "
                             + id
                             + ", returning null state");

                return new StateTransferInfo(null, original_id, 0L, null);
            }

            // state_id will be null, get regular state from the service named state_id
            StateTransferInfo ret=(StateTransferInfo)passToMuxChannel(mux_ch,
                                                                      evt,
                                                                      fifo_queue,
                                                                      requester,
                                                                      id,
                                                                      hasReturnValue);
            if(ret != null) {
                ret.state_id=original_id;
            }
            else {
                return new StateTransferInfo(null, original_id, 0L, null);
            }
            return ret;
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled())
                log.error("failed returning the application state, will return null", ex);
            return new StateTransferInfo(null, original_id, 0L, null);
        }
    }

    private void handleStateResponse(Event evt, boolean block) {
        StateTransferInfo info=(StateTransferInfo)evt.getArg();
        MuxChannel mux_ch;
        Address state_sender=info.target;

        String appl_id, substate_id, tmp;
        tmp=info.state_id;

        if(tmp == null) {
            if(log.isTraceEnabled())
                log.trace("state is null, not passing up: " + info);
            return;
        }

        int index=tmp.indexOf(SEPARATOR);
        if(index > -1) {
            appl_id=tmp.substring(0, index);
            substate_id=tmp.substring(index + SEPARATOR_LEN);
        }
        else {
            appl_id=tmp;
            substate_id=null;
        }

        mux_ch=services.get(appl_id);
        if(mux_ch == null) {
            log.error("State receiver " + channel.getAddress()
                      + " does not have service with id "
                      + appl_id);
        }
        else {
            StateTransferInfo tmp_info=info.copy();
            tmp_info.state_id=substate_id;
            Event tmpEvt=new Event(evt.getType(), tmp_info);
            passToMuxChannel(mux_ch, tmpEvt, fifo_queue, state_sender, appl_id, block);
        }
    }

    private void handleServiceMessage(ServiceInfo info, Address sender) throws Exception {
        switch(info.type) {
            case ServiceInfo.SERVICE_UP:
                handleServiceUp(info.service, info.host);
                ackServiceMessage(info, sender);
                break;
            case ServiceInfo.SERVICE_DOWN:
                handleServiceDown(info.service, info.host);
                ackServiceMessage(info, sender);
                break;
            case ServiceInfo.LIST_SERVICES_RSP:
                handleServicesRsp(sender, info.state);
                break;
            case ServiceInfo.ACK:
                service_ack_collector.ack(sender);
                break;
            case ServiceInfo.SERVICES_MERGED:
                synchronized(services_merged_collector) {
                    if(!services_merged_collector.contains(sender)) {
                        services_merged_collector.add(sender);
                    }
                    boolean mergeOk=view != null && services_merged_collector.containsAll(view.getMembers());
                    services_merged.set(mergeOk);
                    if(log.isDebugEnabled())
                        log.debug(getLocalAddress() + " got service merged from "
                                  + sender
                                  + " merged so far "
                                  + services_merged_collector
                                  + " view is "
                                  + view.size());

                }
                break;
            default:
                if(log.isErrorEnabled())
                    log.error("service request type " + info.type + " not known");
                break;
        }
    }

    private void ackServiceMessage(ServiceInfo info, Address ackTarget) throws ChannelNotConnectedException,
                                                                       ChannelClosedException {

        Message ack=new Message(ackTarget, null, null);
        ack.setFlag(Message.OOB);

        ServiceInfo si=new ServiceInfo(ServiceInfo.ACK, info.service, info.host, null);
        MuxHeader hdr=new MuxHeader(si);
        ack.putHeader(NAME, hdr);

        if(channel.isConnected())
            channel.send(ack);
    }

    private void handleServicesRsp(Address sender, byte[] state) throws Exception {
        Set<String> s=(Set<String>)Util.objectFromByteBuffer(state);
        boolean all_merged=false;

        synchronized(service_responses) {
            Set<String> tmp=service_responses.get(sender);
            if(tmp == null)
                tmp=new HashSet<String>();
            tmp.addAll(s);

            service_responses.put(sender, tmp);
            if(log.isDebugEnabled())
                log.debug(getLocalAddress() + " received service response: "
                          + sender
                          + "("
                          + s.toString()
                          + ")");

            all_merged=(view != null && service_responses.keySet().containsAll(view.getMembers()));
        }
        if(all_merged) {
            if(log.isDebugEnabled())
                log.debug(getLocalAddress() + " sent service merged "
                          + service_responses.keySet()
                          + " view is "
                          + view.getMembers());

            sendServiceMessage(false, ServiceInfo.SERVICES_MERGED, null, null, true);
        }
    }

    private void handleServiceDown(String service, Address host) {
        List<Address> hosts, hosts_copy;
        boolean removed=false;

        synchronized(service_state) {
            hosts=service_state.get(service);
            if(hosts == null)
                return;
            removed=hosts.remove(host);
            hosts_copy=new ArrayList<Address>(hosts); // make a copy so we don't modify hosts in generateServiceView()
        }

        if(removed) {
            View service_view=generateServiceView(hosts_copy);
            MuxChannel ch=services.get(service);
            if(ch != null && ch.isConnected()) {
                Event view_evt=new Event(Event.VIEW_CHANGE, service_view);
                //we cannot pass service message to thread pool since we have
                //to FIFO order service messages with BLOCK/UNBLOCK messages
                //therefore all of them have to bypass thread pool

                passToMuxChannel(ch, view_evt, fifo_queue, null, service, false, true);
            }
            else {
                if(log.isTraceEnabled())
                    log.trace("service " + service
                              + " not found, cannot dispatch service view "
                              + service_view);
            }
        }

        Address local_address=getLocalAddress();
        boolean isMyService=local_address != null && local_address.equals(host);
        if(isMyService)
            removeService(service);
    }

    private void handleServiceUp(String service, Address host) {
        List<Address> hosts, hosts_copy;
        boolean added=false;

        synchronized(service_state) {
            hosts=service_state.get(service);
            if(hosts == null) {
                hosts=new CopyOnWriteArrayList<Address>();
                service_state.put(service, hosts);
            }
            if(!hosts.contains(host)) {
                hosts.add(host);
                added=true;
            }
            hosts_copy=new ArrayList<Address>(hosts); // make a copy so we don't modify hosts in generateServiceView()
        }

        if(added) {
            View service_view=generateServiceView(hosts_copy);
            MuxChannel ch=services.get(service);
            if(ch != null) {
                Event view_evt=new Event(Event.VIEW_CHANGE, service_view);
                //we cannot pass service message to thread pool since we have
                //to FIFO order service messages with BLOCK/UNBLOCK messages
                //therefore all of them have to bypass thread pool
                passToMuxChannel(ch, view_evt, fifo_queue, null, service, false, true);
            }
            else {
                if(log.isTraceEnabled())
                    log.trace("service " + service
                              + " not found, cannot dispatch service view "
                              + service_view);
            }
        }
    }

    /**
     * Fetches the service states from everyone else in the cluster. Once all
     * states have been received and inserted into service_state, compute a
     * service view (a copy of MergeView) for each service and pass it up
     *
     * @param view
     */
    private void handleMergeView(MergeView view) throws Exception {
        long time_to_wait=service_response_timeout;
        long start_time=System.currentTimeMillis();
        Map<Address,Set<String>> copy=null;

        byte[] data=Util.objectToByteBuffer(new HashSet<String>(services.keySet()));

        //loop and keep sending our service list until either
        //we hit timeout or we get notification of merge completed
        //http://jira.jboss.com/jira/browse/JGRP-665
        while(time_to_wait > 0 && !services_merged.get()) {
            // we have to make this message OOB since we are running on a thread
            // propelling a regular synchronous message call to install a new view
            sendServiceMessage(false, ServiceInfo.LIST_SERVICES_RSP, null, data, true);
            Util.sleep(500);
            time_to_wait=service_response_timeout - (System.currentTimeMillis() - start_time);
        }

        if(time_to_wait <= 0 && !services_merged.get()) {
            log.warn("Services not merged at " + getLocalAddress()
                     + " received merge from "
                     + services_merged_collector);
        }

        synchronized(service_responses) {
            copy=new HashMap<Address,Set<String>>(service_responses);
        }

        if(log.isDebugEnabled())
            log.debug("At " + getLocalAddress() + " emitting views to MuxChannels " + copy);

        // merges service_responses with service_state and emits MergeViews for
        // the services affected (MuxChannel)
        mergeServiceState(view, copy);
    }

    private void mergeServiceState(MergeView view, Map<Address,Set<String>> copy) {
        Set<String> modified_services=new HashSet<String>();
        synchronized(service_state) {
            for(Iterator<Map.Entry<Address,Set<String>>> it=copy.entrySet().iterator();it.hasNext();) {
                Map.Entry<Address,Set<String>> entry=it.next();
                Address host=entry.getKey();
                Set<String> service_list=entry.getValue();
                if(service_list == null)
                    continue;

                for(String service:service_list) {
                    List<Address> my_services=service_state.get(service);
                    if(my_services == null) {
                        my_services=new CopyOnWriteArrayList<Address>();
                        service_state.put(service, my_services);
                    }

                    boolean was_modified=my_services.add(host);
                    if(was_modified) {
                        modified_services.add(service);
                    }
                }
            }
        }

        // now emit MergeViews for all services which were modified
        for(String service:modified_services) {
            MuxChannel ch=services.get(service);
            if(ch != null) {
                List<Address> hosts=service_state.get(service);
                Vector<Address> membersCopy=new Vector<Address>(view.getMembers());
                membersCopy.retainAll(hosts);
                MergeView v=new MergeView(view.getVid(), membersCopy, view.getSubgroups());
                passToMuxChannel(ch,
                                 new Event(Event.VIEW_CHANGE, v),
                                 fifo_queue,
                                 null,
                                 service,
                                 false);
            }
        }
    }

    private void adjustServiceView(Address host) {

        Address local_address=getLocalAddress();
        synchronized(service_state) {
            for(Iterator<Map.Entry<String,List<Address>>> it=service_state.entrySet().iterator();it.hasNext();) {
                Map.Entry<String,List<Address>> entry=it.next();
                String service=entry.getKey();
                List<Address> hosts=entry.getValue();
                if(hosts == null)
                    continue;

                if(hosts.remove(host)) {
                    // make a copy so we don't modify hosts in
                    // generateServiceView()
                    View service_view=generateServiceView(new ArrayList<Address>(hosts));
                    MuxChannel ch=services.get(service);
                    if(ch != null && ch.isConnected()) {
                        Event view_evt=new Event(Event.VIEW_CHANGE, service_view);
                        passToMuxChannel(ch, view_evt, fifo_queue, null, service, false, true);
                    }
                    else {
                        if(log.isTraceEnabled())
                            log.trace("service " + service
                                      + " not found, cannot dispatch service view "
                                      + service_view);
                    }
                }
                boolean isMyService=local_address != null && local_address.equals(host);
                if(isMyService)
                    removeService(service);
            }
        }
    }

    /**
     * Create a copy of view which contains only members which are present in
     * hosts. Call viewAccepted() on the MuxChannel which corresponds with
     * service. If no members are removed or added from/to view, this is a
     * no-op.
     *
     * @param hosts
     *                List<Address>
     * @return the servicd view (a modified copy of the real view), or null if
     *         the view was not modified
     */
    private View generateServiceView(List<Address> hosts) {
        if(view == null) {
            Vector<Address> tmp=new Vector<Address>();
            tmp.add(getLocalAddress());
            view=new View(new ViewId(getLocalAddress()), tmp);
        }
        Vector<Address> members=new Vector<Address>(view.getMembers());
        members.retainAll(hosts);
        return new View(view.getVid(), members);
    }

    private Object passToMuxChannel(MuxChannel ch,
                                    Event evt,
                                    final FIFOMessageQueue<String,Runnable> queue,
                                    final Address sender,
                                    final String dest,
                                    boolean block) {
        return passToMuxChannel(ch, evt, queue, sender, dest, block, false);
    }

    private Object passToMuxChannel(MuxChannel ch,
                                    Event evt,
                                    final FIFOMessageQueue<String,Runnable> queue,
                                    final Address sender,
                                    final String dest,
                                    boolean block,
                                    boolean bypass_thread_pool) {
        if(thread_pool == null || bypass_thread_pool) {
            return ch.up(evt);
        }

        Task task=new Task(ch, evt, queue, sender, dest, block);
        ExecuteTask execute_task=new ExecuteTask(fifo_queue); // takes Task from queue and executes it

        try {
            fifo_queue.put(sender, dest, task);
            thread_pool.execute(execute_task);
            if(block) {
                try {
                    return task.exchanger.exchange(null);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }



    private static class Task implements Runnable {
        Exchanger<Object> exchanger;
        MuxChannel channel;
        Event evt;
        FIFOMessageQueue<String,Runnable> queue;
        Address sender;
        String dest;

        Task(MuxChannel channel,
             Event evt,
             FIFOMessageQueue<String,Runnable> queue,
             Address sender,
             String dest,
             boolean result_expected) {
            this.channel=channel;
            this.evt=evt;
            this.queue=queue;
            this.sender=sender;
            this.dest=dest;
            if(result_expected)
                exchanger=new Exchanger<Object>();
        }

        public void run() {
            Object retval;
            try {
                retval=channel.up(evt);
                if(exchanger != null)
                    exchanger.exchange(retval);
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt(); // let the thread pool handle the interrupt - we're done anyway
            }
            finally {
                queue.done(sender, dest);
            }
        }
    }

    private static class ExecuteTask implements Runnable {
        FIFOMessageQueue<String,Runnable> queue;

        public ExecuteTask(FIFOMessageQueue<String,Runnable> queue) {
            this.queue=queue;
        }

        public void run() {
            try {
                Runnable task=queue.take();
                task.run();
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}