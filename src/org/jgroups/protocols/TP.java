package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Generic transport - specific implementations should extend this class.
 * Features which are provided to the subclasses include
 * <ul>
 * <li>version checking
 * <li>marshalling and unmarshalling
 * <li>message bundling (handling single messages, and message lists)
 * <li>incoming packet handler
 * </ul>
 * A subclass has to override
 * <ul>
 * <li>{@link #sendUnicast(org.jgroups.PhysicalAddress, byte[], int, int)}
 * <li>{@link #init()}
 * <li>{@link #start()}: subclasses <em>must</em> call super.start() <em>after</em> they initialize themselves
 * (e.g., created their sockets).
 * <li>{@link #stop()}: subclasses <em>must</em> call super.stop() after they deinitialized themselves
 * <li>{@link #destroy()}
 * </ul>
 * The create() or start() method has to create a local address.<br>
 * The {@link #receive(Address, byte[], int, int)} method must
 * be called by subclasses when a unicast or multicast message has been received.
 * @author Bela Ban
 */
@MBean(description="Transport protocol")
public abstract class TP extends TPConfig implements DiagnosticsHandler.ProbeHandler {
    public static final    byte    LIST=1; // we have a list of messages rather than a single message when set
    public static final    byte    MULTICAST=2; // message is a multicast (versus a unicast) message when set
    public static final    int     MSG_OVERHEAD=Global.SHORT_SIZE*2 + Global.BYTE_SIZE; // version + flags
    protected static final long    MIN_WAIT_BETWEEN_DISCOVERIES=TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);  // ns


    protected TP() {
    }

    /** Whether hardware multicasting is supported */
    public abstract boolean            supportsMulticasting();

    @Deprecated(since="5.5.4",forRemoval=true)
    public boolean                     isMulticastCapable() {return supportsMulticasting();}

    /** Returns the physical address of this transport */
    protected abstract PhysicalAddress getPhysicalAddress();

    /**
     * Send a unicast message to a member. Note that the destination address is a *physical*, not a logical address
     * @param dest Must be a non-null physical unicast address (e.g. {@link org.jgroups.stack.IpAddress})
     * @param data The data to be sent. This is not a copy, so don't modify it
     */
    public abstract void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception;

    public String toString() {
        return local_addr != null? getName() + "(local address: " + local_addr + ')' : getName();
    }

    public <T extends Protocol> T setAddress(Address addr) {
        super.setAddress(addr);
        registerLocalAddress(addr);
        return (T)this;
    }

    public void init() throws Exception {
        this.id=ClassConfigurator.getProtocolId(TP.class);

        if(use_vthreads && !Util.virtualThreadsAvailable()) {
            log.debug("use_virtual_threads was set to false, as virtual threads are not available in this Java version");
            use_vthreads=false;
        }
        if(local_transport_class != null) {
            Class<?> cl=Util.loadClass(local_transport_class, getClass());
            local_transport=(LocalTransport)cl.getDeclaredConstructor().newInstance();
            local_transport.init(this);
        }

        if(thread_factory == null)
            setThreadFactory(new LazyThreadFactory("jgroups", false, true).useVThreads(use_vthreads).log(this.log));

        // local_addr is null when shared transport, channel_name is not used
        setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);

        diag_handler=createDiagnosticsHandler();

        who_has_cache=new ExpiryCache<>(who_has_cache_timeout);

        if(suppress_time_different_version_warnings > 0)
            suppress_log_different_version=new SuppressLog<>(log, "VersionMismatch");
        if(suppress_time_different_cluster_warnings > 0)
            suppress_log_different_cluster=new SuppressLog<>(log, "MsgDroppedDiffCluster");

        if(timer == null) {
            timer=new TimeScheduler3(thread_pool, thread_factory, false); // don't start the timer thread yet (JGRP-2332)
            timer.setNonBlockingTaskHandling(timer_handle_non_blocking_tasks);
        }

        if(time_service_interval > 0)
            time_service=new TimeService(timer, time_service_interval);

        Map<String, Object> m=new HashMap<>(2);
        if(bind_addr != null)
            m.put("bind_addr", bind_addr);
        if(external_addr != null)
            m.put("external_addr", external_addr);
        if(external_port > 0)
            m.put("external_port", external_port);
        if(!m.isEmpty())
            up(new Event(Event.CONFIG, m));

        logical_addr_cache=new LazyRemovalCache<>(logical_addr_cache_max_size, logical_addr_cache_expiration);
        if(logical_addr_cache_reaper_interval > 0 && (logical_addr_cache_reaper == null || logical_addr_cache_reaper.isDone())) {
            logical_addr_cache_reaper=timer.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    evictLogicalAddressCache();
                }

                public String toString() {
                    return TP.this.getClass().getSimpleName() + ": LogicalAddressCacheReaper (interval=" + logical_addr_cache_reaper_interval + " ms)";
                }
            }, logical_addr_cache_reaper_interval, logical_addr_cache_reaper_interval, TimeUnit.MILLISECONDS, false);
        }

        if(message_processing_policy != null)
            setMessageProcessingPolicy(message_processing_policy);
        else
            msg_processing_policy.init(this);

        if(bundler == null)
            bundler=createBundler(bundler_type, getClass());
        bundler.init(this);
        rtt.init(this);
        // When stats is false, we'll set msg_stats.enabled to false, too. However, msg_stats.enabled=false can be
        // set to false even if stats is true
        if(!stats)
            msg_stats.enable(false);
    }

    /** Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads */
    public void start() throws Exception {
        timer.start();
        thread_pool.setAddress(local_addr);
        async_executor.start();
        if(time_service != null)
            time_service.start();
        fetchLocalAddresses();
        startDiagnostics();
        bundler.start();
        // local_addr is null when shared transport
        setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);
    }

    public void stop() {
        stopDiagnostics();
        bundler.stop();
        if(msg_processing_policy != null)
            msg_processing_policy.destroy();

        if(time_service != null)
            time_service.stop();
        timer.stop();
        async_executor.stop();
    }

    public void destroy() {
        super.destroy();
        if(local_transport != null)
            local_transport.destroy();
        if(logical_addr_cache_reaper != null) {
            logical_addr_cache_reaper.cancel(false);
            logical_addr_cache_reaper=null;
        }
        thread_pool.destroy();
        if(bundler != null)
            bundler.destroy();
    }


    @ManagedOperation(description="Enables diagnostics and starts DiagnosticsHandler (if not running)")
    public <T extends TP> T enableDiagnostics() {
        diag_handler.setEnabled(true);
        try {
            startDiagnostics();
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedStartingDiagnostics"), e);
        }
        return (T)this;
    }

    @ManagedOperation(description="Disables diagnostics and stops DiagnosticsHandler (if running)")
    public void disableDiagnostics() {
        stopDiagnostics();
    }

    public Map<String, String> handleProbe(String... keys) {
        Map<String,String> retval=new HashMap<>(keys != null? keys.length : 2);
        if(keys == null)
            return retval;

        for(String key: keys) {
            switch(key) {
                case "dump":
                    retval.put(key, Util.dumpThreads());
                    break;
                case "uuids":
                    retval.put(key, printLogicalAddressCache());
                    if(!retval.containsKey("local_addr"))
                        retval.put("local_addr", local_addr != null? local_addr.toString() : null);
                    break;
                case "keys":
                    StringBuilder sb=new StringBuilder();
                    for(DiagnosticsHandler.ProbeHandler handler : diag_handler.getProbeHandlers()) {
                        String[] tmp=handler.supportedKeys();
                        if(tmp != null) {
                            for(String s : tmp)
                                sb.append(s).append(" ");
                        }
                    }
                    retval.put(key, sb.toString());
                    break;
                case "member-addrs":
                    Set<PhysicalAddress> physical_addrs=logical_addr_cache.nonRemovedValues();
                    String list=Util.print(physical_addrs);
                    retval.put(key, list);
                break;
            }
        }
        return retval;
    }

    public String[] supportedKeys() {
        return new String[]{"dump", "keys", "uuids", "member-addrs"};
    }

    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                Collection<Address> old_members;
                synchronized(members) {
                    View v=evt.getArg();
                    this.view=v;
                    old_members=new ArrayList<>(members);
                    members.clear();
                    members.addAll(v.getMembers());

                    // fix for https://issues.redhat.com/browse/JGRP-918
                    logical_addr_cache.retainAll(members);
                    fetchLocalAddresses();

                    List<Address> left_mbrs=Util.leftMembers(old_members,members);
                    if(left_mbrs != null && !left_mbrs.isEmpty())
                        NameCache.removeAll(left_mbrs);

                    if(suppress_log_different_version != null)
                        suppress_log_different_version.removeExpired(suppress_time_different_version_warnings);
                    if(suppress_log_different_cluster != null)
                        suppress_log_different_cluster.removeExpired(suppress_time_different_cluster_warnings);
                }
                who_has_cache.removeExpiredElements();
                if(bundler != null)
                    bundler.viewChange(evt.getArg());
                if(msg_processing_policy instanceof MaxOneThreadPerSender)
                    ((MaxOneThreadPerSender)msg_processing_policy).viewChange(view.getMembers());

                if(local_transport != null)
                    local_transport.viewChange(this.view);

                // Increase thread pool size when view.size() > max_threads (https://issues.redhat.com/browse/JGRP-2655)
                if(ergonomics && thread_pool.getIncreaseMaxSizeDynamically()) {
                    int size=view.size();
                    if(size >= thread_pool.getMaxThreads()) {
                        int new_size=size+thread_pool.getDelta();
                        log.warn("%s: view size=%d, thread_pool.max-threads=%d: increasing max-threads to %d",
                                 local_addr, size, thread_pool.getMaxThreads(), new_size);
                        thread_pool.setMaxThreads(new_size);
                    }
                }
                thread_pool.removeExpired();
                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
                cluster_name=new AsciiString((String)evt.getArg());
                header=new TpHeader(cluster_name);
                setInAllThreadFactories(cluster_name != null? cluster_name.toString() : null, local_addr, thread_naming_pattern);
                setThreadNames();
                connectLock.lock();
                try {
                    if(local_transport != null)
                        local_transport.start();
                    handleConnect();
                }
                catch(Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    connectLock.unlock();
                }
                return null;

            case Event.DISCONNECT:
                unsetThreadNames();
                connectLock.lock();
                try {
                    if(local_transport != null)
                        local_transport.stop();
                    handleDisconnect();
                }
                finally {
                    connectLock.unlock();
                }
                break;

            case Event.GET_PHYSICAL_ADDRESS:
                Address addr=evt.getArg();
                PhysicalAddress physical_addr=getPhysicalAddressFromCache(addr);
                if(physical_addr != null)
                    return physical_addr;
                if(Objects.equals(addr, local_addr)) {
                    physical_addr=getPhysicalAddress();
                    if(physical_addr != null)
                        addPhysicalAddressToCache(addr, physical_addr);
                }
                return physical_addr;

            case Event.GET_PHYSICAL_ADDRESSES:
                return getAllPhysicalAddressesFromCache();

            case Event.GET_LOGICAL_PHYSICAL_MAPPINGS:
                Object arg=evt.getArg();
                boolean skip_removed_values=arg instanceof Boolean && (Boolean)arg;
                return logical_addr_cache.contents(skip_removed_values);

            case Event.ADD_PHYSICAL_ADDRESS:
                Tuple<Address,PhysicalAddress> tuple=evt.getArg();
                return addPhysicalAddressToCache(tuple.val1(), tuple.val2());

            case Event.REMOVE_ADDRESS:
                removeLogicalAddressFromCache(evt.getArg());
                local_addr=null;
                break;
        }
        return null;
    }

    /** A message needs to be sent to a single member or all members */
    public Object down(Message msg) {
        if(header != null)
            msg.putHeaderIfAbsent(this.id, header); // added patch by Roland Kurmann (March 20, 2003)
        setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
        Address dest=msg.getDest();
        if(is_trace)
            log.trace("%s: sending msg to %s, src=%s, size=%d, hdrs: %s", local_addr, dest, msg.src(), msg.size(), msg.printHeaders());
        try {
            Bundler tmp_bundler=bundler;
            if(tmp_bundler != null) {
                tmp_bundler.send(msg);
                msg_stats.sent(msg);
            }
        }
        catch(InterruptedIOException ignored) {
        }
        catch(InterruptedException interruptedEx) {
            Thread.currentThread().interrupt(); // let someone else handle the interrupt
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("SendFailure"),
                      local_addr, (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
        }
        return null;
    }

    public void passMessageUp(Message msg, boolean perform_cluster_name_matching,
                              boolean multicast, boolean discard_own_mcast) {
        if(is_trace)
            log.trace("%s: received %s, headers are %s", local_addr, msg, msg.printHeaders());

        if(up_prot == null)
            return;

        if(multicast && discard_own_mcast && local_addr != null && local_addr.equals(msg.getSrc()))
            return;

        // Discard if message's cluster name is not the same as our cluster name
        if(perform_cluster_name_matching && this.cluster_name != null) {
            TpHeader hdr=msg.getHeader(id);
            byte[] cname=hdr != null? hdr.clusterName() : null;
            if(cname != null && !this.cluster_name.equals(cname)) {
                if(log_discard_msgs && log.isWarnEnabled()) {
                    Address sender=msg.getSrc();
                    if(suppress_log_different_cluster != null)
                        suppress_log_different_cluster.log(SuppressLog.Level.warn, sender,
                                                           suppress_time_different_cluster_warnings,
                                                           new AsciiString(cname), this.cluster_name, sender);
                    else
                        log.warn(Util.getMessage("MsgDroppedDiffCluster"), new AsciiString(cname), this.cluster_name, sender);
                }
                return;
            }
        }
        if(rtt.enabled()) {
            TpHeader hdr=msg.getHeader(id);
            if(hdr != null && hdr.flag() > 0) {
                rtt.handleMessage(msg, hdr);
                return;
            }
        }
        up_prot.up(msg);
    }

    public void passBatchUp(MessageBatch batch, boolean perform_cluster_name_matching, boolean discard_own_mcast) {
        if(is_trace)
            log.trace("%s: received message batch of %d messages from %s", local_addr, batch.size(), batch.sender());
        if(up_prot == null)
            return;

        // Discard if message's cluster name is not the same as our cluster name
        if(perform_cluster_name_matching && cluster_name != null && !cluster_name.equals(batch.clusterName())) {
            if(log_discard_msgs && log.isWarnEnabled()) {
                Address sender=batch.sender();
                if(suppress_log_different_cluster != null)
                    suppress_log_different_cluster.log(SuppressLog.Level.warn, sender,
                                                       suppress_time_different_cluster_warnings,
                                                       batch.clusterName(),cluster_name, sender);
                else
                    log.warn(Util.getMessage("BatchDroppedDiffCluster"), batch.clusterName(),cluster_name, sender);
            }
            return;
        }

        if(batch.multicast() && discard_own_mcast && local_addr != null && local_addr.equals(batch.sender()))
            return;
        if(rtt.enabled()) {
            for(Iterator<Message> it=batch.iterator(); it.hasNext(); ) {
                Message msg=it.next();
                TpHeader hdr=msg.getHeader(id);
                if(hdr != null && hdr.flag() > 0) {
                    it.remove();
                    rtt.handleMessage(msg, hdr);
                }
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /** Subclasses must call this method when a unicast or multicast message has been received */
    public void receive(Address sender, byte[] data, int offset, int length) {
        if(data == null) return;

        // drop message from self; it has already been looped back up (https://issues.redhat.com/browse/JGRP-1765)
        if(Objects.equals(local_physical_addr, sender))
            return;

        // the length of a message needs to be at least 3 bytes: version (2) and flags (1) // JGRP-2210
        if(length < Global.SHORT_SIZE + Global.BYTE_SIZE)
            return;

        short version=Bits.readShort(data, offset);
        if(!versionMatch(version, sender))
            return;
        offset+=Global.SHORT_SIZE;
        byte flags=data[offset];
        offset+=Global.BYTE_SIZE;

        boolean is_message_list=(flags & LIST) == LIST, multicast=(flags & MULTICAST) == MULTICAST;
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        if(is_message_list) // used if message bundling is enabled
            handleMessageBatch(in, multicast);
        else
            handleSingleMessage(in, multicast);
    }

    public void receive(Address sender, DataInput in, int ignoredLength) throws Exception {
        if(in == null) return;

        // drop message from self; it has already been looped back up (https://issues.redhat.com/browse/JGRP-1765)
        if(Objects.equals(local_physical_addr, sender))
            return;

        short version=in.readShort();
        if(!versionMatch(version, sender))
            return;
        byte flags=in.readByte();

        boolean is_message_list=(flags & LIST) == LIST, multicast=(flags & MULTICAST) == MULTICAST;
        if(is_message_list) // used if message bundling is enabled
            handleMessageBatch(in, multicast);
        else
            handleSingleMessage(in, multicast);
    }

    public void doSend(byte[] buf, int offset, int length, Address dest) throws Exception {
        if(dest != null)
            sendTo(dest, buf, offset, length);
        else
            sendToAll(buf, offset, length);
    }

    public boolean unicastDestMismatch(Address dest) {
        return dest != null && !(Objects.equals(dest, local_addr) || Objects.equals(dest, local_physical_addr));
    }

    public boolean addPhysicalAddressToCache(Address logical_addr, PhysicalAddress physical_addr) {
        return addPhysicalAddressToCache(logical_addr, physical_addr, true);
    }

    public PhysicalAddress getPhysicalAddressFromCache(Address logical_addr) {
        return logical_addr != null? logical_addr_cache.get(logical_addr) : null;
    }

    protected void handleMessageBatch(DataInput in, boolean multicast) {
        try {
            final MessageBatch[] batches=Util.readMessageBatch(in, multicast);
            final MessageBatch regular=batches[0], oob=batches[1];

            // we need to update the stats *before* processing the batches: protocols can remove msgs from the batch
            if(oob != null) msg_stats.received(oob);
            if(regular != null) msg_stats.received(regular);
            processBatch(oob,    true);
            processBatch(regular,false);
        }
        catch(Throwable t) {
            log.error(String.format(Util.getMessage("IncomingMsgFailure"), local_addr), t);
        }
    }

    protected void handleSingleMessage(DataInput in, boolean multicast) {
        try {
            short type=in.readShort();
            Message msg=MessageFactory.create(type); // don't create headers, readFrom() will do this
            msg.readFrom(in);

            if(!multicast && unicastDestMismatch(msg.getDest()))
                return;

            boolean oob=msg.isFlagSet(Message.Flag.OOB);
            msg_processing_policy.process(msg, oob);
            msg_stats.received(msg);
        }
        catch(Throwable t) {
            log.error(String.format(Util.getMessage("IncomingMsgFailure"), local_addr), t);
        }
    }

    protected void processBatch(MessageBatch batch, boolean oob) {
        try {
            if(batch != null && !batch.isEmpty() && !unicastDestMismatch(batch.getDest()))
                msg_processing_policy.process(batch, oob);
        }
        catch(Throwable t) {
            log.error("processing batch failed", t);
        }
    }


    protected boolean versionMatch(short version, Address sender) {
        boolean match=Version.isBinaryCompatible(version);
        if(!match && log_discard_msgs_version && log.isWarnEnabled()) {
            if(suppress_log_different_version != null)
                suppress_log_different_version.log(SuppressLog.Level.warn, sender,
                        suppress_time_different_version_warnings,
                        sender, Version.print(version), Version.printVersion());
            else
                log.warn(Util.getMessage("VersionMismatch"), sender, Version.print(version), Version.printVersion());
        }
        return match;
    }

    protected void sendTo(final Address dest, byte[] buf, int offset, int length) throws Exception {
        if(local_transport != null && local_transport.isLocalMember(dest)) {
            try {
                local_transport.sendTo(dest, buf, offset, length);
                return;
            }
            catch(Exception ex) {
                log.warn("failed sending message to %s via local transport, sending message via regular transport: %s",
                         dest, ex);
            }
        }

        PhysicalAddress physical_dest=dest instanceof PhysicalAddress? (PhysicalAddress)dest : getPhysicalAddressFromCache(dest);
        if(physical_dest != null) {
            sendUnicast(physical_dest,buf,offset,length);
            return;
        }
        if(who_has_cache.addIfAbsentOrExpired(dest)) { // true if address was added
            // FIND_MBRS must return quickly
            Responses responses=fetchResponsesFromDiscoveryProtocol(Collections.singletonList(dest));
            try {
                for(PingData data: responses) {
                    if(data.getAddress() != null && data.getAddress().equals(dest)) {
                        if((physical_dest=data.getPhysicalAddr()) != null) {
                            sendUnicast(physical_dest, buf, offset, length);
                            return;
                        }
                    }
                }
                log.warn(Util.getMessage("PhysicalAddrMissing"), local_addr, dest);
            }
            finally {
                responses.done();
            }
        }
    }

    /** Fetches the physical addrs for all mbrs and sends the msg to each physical address. Asks discovery for missing
     * members' physical addresses if needed */
    protected void sendToAll(byte[] buf, int offset, int length) throws Exception {
        List<Address> missing=null;
        Set<Address>  mbrs=members;
        boolean       local_send_successful=true;

        if(mbrs == null || mbrs.isEmpty())
            mbrs=logical_addr_cache.keySet();

        if(local_transport != null) {
            try {
                local_transport.sendToAll(buf, offset, length);
            }
            catch(Exception ex) {
                log.warn("failed sending group message via local transport, sending it via regular transport", ex);
                local_send_successful=false;
            }
        }

        List<PhysicalAddress> dests=new ArrayList<>(mbrs.size());
        for(Address mbr: mbrs) {
            if(local_send_successful && local_transport != null && local_transport.isLocalMember(mbr))
                continue; // skip if local transport sent the message successfully

            PhysicalAddress target=mbr instanceof PhysicalAddress? (PhysicalAddress)mbr : logical_addr_cache.get(mbr);
            if(target == null) {
                if(missing == null)
                    missing=new ArrayList<>(mbrs.size());
                missing.add(mbr);
                continue;
            }
            if(!Objects.equals(local_physical_addr, target))
                dests.add(target);
        }
        if(!dests.isEmpty())
            sendUnicasts(dests, buf, offset, length);
        if(missing != null)
            fetchPhysicalAddrs(missing);
    }

    protected void sendUnicasts(List<PhysicalAddress> dests, byte[] data, int offset, int length) throws Exception {
        for(PhysicalAddress dest: dests) {
            try {
                sendUnicast(dest, data, offset, length);
            }
            catch(SocketException | SocketTimeoutException sock_ex) {
                log.trace(Util.getMessage("FailureSendingToPhysAddr"), local_addr, dest, sock_ex);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailureSendingToPhysAddr"), local_addr, dest, t);
            }
        }
    }

    protected void fetchPhysicalAddrs(List<Address> missing) {
        long current_time=0;
        boolean do_send=false;
        synchronized(this) {
            if(last_discovery_request == 0 ||
              (current_time=timestamp()) - last_discovery_request >= MIN_WAIT_BETWEEN_DISCOVERIES) {
                last_discovery_request=current_time == 0? timestamp() : current_time;
                do_send=true;
            }
        }
        if(do_send) {
            missing.removeAll(logical_addr_cache.keySet());
            if(!missing.isEmpty()) {  // FIND_MBRS either returns immediately or is processed in a separate thread
                Responses rsps=fetchResponsesFromDiscoveryProtocol(missing);
                rsps.done();
            }
        }
    }

    protected Responses fetchResponsesFromDiscoveryProtocol(List<Address> missing) {
        return (Responses)up_prot.up(new Event(Event.FIND_MBRS, missing));
    }

    /**
     * If the sender is null, set our own address. We cannot just go ahead and set the address anyway, as we might send
     * a message on behalf of someone else, e.g. in case of retransmission, when the original sender has crashed.
     */
    protected void setSourceAddress(Message msg) {
        if(msg.getSrc() == null && local_addr != null)
            msg.setSrc(local_addr);
    }

    protected void handleConnect() throws Exception {
    }

    protected void handleDisconnect() {
    }

    @ManagedOperation(description="Evicts elements in the logical address cache which have expired")
    public void evictLogicalAddressCache() {
        evictLogicalAddressCache(false);
    }

    public void evictLogicalAddressCache(boolean force) {
        logical_addr_cache.removeMarkedElements(force);
        fetchLocalAddresses();
    }

    /** Associates the address with the physical address fetched from the cache */
    protected void registerLocalAddress(Address addr) {
        PhysicalAddress physical_addr=getPhysicalAddress();
        if(physical_addr == null)
            return;
        local_physical_addr=physical_addr;
        if(addr != null)
            addPhysicalAddressToCache(addr, physical_addr, true);
    }

    /** Grabs the local address (or addresses in the shared transport case) and registers them with the physical
     *  address in the transport's cache */
    protected void fetchLocalAddresses() {
        if(local_addr != null)
            registerLocalAddress(local_addr);
        else {
            Address addr=(Address)up_prot.up(new Event(Event.GET_LOCAL_ADDRESS));
            local_addr=addr;
            registerLocalAddress(addr);
        }
    }

    protected void startDiagnostics() throws Exception {
        if(diag_handler != null) {
            diag_handler.registerProbeHandler(this);
            diag_handler.start();
        }
    }

    protected void stopDiagnostics() {
        if(diag_handler != null) {
            diag_handler.unregisterProbeHandler(this);
            diag_handler.stop();
        }
    }

    protected void setThreadNames() {
        if(diag_handler != null)
            diag_handler.setThreadNames();
        if(bundler != null)
            bundler.renameThread();
    }

    protected void unsetThreadNames() {
        if(diag_handler != null)
            diag_handler.unsetThreadNames();
        if(bundler != null)
            bundler.renameThread();
    }

    protected void setInAllThreadFactories(String cluster_name, Address local_address, String pattern) {
        ThreadFactory[] factories={thread_factory};
        for(ThreadFactory factory: factories) {
            if(pattern != null)
                factory.setPattern(pattern);
            if(cluster_name != null) // if we have a shared transport, use singleton_name as cluster_name
                factory.setClusterName(cluster_name);
            if(local_address != null)
                factory.setAddress(local_address.toString());
        }
    }



    protected boolean addPhysicalAddressToCache(Address logical_addr, PhysicalAddress physical_addr, boolean overwrite) {
        return logical_addr != null && physical_addr != null &&
          overwrite? logical_addr_cache.add(logical_addr, physical_addr) : logical_addr_cache.addIfAbsent(logical_addr, physical_addr);
    }

    protected Collection<PhysicalAddress> getAllPhysicalAddressesFromCache() {
        return logical_addr_cache.nonRemovedValues();
    }

    protected void removeLogicalAddressFromCache(Address logical_addr) {
        if(logical_addr != null) {
            logical_addr_cache.remove(logical_addr);
            fetchLocalAddresses();
        }
    }

    /** Clears the cache. <em>Do not use, this is only for unit testing !</em> */
    @ManagedOperation(description="Clears the logical address cache; only used for testing")
    public void clearLogicalAddressCache() {
        logical_addr_cache.clear(true);
        fetchLocalAddresses();
    }

}
