package org.jgroups.mux;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.FIFOMessageQueue;
import org.jgroups.util.ThreadNamingPattern;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used for dispatching incoming messages. The Multiplexer implements UpHandler and registers with the associated
 * JChannel (there can only be 1 Multiplexer per JChannel). When up() is called with a message, the header of the
 * message is removed and the MuxChannel corresponding to the header's service ID is retrieved from the map,
 * and MuxChannel.up() is called with the message.
 * @author Bela Ban
 * @version $Id: Multiplexer.java,v 1.72 2007/10/01 11:49:07 vlada Exp $
 */
public class Multiplexer implements UpHandler {
    /** Map<String,MuxChannel>. Maintains the mapping between service IDs and their associated MuxChannels */
    private final ConcurrentMap<String,MuxChannel> services=new ConcurrentHashMap<String,MuxChannel>();
    private final JChannel channel;
    static final Log log=LogFactory.getLog(Multiplexer.class);
    static final String SEPARATOR="::";
    static final short SEPARATOR_LEN=(short)SEPARATOR.length();
    static final String NAME="MUX";    

    private boolean flush_present=true;
    private boolean blocked=false;

    /** Thread pool to concurrently process messages sent to different services */
    private ExecutorService thread_pool;

    /** To make sure messages sent to different services are processed concurrently (using the thread pool above), but
     * messages to the same service are processed FIFO */
    private FIFOMessageQueue<String,Runnable> fifo_queue=new FIFOMessageQueue<String,Runnable>();


    /** Cluster view */
    private volatile View view=null;

    private volatile Address local_addr=null;

    /** Map<String,Boolean>. Map of service IDs and booleans that determine whether getState() has already been called */
    private final Map<String,Boolean> state_transfer_listeners=new HashMap<String,Boolean>();

    /** Map<String,List<Address>>. A map of services as keys and lists of hosts as values */
    private final Map<String,List<Address>> service_state=new HashMap<String,List<Address>>();   

    /** Map<Address, Set<String>>. Keys are senders, values are a set of services hosted by that sender.
     * Used to collect responses to LIST_SERVICES_REQ */
    private final Map<Address, Set<String>> service_responses=new HashMap<Address, Set<String>>();

    private long SERVICES_RSP_TIMEOUT=10000;

    public Multiplexer() {
        this.channel=null;
        flush_present=isFlushPresent(); 
        
        // threadpool is enabled by default
        if(Global.getPropertyAsBoolean(Global.MUX_ENABLED, true)){
            thread_pool=createThreadPool();
        }
    }

    public Multiplexer(JChannel channel) {
        this.channel=channel;
        this.channel.setUpHandler(this);
        this.channel.setOpt(Channel.BLOCK, Boolean.TRUE); // we want to handle BLOCK events ourselves
        flush_present=isFlushPresent(); 
        
        //threadpool is enabled by default
        if(Global.getPropertyAsBoolean(Global.MUX_ENABLED, true)){
            thread_pool=createThreadPool();
        }
    }

    /**
     * @deprecated Use ${link #getServiceIds()} instead
     * @return The set of service IDs
     */
    public Set getApplicationIds() {
        return services != null? Collections.unmodifiableSet(services.keySet()) : null;
    }

    public Set<String> getServiceIds() {
        return services != null? Collections.unmodifiableSet(services.keySet()) : null;
    }


    public long getServicesResponseTimeout() {
        return SERVICES_RSP_TIMEOUT;
    }

    public void setServicesResponseTimeout(long services_rsp_timeout) {
        this.SERVICES_RSP_TIMEOUT=services_rsp_timeout;
    }

    /** Returns a copy of the current view <em>minus</em> the nodes on which service service_id is <em>not</em> running
     *
     * @param service_id
     * @return The service view
     */
    public View getServiceView(String service_id) {
        List<Address> hosts=service_state.get(service_id);
        if(hosts == null) return null;
        return generateServiceView(hosts);
    }

    public boolean stateTransferListenersPresent() {
        return state_transfer_listeners != null && !state_transfer_listeners.isEmpty();
    }
    
    public synchronized void registerForStateTransfer(String appl_id, String substate_id) {
        String key=appl_id;
        if(substate_id != null && substate_id.length() > 0)
            key+=SEPARATOR + substate_id;
        state_transfer_listeners.put(key, Boolean.FALSE);
    }

    public synchronized boolean getState(Address target, String id, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        if(state_transfer_listeners.isEmpty())
            return false;            
        
        for(Iterator<Map.Entry<String,Boolean>> it=state_transfer_listeners.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Boolean> entry = it.next();
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

        ThreadFactory factory=new ThreadFactory() {           
            ThreadGroup mux_threads=new ThreadGroup(Util.getGlobalThreadGroup(), "MultiplexerThreads");

            Map<String,Object> m = channel.getInfo(); 
            ThreadNamingPattern pattern = (ThreadNamingPattern) m.get("thread_naming_pattern");
            public Thread newThread(Runnable command) {
                Thread ret=new Thread(mux_threads, command, "Multiplexer");
                if(pattern != null){
                   pattern.renameThread(ret);
                }                
                return ret;
            }
        };

        min_threads=Global.getPropertyAsInteger(Global.MUX_MIN_THREADS, min_threads);
        max_threads=Global.getPropertyAsInteger(Global.MUX_MAX_THREADS, max_threads);
        keep_alive=Global.getPropertyAsLong(Global.MUX_KEEPALIVE, keep_alive);

        return new ThreadPoolExecutor(min_threads, max_threads, keep_alive, TimeUnit.MILLISECONDS,
                                      new SynchronousQueue<Runnable>(), factory,
                                      new ThreadPoolExecutor.CallerRunsPolicy());
    }

    protected void shutdownThreadPool() {
        if(thread_pool != null && !thread_pool.isShutdown()) {
            thread_pool.shutdownNow();
            try {
                thread_pool.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
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
        boolean rc, all_tranfers_ok = false;
        boolean flushStarted = channel.startFlush(false);
        if(flushStarted){
            try{
                for(String stateId:keys){
                    rc = channel.getState(target, stateId, timeout, false);
                    if(!rc)
                        throw new Exception("Failed transfer for state id " + stateId
                                            + ", state provider was "
                                            + target);
                }
                all_tranfers_ok = true;
            }catch(Exception e){
                log.warn("Failed multiple state transfer under one flush phase ", e);
            }finally{
                channel.stopFlush();
            }
        }
        return flushStarted && all_tranfers_ok;
    }
    
    public void sendServiceUpMessage(String service, Address host,boolean bypassFlush) throws Exception {
        sendServiceMessage(ServiceInfo.SERVICE_UP, service, host,bypassFlush, null);
        if(local_addr != null && host != null && local_addr.equals(host))
            handleServiceUp(service, host, false);
    }


    public void sendServiceDownMessage(String service, Address host,boolean bypassFlush) throws Exception {
        sendServiceMessage(ServiceInfo.SERVICE_DOWN, service, host,bypassFlush, null);
        if(local_addr != null && host != null && local_addr.equals(host))
            handleServiceDown(service, host, false);
    }




    /**
     * Remove mux header and dispatch to correct MuxChannel
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
                if(hdr.info != null) { // it is a service state request - not a default multiplex request
                    try {
                        handleServiceStateRequest(hdr.info, sender);
                    }
                    catch(Exception e) {
                        if(log.isErrorEnabled())
                            log.error("failure in handling service state request", e);
                    }
                    break;
                }

                MuxChannel mux_ch=services.get(hdr.id);
                if(mux_ch == null) {
                    return null;
                }
                return passToMuxChannel(mux_ch, evt, fifo_queue, sender, hdr.id, false); // don't block !

            case Event.VIEW_CHANGE:
                Vector<Address> old_members=view != null? view.getMembers() : null;
                view=(View)evt.getArg();
                Vector<Address> new_members=view != null? view.getMembers() : null;
                Vector<Address> left_members=Util.determineLeftMembers(old_members, new_members);

                if(view instanceof MergeView) {
                    final MergeView temp_merge_view=(MergeView)view.clone();
                    if(log.isTraceEnabled())
                        log.trace("received a MergeView: " + temp_merge_view + ", adjusting the service view");                    
                    try {                        
                        handleMergeView(temp_merge_view);                        
                    }
                    catch(Exception e) {
                        if(log.isErrorEnabled())
                            log.error("failed handling merge view", e);
                    }                                       
                }
                else { // regular view
                    synchronized(service_responses) {
                        service_responses.clear();
                    }
                    HashMap<String,List<Address>> payload = (HashMap<String, List<Address>>) view.getPayload("service_state");                                      
                    if(payload != null){
                        synchronized(service_state){                           
                            service_state.putAll(payload);
                        }
                    }
                    
                }
                if(!left_members.isEmpty())
                    adjustServiceViews(left_members);
                break;
                
            case Event.PREPARE_VIEW:
                View prepare_view=(View)evt.getArg();                  
                old_members=view != null? view.getMembers() : new Vector<Address>();                
                
                Vector <Address> added_members =  new Vector<Address>(prepare_view.getMembers());
                added_members.removeAll(old_members);
                
                if(!added_members.isEmpty()){                    
                    synchronized(service_state){
                        prepare_view.addPayload("service_state", service_state);    
                    }                    
                }
                break;
                

            case Event.SUSPECT:
                Address suspected_mbr=(Address)evt.getArg();

                synchronized(service_responses) {
                    service_responses.put(suspected_mbr, null);
                    service_responses.notifyAll();
                }
                passToAllMuxChannels(evt);
                break;

            case Event.GET_APPLSTATE:
            case Event.STATE_TRANSFER_OUTPUTSTREAM:
                return handleStateRequest(evt);

            case Event.GET_STATE_OK:
            case Event.STATE_TRANSFER_INPUTSTREAM:
                handleStateResponse(evt);
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                passToAllMuxChannels(evt);
                break;

            case Event.BLOCK:
                blocked=true;
                if(!services.isEmpty()) {
                    passToAllMuxChannels(evt, true, true); // do block and bypass the thread pool
                }
                waitUntilThreadPoolHasNoRunningTasks(1000);
                return null;

            case Event.UNBLOCK: // process queued-up MergeViews
                if(blocked)
                    blocked=false;
                passToAllMuxChannels(evt);
                break;

            default:
                passToAllMuxChannels(evt);
                break;
        }
        return null;
    }

    private int waitUntilThreadPoolHasNoRunningTasks(long timeout) {
        int num_threads=0;
        long end_time=System.currentTimeMillis() + timeout;

        while(fifo_queue != null && (num_threads=fifo_queue.size()) > 0 && System.currentTimeMillis() < end_time) {
            Util.sleep(100);
        }
        return num_threads;
    }


    public Channel createMuxChannel(JChannelFactory f, String id, String stack_name) throws Exception {
        MuxChannel ch;
        if(services.containsKey(id))
            throw new Exception("service ID \"" + id + "\" is already registered, cannot register duplicate ID");
        ch=new MuxChannel(f, channel, id, stack_name, this);
        services.put(id, ch);
        return ch;
    }




    private void passToAllMuxChannels(Event evt) {
        passToAllMuxChannels(evt, false, true);
    }


    private void passToAllMuxChannels(Event evt, boolean block, boolean bypass_thread_pool) {
        String service_name;
        MuxChannel ch;
        for(Map.Entry<String,MuxChannel> entry: services.entrySet()) {
            service_name=entry.getKey();
            ch=entry.getValue();
            // these events are directly delivered, don't get added to any queue
            passToMuxChannel(ch, evt, fifo_queue, null, service_name, block, bypass_thread_pool);
        }
    }

    public MuxChannel remove(String id) {
        return services.remove(id);
    }



    /** Closes the underlying JChannel if all MuxChannels have been disconnected */
    public void disconnect() {
        boolean all_disconnected=true;
        for(MuxChannel mux_ch: services.values()) {
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


    public void unregister(String appl_id) {
        services.remove(appl_id);
    }

    public boolean close() {
        boolean all_closed=true;
        for(MuxChannel mux_ch: services.values()) {
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
        for(MuxChannel mux_ch: services.values()) {
            mux_ch.setConnected(false);
            mux_ch.setClosed(true);
            mux_ch.closeMessageQueue(true);
        }
        shutdownThreadPool();
    }

    public boolean shutdown() {
        boolean all_closed=true;
        for(MuxChannel mux_ch: services.values()) {
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


    private boolean isFlushPresent() {
        return channel.getProtocolStack().findProtocol("FLUSH") != null;
    }   

    private Address getLocalAddress() {
        if(local_addr != null)
            return local_addr;
        if(channel != null)
            local_addr=channel.getLocalAddress();
        return local_addr;
    } 

    /**
    * Returns an Address of a state provider for a given service_id.
    * If preferredTarget is a member of a service view for a given service_id 
    * then preferredTarget is returned. Otherwise, service view coordinator is 
    * returned if such node exists. If service view is empty for a given service_id 
    * null is returned.  
    *
    * @param preferredTarget
    * @param service_id
    * @return
    */
    public Address getStateProvider(Address preferredTarget, String service_id) {
        Address result = null;      
        List<Address> hosts=service_state.get(service_id);
        if(hosts != null && !hosts.isEmpty()){
           if(hosts.contains(preferredTarget)){
              result = preferredTarget;
           }
           else{
              result = hosts.get(0);
           }          
        }
        return result;       
    }

    private void sendServiceMessage(byte type, String service, Address host,boolean bypassFlush, byte[] payload) throws Exception {
        if(host == null)
            host=getLocalAddress();
        if(host == null) {
            if(log.isWarnEnabled()) {
                log.warn("local_addr is null, cannot send ServiceInfo." + ServiceInfo.typeToString(type) + " message");
            }
            return;
        }

        ServiceInfo si=new ServiceInfo(type, service, host, payload);
        MuxHeader hdr=new MuxHeader(si);
        Message service_msg=new Message();
        service_msg.setFlag(Message.OOB);
        service_msg.putHeader(NAME, hdr);
        if(bypassFlush && flush_present)
           service_msg.putHeader(FLUSH.NAME, new FLUSH.FlushHeader(FLUSH.FlushHeader.FLUSH_BYPASS));
        
        channel.send(service_msg);
    }



    private Object handleStateRequest(Event evt) {
        StateTransferInfo info=(StateTransferInfo)evt.getArg();
        String id=info.state_id;
        String original_id=id;
        Address requester=info.target; // the sender of the state request

        try {
            int index=id.indexOf(SEPARATOR);
            if(index > -1) {
                info.state_id=id.substring(index + SEPARATOR_LEN);
                id=id.substring(0, index);  // similar reuse as above...
            }
            else {
                info.state_id=null;
            }

            MuxChannel mux_ch=services.get(id);
            if(mux_ch == null)
                throw new IllegalArgumentException("didn't find service with ID=" + id + " to fetch state from");

            // state_id will be null, get regular state from the service named state_id
            StateTransferInfo ret=(StateTransferInfo)passToMuxChannel(mux_ch, evt, fifo_queue, requester, id, true);
            if(ret != null)
        	ret.state_id=original_id;
            return ret;
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled())
                log.error("failed returning the application state, will return null", ex);
            return new StateTransferInfo(null, original_id, 0L, null);
        }
    }



    private void handleStateResponse(Event evt) {
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
            substate_id=tmp.substring(index+SEPARATOR_LEN);
        }
        else {
            appl_id=tmp;
            substate_id=null;
        }

        mux_ch=services.get(appl_id);
        if(mux_ch == null) {
            log.error("didn't find service with ID=" + appl_id + " to fetch state from");
        }
        else {
            StateTransferInfo tmp_info=info.copy();
            tmp_info.state_id=substate_id;
            Event tmpEvt=new Event(evt.getType(), tmp_info);
            passToMuxChannel(mux_ch, tmpEvt, fifo_queue, state_sender, appl_id, false);
        }
    }

    private void handleServiceStateRequest(ServiceInfo info, Address sender) throws Exception {
        switch(info.type) {                    
            case ServiceInfo.SERVICE_UP:
                handleServiceUp(info.service, info.host, true);
                break;
            case ServiceInfo.SERVICE_DOWN:
                handleServiceDown(info.service, info.host, true);
                break;
            case ServiceInfo.LIST_SERVICES_RSP:
                handleServicesRsp(sender, info.state);
                break;
            default:
                if(log.isErrorEnabled())
                    log.error("service request type " + info.type + " not known");
                break;
        }
    }

    private void handleServicesRsp(Address sender, byte[] state) throws Exception {       
        Set<String> s=(Set<String>) Util.objectFromByteBuffer(state);

        synchronized(service_responses) {
            Set<String> tmp=service_responses.get(sender);
            if(tmp == null)
                tmp=new HashSet<String>();
            tmp.addAll(s);

            service_responses.put(sender, tmp);
            if(log.isTraceEnabled())
                log.trace("received service response: " + sender + "(" + s.toString() + ")");
            service_responses.notifyAll();
        }
    }


    private void handleServiceDown(String service, Address host, boolean received) {
        List<Address>    hosts, hosts_copy;
        boolean removed=false;

        // discard if we sent this message
        if(received && host != null && local_addr != null && local_addr.equals(host)) {
            return;
        }

        synchronized(service_state) {
            hosts=service_state.get(service);
            if(hosts == null)
                return;
            removed=hosts.remove(host);
            hosts_copy=new ArrayList<Address>(hosts); // make a copy so we don't modify hosts in generateServiceView()
        }

        if(removed){
            View service_view = generateServiceView(hosts_copy);
            MuxChannel ch = services.get(service);
            if(ch != null){
                Event view_evt = new Event(Event.VIEW_CHANGE, service_view);               
                if(ch.isConnected())
                    passToMuxChannel(ch, view_evt, fifo_queue, null, service, false);
            }else{
                if(log.isTraceEnabled())
                    log.trace("service " + service
                              + " not found, cannot dispatch service view "
                              + service_view);
            }
        }

        Address local_address=getLocalAddress();
        if(local_address != null && host != null && host.equals(local_address))
            unregister(service);
    }


    private void handleServiceUp(String service, Address host, boolean received) {
        List<Address>    hosts, hosts_copy;
        boolean added=false;

        // discard if we sent this message
        if(received && host != null && local_addr != null && local_addr.equals(host)) {
            return;
        }

        synchronized(service_state) {
            hosts=service_state.get(service);
            if(hosts == null) {
                hosts=new ArrayList<Address>();
                service_state.put(service,  hosts);
            }
            if(!hosts.contains(host)) {
                hosts.add(host);
                added=true;
            }
            hosts_copy=new ArrayList<Address>(hosts); // make a copy so we don't modify hosts in generateServiceView()
        }

        if(added){
            View service_view = generateServiceView(hosts_copy);
            MuxChannel ch = services.get(service);
            if(ch != null){
                Event view_evt = new Event(Event.VIEW_CHANGE, service_view);
                passToMuxChannel(ch, view_evt, fifo_queue, null, service, false);
            }else{
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
        long time_to_wait=SERVICES_RSP_TIMEOUT, start;
        int num_members=view.size(); // include myself
        Map<Address, Set<String>> copy=null;

        byte[] data=Util.objectToByteBuffer(new HashSet<String>(services.keySet()));
        sendServiceMessage(ServiceInfo.LIST_SERVICES_RSP, null, channel.getLocalAddress(), true, data);

        synchronized(service_responses) {
            start=System.currentTimeMillis();
            try {
                while(time_to_wait > 0 && numResponses(service_responses) < num_members) {
                    service_responses.wait(time_to_wait);
                    time_to_wait-=System.currentTimeMillis() - start;
                }
                copy=new HashMap<Address, Set<String>>(service_responses);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled())
                    log.error("failed fetching a list of services from other members in the cluster, cannot handle merge view " + view, ex);
            }
        }

        if(log.isTraceEnabled())
            log.trace("merging service state, my service_state: " + service_state + ", received responses: " + copy);

        // merges service_responses with service_state and emits MergeViews for the services affected (MuxChannel)
        mergeServiceState(view, copy);
        service_responses.clear();       
    }

    private static int numResponses(Map<Address, Set<String>> m) {
        int num=0;
        Collection<Set<String>> values=m.values();
        for(Iterator<Set<String>> it=values.iterator(); it.hasNext();) {
            if(it.next() != null)
                num++;
        }

        return num;
    }


    private void mergeServiceState(MergeView view, Map<Address, Set<String>> copy) {
        Set<String> modified_services=new HashSet<String>();                             
        synchronized(service_state) {
            for(Iterator <Map.Entry<Address, Set<String>>> it=copy.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Address, Set<String>> entry = it.next();
                Address host=entry.getKey();
                Set<String> service_list=entry.getValue();
                if(service_list == null)
                    continue;

                for(String service:service_list) {                    
                    List<Address> my_services=service_state.get(service);
                    if(my_services == null) {
                        my_services=new ArrayList<Address>();
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
            List<Address> hosts=service_state.get(service);           
            Vector<Address> membersCopy = new Vector<Address>(view.getMembers());
            membersCopy.retainAll(hosts);
            MergeView v=new MergeView(view.getVid(), membersCopy, view.getSubgroups());            
            // ch.up(evt);
            passToMuxChannel(ch, new Event(Event.VIEW_CHANGE, v), fifo_queue, null, service, false);
        }
    }

    private void adjustServiceViews(Vector<Address> left_members) {
        for(Address member:left_members){
            try{
                adjustServiceView(member);
            }catch(Throwable t){
                if(log.isErrorEnabled())
                    log.error("failed adjusting service views", t);
            }
        }
    }

    private void adjustServiceView(Address host) {

        synchronized(service_state){
            for(Iterator<Map.Entry<String, List<Address>>> it = service_state.entrySet().iterator();it.hasNext();){
                Map.Entry<String, List<Address>> entry = it.next();
                String service = entry.getKey();
                List<Address> hosts = entry.getValue();
                if(hosts == null)
                    continue;

                if(hosts.remove(host)){
                    // make a copy so we don't modify hosts in
                    // generateServiceView()
                    View service_view = generateServiceView(new ArrayList<Address>(hosts));
                    MuxChannel ch = services.get(service);
                    if(ch != null){
                        Event view_evt = new Event(Event.VIEW_CHANGE, service_view);    
                        if(ch.isConnected())
                            passToMuxChannel(ch, view_evt, fifo_queue, null, service, false);
                    }else{
                        if(log.isTraceEnabled())
                            log.trace("service " + service
                                      + " not found, cannot dispatch service view "
                                      + service_view);
                    }
                }
                Address local_address = getLocalAddress();
                if(local_address != null && host != null && host.equals(local_address))
                    unregister(service);
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
            tmp.add(local_addr);
            view= new View(new ViewId(local_addr), tmp);
        }
        Vector<Address> members=new Vector<Address>(view.getMembers());
        members.retainAll(hosts);
        return new View(view.getVid(), members);
    }

    private Object passToMuxChannel(MuxChannel ch, Event evt, final FIFOMessageQueue<String,Runnable> queue,
                                         final Address sender, final String dest, boolean block) {
        return passToMuxChannel(ch, evt, queue, sender, dest, block, false);
    }



    private Object passToMuxChannel(MuxChannel ch, Event evt, final FIFOMessageQueue<String,Runnable> queue,
                                    final Address sender, final String dest, boolean block, boolean bypass_thread_pool) {
        if(thread_pool == null || bypass_thread_pool) {
            return ch.up(evt);
        }

        Task task=new Task(ch, evt, queue, sender, dest, block);
        ExecuteTask execute_task=new ExecuteTask(fifo_queue);  // takes Task from queue and executes it

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

    public void addServiceIfNotPresent(String id, MuxChannel ch) {
        services.putIfAbsent(id, ch);
    }


    private static class Task implements Runnable {
        Exchanger<Object> exchanger;
        MuxChannel        channel;
        Event             evt;
        FIFOMessageQueue<String,Runnable> queue;
        Address           sender;
        String            dest;

        Task(MuxChannel channel, Event evt, FIFOMessageQueue<String,Runnable> queue, Address sender, String dest, boolean result_expected) {
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
            }
        }
    }


}
