package org.jgroups.mux;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.TimeoutException;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.FIFOMessageQueue;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.*;

/**
 * Used for dispatching incoming messages. The Multiplexer implements UpHandler and registers with the associated
 * JChannel (there can only be 1 Multiplexer per JChannel). When up() is called with a message, the header of the
 * message is removed and the MuxChannel corresponding to the header's service ID is retrieved from the map,
 * and MuxChannel.up() is called with the message.
 * @author Bela Ban
 * @version $Id: Multiplexer.java,v 1.56 2007/05/08 18:47:04 vlada Exp $
 */
public class Multiplexer implements UpHandler {
    /** Map<String,MuxChannel>. Maintains the mapping between service IDs and their associated MuxChannels */
    private final Map<String,MuxChannel> services=new HashMap<String,MuxChannel>();
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
    View view=null;

    Address local_addr=null;

    /** Map<String,Boolean>. Map of service IDs and booleans that determine whether getState() has already been called */
    private final Map<String,Boolean> state_transfer_listeners=new HashMap<String,Boolean>();

    /** Map<String,List<Address>>. A map of services as keys and lists of hosts as values */
    private final Map<String,List<Address>> service_state=new HashMap<String,List<Address>>();

    /** Used to wait on service state information */
    private final Promise service_state_promise=new Promise();

    /** Map<Address, Set<String>>. Keys are senders, values are a set of services hosted by that sender.
     * Used to collect responses to LIST_SERVICES_REQ */
    private final Map<Address, Set<String>> service_responses=new HashMap<Address, Set<String>>();

    private long SERVICES_RSP_TIMEOUT=10000;

    public Multiplexer() {
        this.channel=null;
        flush_present=isFlushPresent(); 
        
        // threadpool is enabled by default
	if(Global.getPropertyAsBoolean(Global.MUX_ENABLED, true)){
	    thread_pool = createThreadPool();
	}
    }

    public Multiplexer(JChannel channel) {
        this.channel=channel;
        this.channel.setUpHandler(this);
        this.channel.setOpt(Channel.BLOCK, Boolean.TRUE); // we want to handle BLOCK events ourselves
        flush_present=isFlushPresent(); 
        
        //threadpool is enabled by default
        if(Global.getPropertyAsBoolean(Global.MUX_ENABLED, true)){
	    thread_pool = createThreadPool();
	}
    }

    /**
     * @deprecated Use ${link #getServiceIds()} instead
     * @return The set of service IDs
     */
    public Set getApplicationIds() {
        return services != null? services.keySet() : null;
    }

    public Set<String> getServiceIds() {
        return services != null? services.keySet() : null;
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
        List hosts=service_state.get(service_id);
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
        if(state_transfer_listeners == null)
            return false;
        Map.Entry entry;
        String key;
        for(Iterator it=state_transfer_listeners.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(String)entry.getKey();
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

        Collection values=state_transfer_listeners.values();
        boolean all_true=Util.all(values, Boolean.TRUE);
        if(!all_true)
            return true; // pseudo

        boolean rc=false;           
        Set keys=new HashSet(state_transfer_listeners.keySet());
        rc=fetchServiceStates(target, keys, timeout);
        state_transfer_listeners.clear();        
        return rc;
    }
    
    protected ExecutorService createThreadPool() {
	int min_threads = 1, max_threads = 4;
	long keep_alive = 30000;

	ThreadFactory factory = new ThreadFactory() {
	    int num = 1;

	    ThreadGroup mux_threads = new ThreadGroup(Util.getGlobalThreadGroup(),
						      "MultiplexerThreads");

	    public Thread newThread(Runnable command) {
		Thread ret = new Thread(mux_threads, command, "Multiplexer-" + num++);
		ret.setDaemon(true);
		return ret;
	    }
	};

	min_threads = Global.getPropertyAsInteger(Global.MUX_MIN_THREADS, min_threads);
	max_threads = Global.getPropertyAsInteger(Global.MUX_MAX_THREADS, max_threads);
	keep_alive = Global.getPropertyAsLong(Global.MUX_KEEPALIVE, keep_alive);

	return new ThreadPoolExecutor(min_threads, max_threads, keep_alive, TimeUnit.MILLISECONDS,
				      new SynchronousQueue(), factory,
				      new ThreadPoolExecutor.CallerRunsPolicy());
    }
    
    protected void shutdownThreadPool() {
	if(thread_pool != null && !thread_pool.isShutdown()){
	    thread_pool.shutdownNow();
	    try{
		thread_pool.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME,
					     TimeUnit.MILLISECONDS);
	    }catch(InterruptedException e){
	    }
	}
    }

    /** Fetches the app states for all service IDs in keys.
     * The keys are a duplicate list, so it cannot be modified by the caller of this method
     * @param keys
     */
    private boolean fetchServiceStates(Address target, Set keys, long timeout) throws ChannelClosedException, ChannelNotConnectedException {
        boolean rc, all_rcs=true;
        String appl_id;
        for(Iterator it=keys.iterator(); it.hasNext();) {
            appl_id=(String)it.next();
            rc=channel.getState(target, appl_id, timeout);
            if(rc == false)
                all_rcs=false;
        }
        return all_rcs;
    }


    /**
     * Fetches the map of services and hosts from the coordinator (Multiplexer). No-op if we are the coordinator
     */
    public void fetchServiceInformation() throws Exception {
        while(true) {
            Address coord=getCoordinator(), local_address=channel != null? channel.getLocalAddress() : null;
            boolean is_coord=coord != null && local_address != null && local_address.equals(coord);
            if(is_coord) {
                if(log.isTraceEnabled())
                    log.trace("I'm coordinator, will not fetch service state information");
                break;
            }

            ServiceInfo si=new ServiceInfo(ServiceInfo.STATE_REQ, null, null, null);
            MuxHeader hdr=new MuxHeader(si);
            Message state_req=new Message(coord, null, null);
            state_req.putHeader(NAME, hdr);
            service_state_promise.reset();
            channel.send(state_req);

            try {
                byte[] state=(byte[])service_state_promise.getResultWithTimeout(2000);
                if(state != null) {
                    Map new_state=(Map)Util.objectFromByteBuffer(state);
                    synchronized(service_state) {
                        service_state.clear();
                        service_state.putAll(new_state);
                    }
                    if(log.isTraceEnabled())
                        log.trace("service state was set successfully (" + service_state.size() + " entries)");
                }
                else {
                    if(log.isWarnEnabled())
                        log.warn("received service state was null");
                }
                break;
            }
            catch(TimeoutException e) {
                if(log.isTraceEnabled())
                    log.trace("timed out waiting for service state from " + coord + ", retrying");
            }
        }
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
                    log.warn("service " + hdr.id + " not currently running, discarding message " + msg);
                    return null;
                }
                return passToMuxChannel(mux_ch, evt, fifo_queue, sender, hdr.id, false); // don't block !

            case Event.VIEW_CHANGE:
                Vector old_members=view != null? view.getMembers() : null;
                view=(View)evt.getArg();
                Vector new_members=view != null? view.getMembers() : null;
                Vector left_members=Util.determineLeftMembers(old_members, new_members);

                if(view instanceof MergeView) {
                    final MergeView temp_merge_view=(MergeView)view.clone();
                    if(log.isTraceEnabled())
                        log.trace("received a MergeView: " + temp_merge_view + ", adjusting the service view");                    
                    try {                        
                        Thread merge_handler=new Thread() {
                            public void run() {
                                try {
                                    handleMergeView(temp_merge_view);
                                }
                                catch(Exception e) {
                                    if(log.isErrorEnabled())
                                        log.error("problems handling merge view", e);
                                }
                            }
                        };
                        merge_handler.setName("merge handler view_change");
                        merge_handler.setDaemon(false);
                        merge_handler.start();
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
                }
                if(!left_members.isEmpty())
                    adjustServiceViews(left_members);
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
                    passToAllMuxChannels(evt);
                }
                return null;

            case Event.UNBLOCK: // process queued-up MergeViews
                if(!blocked) {
                    passToAllMuxChannels(evt);
                    return null;
                }
                else
                    blocked=false;
                
                passToAllMuxChannels(evt);
                break;

            default:
                passToAllMuxChannels(evt);
                break;
        }
        return null;
    }


    public Channel createMuxChannel(JChannelFactory f, String id, String stack_name) throws Exception {
        MuxChannel ch;
        synchronized(services) {
            if(services.containsKey(id))
                throw new Exception("service ID \"" + id + "\" is already registered, cannot register duplicate ID");
            ch=new MuxChannel(f, channel, id, stack_name, this);
            services.put(id, ch);
        }
        return ch;
    }




    private void passToAllMuxChannels(Event evt) {
        String service_name;
        MuxChannel ch;
        for(Map.Entry<String,MuxChannel> entry: services.entrySet()) {
            service_name=entry.getKey();
            ch=entry.getValue();
            // these events are directly delivered, don't get added to any queue
            passToMuxChannel(ch, evt, fifo_queue, null, service_name, false);
        }
    }

    public MuxChannel remove(String id) {
        synchronized(services) {
            return services.remove(id);
        }
    }



    /** Closes the underlying JChannel if all MuxChannels have been disconnected */
    public void disconnect() {
        boolean all_disconnected=true;
        synchronized(services) {
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
    }


    public void unregister(String appl_id) {
        synchronized(services) {
            services.remove(appl_id);
        }
    }

    public boolean close() {
        boolean all_closed=true;
        synchronized(services) {
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
    }

    public void closeAll() {
        synchronized(services) {
            for(MuxChannel mux_ch: services.values()) {
                mux_ch.setConnected(false);
                mux_ch.setClosed(true);
                mux_ch.closeMessageQueue(true);
            }
        }
        shutdownThreadPool();
    }

    public boolean shutdown() {
        boolean all_closed=true;
        synchronized(services) {
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
    }


    private boolean isFlushPresent() {
        return channel.getProtocolStack().findProtocol("FLUSH") != null;
    }


    private void sendServiceState() throws Exception {
        Object[] my_services=services.keySet().toArray();
        byte[] data=Util.objectToByteBuffer(my_services);
        sendServiceMessage(ServiceInfo.LIST_SERVICES_RSP, null, channel.getLocalAddress(), true, data);
    }


    private Address getLocalAddress() {
        if(local_addr != null)
            return local_addr;
        if(channel != null)
            local_addr=channel.getLocalAddress();
        return local_addr;
    }

    private Address getCoordinator() {
        if(channel != null) {
            View v=channel.getView();
            if(v != null) {
                Vector members=v.getMembers();
                if(members != null && !members.isEmpty()) {
                    return (Address)members.firstElement();
                }
            }
        }
        return null;
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
        List hosts=service_state.get(service_id);
        if(hosts != null && !hosts.isEmpty()){
           if(hosts.contains(preferredTarget)){
              result = preferredTarget;
           }
           else{
              result = (Address)hosts.get(0);
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
            case ServiceInfo.STATE_REQ:
                byte[] state;
                synchronized(service_state) {
                    state=Util.objectToByteBuffer(service_state);
                }
                ServiceInfo si=new ServiceInfo(ServiceInfo.STATE_RSP, null, null, state);
                MuxHeader hdr=new MuxHeader(si);
                Message state_rsp=new Message(sender);
                state_rsp.putHeader(NAME, hdr);
                channel.send(state_rsp);
                break;
            case ServiceInfo.STATE_RSP:
                service_state_promise.setResult(info.state);
                break;
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
        Object[] keys=(Object[])Util.objectFromByteBuffer(state);
        Set s=new HashSet();
        for(int i=0; i < keys.length; i++)
            s.add(keys[i]);



        synchronized(service_responses) {
            Set tmp=service_responses.get(sender);
            if(tmp == null)
                tmp=new HashSet();
            tmp.addAll(s);

            service_responses.put(sender, tmp);
            if(log.isTraceEnabled())
                log.trace("received service response: " + sender + "(" + s.toString() + ")");
            service_responses.notifyAll();
        }
    }


    private void handleServiceDown(String service, Address host, boolean received) {
        List    hosts, hosts_copy;
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
            hosts_copy=new ArrayList(hosts); // make a copy so we don't modify hosts in generateServiceView()
        }

        if(removed) {
            View service_view=generateServiceView(hosts_copy);
            if(service_view != null) {
                MuxChannel ch=services.get(service);
                if(ch != null) {
                    Event view_evt=new Event(Event.VIEW_CHANGE, service_view);
                    // ch.up(view_evt);
                    passToMuxChannel(ch, view_evt, fifo_queue, null, service, false);
                }
                else {
                    if(log.isTraceEnabled())
                        log.trace("service " + service + " not found, cannot dispatch service view " + service_view);
                }
            }
        }

        Address local_address=getLocalAddress();
        if(local_address != null && host != null && host.equals(local_address))
            unregister(service);
    }


    private void handleServiceUp(String service, Address host, boolean received) {
        List    hosts, hosts_copy;
        boolean added=false;

        // discard if we sent this message
        if(received && host != null && local_addr != null && local_addr.equals(host)) {
            return;
        }

        synchronized(service_state) {
            hosts=service_state.get(service);
            if(hosts == null) {
                hosts=new ArrayList();
                service_state.put(service,  hosts);
            }
            if(!hosts.contains(host)) {
                hosts.add(host);
                added=true;
            }
            hosts_copy=new ArrayList(hosts); // make a copy so we don't modify hosts in generateServiceView()
        }

        if(added) {
            View service_view=generateServiceView(hosts_copy);
            if(service_view != null) {
                MuxChannel ch=services.get(service);
                if(ch != null) {
                    Event view_evt=new Event(Event.VIEW_CHANGE, service_view);
                    // ch.up(view_evt);
                    passToMuxChannel(ch, view_evt, fifo_queue, null, service, false);
                }
                else {
                    if(log.isTraceEnabled())
                        log.trace("service " + service + " not found, cannot dispatch service view " + service_view);
                }
            }
        }
    }


    /**
     * Fetches the service states from everyone else in the cluster. Once all states have been received and inserted into
     * service_state, compute a service view (a copy of MergeView) for each service and pass it up
     * @param view
     */
    private void handleMergeView(MergeView view) throws Exception {
        long time_to_wait=SERVICES_RSP_TIMEOUT, start;
        int num_members=view.size(); // include myself
        Map copy=null;

        sendServiceState();

        synchronized(service_responses) {
            start=System.currentTimeMillis();
            try {
                while(time_to_wait > 0 && numResponses(service_responses) < num_members) {
                    service_responses.wait(time_to_wait);
                    time_to_wait-=System.currentTimeMillis() - start;
                }
                copy=new HashMap(service_responses);
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

    private int numResponses(Map m) {
        int num=0;
        Collection values=m.values();
        for(Iterator it=values.iterator(); it.hasNext();) {
            if(it.next() != null)
                num++;
        }

        return num;
    }


    private void mergeServiceState(MergeView view, Map copy) {
        Set modified_services=new HashSet();
        Map.Entry entry;
        Address host;     // address of the sender
        Set service_list; // Set<String> of services
        List my_services;
        String service;

        synchronized(service_state) {
            for(Iterator it=copy.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                host=(Address)entry.getKey();
                service_list=(Set)entry.getValue();
                if(service_list == null)
                    continue;

                for(Iterator it2=service_list.iterator(); it2.hasNext();) {
                    service=(String)it2.next();
                    my_services=service_state.get(service);
                    if(my_services == null) {
                        my_services=new ArrayList();
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
        for(Iterator it=modified_services.iterator(); it.hasNext();) {
            service=(String)it.next();
            MuxChannel ch=services.get(service);
            my_services=service_state.get(service);           
            Vector membersCopy = new Vector(view.getMembers());
            membersCopy.retainAll(my_services);
            MergeView v=new MergeView(view.getVid(), membersCopy, view.getSubgroups());
            Event evt=new Event(Event.VIEW_CHANGE, v);
            // ch.up(evt);
            passToMuxChannel(ch, evt, fifo_queue, null, service, false);
        }
    }

    private void adjustServiceViews(Vector left_members) {
        if(left_members != null)
            for(int i=0; i < left_members.size(); i++) {
                try {
                    adjustServiceView((Address)left_members.elementAt(i));
                }
                catch(Throwable t) {
                    if(log.isErrorEnabled())
                        log.error("failed adjusting service views", t);
                }
            }
    }

    private void adjustServiceView(Address host) {
        Map.Entry entry;
        List hosts, hosts_copy;
        String service;
        boolean removed=false;

        synchronized(service_state) {
            for(Iterator it=service_state.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                service=(String)entry.getKey();
                hosts=(List)entry.getValue();
                if(hosts == null)
                    continue;

                removed=hosts.remove(host);
                hosts_copy=new ArrayList(hosts); // make a copy so we don't modify hosts in generateServiceView()

                if(removed) {
                    View service_view=generateServiceView(hosts_copy);
                    if(service_view != null) {
                        MuxChannel ch=services.get(service);
                        if(ch != null) {
                            Event view_evt=new Event(Event.VIEW_CHANGE, service_view);
                            // ch.up(view_evt);
                            passToMuxChannel(ch, view_evt, fifo_queue, null, service, false);
                        }
                        else {
                            if(log.isTraceEnabled())
                                log.trace("service " + service + " not found, cannot dispatch service view " + service_view);
                        }
                    }
                }
                Address local_address=getLocalAddress();
                if(local_address != null && host != null && host.equals(local_address))
                    unregister(service);
            }
        }
    }


    /**
     * Create a copy of view which contains only members which are present in hosts. Call viewAccepted() on the MuxChannel
     * which corresponds with service. If no members are removed or added from/to view, this is a no-op.
     * @param hosts List<Address>
     * @return the servicd view (a modified copy of the real view), or null if the view was not modified
     */
    private View generateServiceView(List hosts) {
        Vector members=new Vector(view.getMembers());
        members.retainAll(hosts);
        return new View(view.getVid(), members);
    }


    /** Tell the underlying channel to start the flush protocol, this will be handled by FLUSH */
    private void startFlush(long timeout) {
    	Map atts = new HashMap();	           	
     	atts.put("timeout",new Long(timeout));
        channel.down(new Event(Event.SUSPEND,atts));
    }

    /** Tell the underlying channel to stop the flush, and resume message sending. This will be handled by FLUSH */
    private void stopFlush() {
        channel.down(new Event(Event.RESUME));
    }


    private Object passToMuxChannel(MuxChannel ch, Event evt, final FIFOMessageQueue<String,Runnable> queue,
                                         final Address sender, final String dest, boolean block) {
        if(thread_pool == null)
            return ch.up(evt);

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
        MuxChannel tmp;
        synchronized(services) {
            tmp=services.get(id);
            if(tmp == null) {
                services.put(id, ch);
            }
        }
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
