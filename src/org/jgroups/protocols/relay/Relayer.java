package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.*;

/**
 * Maintains bridges and routing table. Does the routing of outgoing messages and dispatches incoming messages to
 * the right members.<p/>
 * A Relayer cannot be reused once it is stopped, but a new Relayer instance must be created.
 * @author Bela Ban
 * @since 3.2
 */
public class Relayer {
    /** The routing table. Site IDs are used as indices, e.g. the route for site=2 is at index 2. */
    protected Route[]                      routes;

    /** The bridges which are used to connect to different sites */
    protected final Queue<Bridge>          bridges=new ConcurrentLinkedQueue<Bridge>();

    protected final Log                    log;

    protected final RELAY2                 relay;

    /** Flag set when stop() is called. Since a Relayer should not be used after stop() has been called, a new
     * instance needs to be created */
    protected volatile boolean             done;

    protected boolean                      stats;

    // Used to store messages for a site with status UNKNOWN. Messages will be flushed when the status changes to UP, or
    // a SITE-UNREACHABLE message will be sent to each member *once* when the status changes to DOWN
    protected final ConcurrentMap<Short,BlockingQueue<Message>> fwd_queue=new ConcurrentHashMap<Short,BlockingQueue<Message>>();

    /** Map to store tasks which set the status of a site from UNKNOWN to DOWN. These are started when a site is
     * set to UNKNOWN, but they need to be cancelled when the status goes from UNKNOWN back to UP <em>before</em>
     * they kick in.*/
    protected final ConcurrentMap<Short,Future<?>>   down_tasks=new ConcurrentHashMap<Short,Future<?>>();



    public Relayer(RELAY2 relay, Log log, int num_routes) {
        this.relay=relay;
        stats=relay.statsEnabled();
        this.log=log;
        init(num_routes);
    }


    public boolean done() {return done;}

    
    /**
     * Creates all bridges from site_config and connects them (joining the bridge clusters)
     * @param bridge_configs A list of bridge configurations
     * @param bridge_name The name of the local bridge channel, prefixed with '_'.
     * @param my_site_id The ID of this site
     * @throws Throwable
     */
    public void start(List<RelayConfig.BridgeConfig> bridge_configs, String bridge_name, final short my_site_id)
      throws Throwable {
        if(done) {
            if(log.isTraceEnabled())
                log.trace(relay.getLocalAddress() + ": will not start the Relayer as stop() has been called");
            return;
        }
        try {
            for(RelayConfig.BridgeConfig bridge_config: bridge_configs) {
                Bridge bridge=new Bridge(bridge_config.createChannel(), bridge_config.getClusterName(), bridge_name,
                                         new AddressGenerator() {
                                             public Address generateAddress() {
                                                 UUID uuid=UUID.randomUUID();
                                                 return new SiteUUID(uuid, null, my_site_id);
                                             }
                                         });
                bridges.add(bridge);
            }
            for(Bridge bridge: bridges)
                bridge.start();
        }
        catch(Throwable t) {
            stop();
            throw t;
        }
        finally {
            if(done) {
                if(log.isTraceEnabled())
                    log.trace(relay.getLocalAddress() + ": stop() was called while starting the relayer; stopping the relayer now");
                stop();
            }
        }
    }


    protected void init(int num_routes) {
        if(routes == null)
            routes=new Route[num_routes];
        for(short i=0; i < num_routes; i++) {
            if(routes[i] == null)
                routes[i]=new Route(null, null, RELAY2.RouteStatus.DOWN);
        }
    }


    /**
     * Disconnects and destroys all bridges
     */
    public void stop() {
        done=true;
        List<Future<?>> tasks=new ArrayList<Future<?>>(down_tasks.values());
        down_tasks.clear();
        for(Future<?> task: tasks)
            task.cancel(true);

        for(Bridge bridge: bridges)
            bridge.stop();
        bridges.clear();
        fwd_queue.clear();
        for(Route route: routes)
            route.reset();
    }


    public synchronized String printRoutes() {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < routes.length; i++) {
            Route route=routes[i];
            if(route != null) {
                String name=SiteUUID.getSiteName((short)i);
                if(name == null)
                    name=String.valueOf(i);
                sb.append(name + " --> " + route + "\n");
            }
        }

        return sb.toString();
    }


    protected synchronized void setRoute(short site, JChannel bridge, SiteMaster site_master, RELAY2.RouteStatus status) {
        Route existing_route=routes[site];
        existing_route.bridge(bridge);
        existing_route.siteMaster(site_master);
        existing_route.status(status);
    }



    protected synchronized Route getRoute(short site) {
        if(site <= routes.length -1)
            return routes[site];
        return null;
    }

    protected synchronized List<Route> getRoutes(short ... excluded_sites) {
        List<Route> retval=new ArrayList<Route>(routes.length);
        for(short i=0; i < routes.length; i++) {
            Route tmp=routes[i];
            if(tmp != null && tmp.status() != RELAY2.RouteStatus.DOWN) {
                if(!isExcluded(tmp, excluded_sites))
                    retval.add(tmp);
            }
        }

        return retval;
    }

    protected View getBridgeView(String cluster_name) {
        if(cluster_name == null || bridges == null)
            return null;
        for(Bridge bridge: bridges) {
            if(bridge.cluster_name != null && bridge.cluster_name.equals(cluster_name))
                return bridge.view;
        }
        return null;
    }

    protected static boolean isExcluded(Route route, short... excluded_sites) {
        if(excluded_sites == null)
            return false;
        short site=((SiteUUID)route.site_master).getSite();
        for(short excluded_site: excluded_sites)
            if(site == excluded_site)
                return true;
        return false;
    }




    /**
     * Includes information about the site master of the route and the channel to be used
     */
    public class Route {
        private   volatile Address            site_master;
        private   volatile JChannel           bridge;
        private   volatile RELAY2.RouteStatus status;

        public Route(Address site_master, JChannel bridge) {
            this(site_master, bridge, RELAY2.RouteStatus.UP);
        }

        public Route(Address site_master, JChannel bridge, RELAY2.RouteStatus status) {
            this.site_master=site_master;
            this.bridge=bridge;
            this.status=status;
        }

        public JChannel           bridge()                               {return bridge;}
        public Route              bridge(JChannel new_bridge)            {bridge=new_bridge; return this;}
        public Address            siteMaster()                           {return site_master;}
        public Route              siteMaster(Address new_site_master)    {site_master=new_site_master; return this;}
        public RELAY2.RouteStatus status()                               {return status;}
        public Route              status(RELAY2.RouteStatus new_status)  {status=new_status; return this;}
        public Route              reset()     {return bridge(null).siteMaster(null).status(RELAY2.RouteStatus.DOWN);}

        public void send(short target_site, Address final_destination, Address original_sender, final Message msg) {
            switch(status) {
                case DOWN:    // send SITE-UNREACHABLE message back to sender
                    relay.sendSiteUnreachableTo(original_sender, target_site);
                    return;
                case UNKNOWN: // queue message
                    BlockingQueue<Message> queue=fwd_queue.get(target_site);
                    if(queue == null) {
                        queue=new LinkedBlockingQueue<Message>(relay.forwardQueueMaxSize());
                        BlockingQueue<Message> existing=fwd_queue.putIfAbsent(target_site, queue);
                        if(existing != null)
                            queue=existing;
                    }
                    try {
                        queue.put(createMessage(new SiteMaster(target_site), final_destination, original_sender, msg));
                    }
                    catch(InterruptedException e) {
                    }
                    return;
            }

            // at this point status is RUNNING
            if(log.isTraceEnabled())
                log.trace("routing message to " + final_destination + " via " + site_master);
            long start=stats? System.nanoTime() : 0;
            try {
                Message copy=createMessage(site_master, final_destination, original_sender, msg);
                bridge.send(copy);
                if(stats) {
                    relay.addToRelayedTime(System.nanoTime() - start);
                    relay.incrementRelayed();
                }
            }
            catch(Exception e) {
                log.error("failure relaying message", e);
            }
        }


        public String toString() {
            return (site_master != null? site_master + " " : "") + "[" + status + "]";
        }

        protected Message createMessage(Address target, Address final_destination, Address original_sender, final Message msg) {
            Message copy=relay.copy(msg).dest(target).src(null);
            RELAY2.Relay2Header hdr=new RELAY2.Relay2Header(RELAY2.Relay2Header.DATA, final_destination, original_sender);
            copy.putHeader(relay.getId(), hdr);
            return copy;
        }
    }


    protected class Bridge extends ReceiverAdapter {
        protected JChannel     channel;
        protected final String cluster_name;
        protected View         view;

        protected Bridge(final JChannel ch, final String cluster_name, String channel_name, AddressGenerator addr_generator) throws Exception {
            this.channel=ch;
            channel.setName(channel_name);
            channel.setReceiver(this);
            channel.setAddressGenerator(addr_generator);
            this.cluster_name=cluster_name;
        }

        protected void start() throws Exception {
            channel.connect(cluster_name);
        }

        protected void stop() {
            Util.close(channel);
        }

        public void receive(Message msg) {
            RELAY2.Relay2Header hdr=(RELAY2.Relay2Header)msg.getHeader(relay.getId());
            if(hdr == null) {
                log.warn("received a message without a relay header; discarding it");
                return;
            }
            relay.handleRelayMessage(hdr, msg);
        }

        public void viewAccepted(View new_view) {
            List<Address> left_mbrs=this.view != null? Util.determineLeftMembers(this.view.getMembers(),new_view.getMembers()) : null;
            this.view=new_view;

            if(log.isTraceEnabled())
                log.trace("[Relayer " + channel.getAddress() + "] view: " + new_view);

            if(left_mbrs != null) {
                Set<Short> sites=new HashSet<Short>(); // site-ids to be set to UNKNOWN
                for(Address addr: left_mbrs)
                    if(addr instanceof SiteUUID)
                        sites.add(((SiteUUID)addr).getSite());
                for(short site: sites)
                    changeStatusToUnknown(site);
            }

            // map of site-ids and associated site master addresses
            Map<Short,Address> sites=new HashMap<Short,Address>();     // site-ids to be set to UP
            for(Address addr: new_view.getMembers())
                if(addr instanceof SiteUUID)
                    sites.put(((SiteUUID)addr).getSite(), addr);
            for(Map.Entry<Short,Address> entry: sites.entrySet())
                changeStatusToUp(entry.getKey(), channel, entry.getValue());
        }


        protected void changeStatusToUnknown(final short site) {
            Route route=routes[site];
            route.status(RELAY2.RouteStatus.UNKNOWN); // messages are queued from now on
            Future<?> task=relay.getTimer().schedule(new Runnable() {
                public void run() {
                    Route route=routes[site];
                    if(route.status() == RELAY2.RouteStatus.UNKNOWN)
                        changeStatusToDown(site);
                }
            }, relay.siteDownTimeout(), TimeUnit.MILLISECONDS);

            if(task == null) // schedule() failed as the pool was already shut down
                return;
            Future<?> existing_task=down_tasks.put(site, task);
            if(existing_task != null)
                existing_task.cancel(true);
        }

        protected void changeStatusToDown(short id) {
            Route route=routes[id];
            if(route.status() == RELAY2.RouteStatus.UNKNOWN)
                route.status(RELAY2.RouteStatus.DOWN);    // SITE-UNREACHABLE responses are sent in this state
            else {
                log.warn(relay.getLocalAddress() + ": didn't change status of " + SiteUUID.getSiteName(id) + " to DOWN as it is UP");
                return;
            }
            BlockingQueue<Message> msgs=fwd_queue.remove(id);
            if(msgs != null && !msgs.isEmpty()) {
                Set<Address> targets=new HashSet<Address>(); // we need to send a SITE-UNREACHABLE only *once* to every sender
                for(Message msg: msgs) {
                    RELAY2.Relay2Header hdr=(RELAY2.Relay2Header)msg.getHeader(relay.getId());
                    targets.add(hdr.original_sender);
                }
                for(Address target: targets) {
                    if(route.status() != RELAY2.RouteStatus.UP)
                        relay.sendSiteUnreachableTo(target, id);
                }
            }
        }

        protected void changeStatusToUp(final short id, JChannel bridge, Address site_master) {
            final Route route=routes[id];
            if(route.bridge() == null || !route.bridge().equals(bridge))
                route.bridge(bridge);
            if(route.siteMaster() == null || !route.siteMaster().equals(site_master))
                route.siteMaster(site_master);

            RELAY2.RouteStatus old_status=route.status();
            route.status(RELAY2.RouteStatus.UP);

            switch(old_status) {
                case UNKNOWN:
                case DOWN: // queue should be empty, but anyway...
                    cancelTask(id);
                    if(old_status == RELAY2.RouteStatus.UNKNOWN) {
                        relay.getTimer().execute(new Runnable() {
                            public void run() {
                                flushQueue(id, route);
                            }
                        });
                    }
                    break;
            }
        }

        protected void cancelTask(short id) {
            Future<?> task=down_tasks.remove(id);
            if(task != null)
                task.cancel(true);
        }

        // Resends all messages in the queue, then clears the queue
        protected void flushQueue(short id, Route route) {
            BlockingQueue<Message> msgs=fwd_queue.get(id);
            if(msgs == null || msgs.isEmpty())
                return;
            Message msg;
            JChannel bridge=route.bridge();
            if(log.isTraceEnabled())
                log.trace(relay.getLocalAddress() + ": forwarding " + msgs.size() + " queued messages");
            while((msg=msgs.poll()) != null && route.status() == RELAY2.RouteStatus.UP) {
                try {
                    msg.setDest(route.siteMaster()); // the message in the queue is already a copy !
                    bridge.send(msg);
                }
                catch(Throwable ex) {
                    log.error("failed forwarding queued message to " + SiteUUID.getSiteName(id), ex);
                }
            }
            fwd_queue.remove(id);
        }

    }
}
