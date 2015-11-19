package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Maintains bridges and routing table. Does the routing of outgoing messages and dispatches incoming messages to
 * the right members.<p/>
 * A Relayer cannot be reused once it is stopped, but a new Relayer instance must be created.
 * @author Bela Ban
 * @since 3.2
 */
public class Relayer {
    /** The routing table. Site IDs are the keys (e.g. "sfo", and list of routes are the values */
    protected ConcurrentMap<String,List<Route>> routes=new ConcurrentHashMap<>(5);

    /** The bridges which are used to connect to different sites */
    protected final Queue<Bridge>               bridges=new ConcurrentLinkedQueue<>();

    protected final Log                         log;

    protected final RELAY2                      relay;

    /** Flag set when stop() is called. Since a Relayer should not be used after stop() has been called, a new
     * instance needs to be created */
    protected volatile boolean                  done;

    protected boolean                           stats;

    // Used to store messages for a site with status UNKNOWN. Messages will be flushed when the status changes to UP, or
    // a SITE-UNREACHABLE message will be sent to each member *once* when the status changes to DOWN
    // protected final ConcurrentMap<String,BlockingQueue<Message>> fwd_queue=new ConcurrentHashMap<String,BlockingQueue<Message>>();

//    /** Map to store tasks which set the status of a site from UNKNOWN to DOWN. These are started when a site is
//     * set to UNKNOWN, but they need to be cancelled when the status goes from UNKNOWN back to UP <em>before</em>
//     * they kick in.*/
//    protected final ConcurrentMap<Short,Future<?>>   down_tasks=new ConcurrentHashMap<Short,Future<?>>();
//


    public Relayer(RELAY2 relay, Log log) {
        this.relay=relay;
        this.stats=relay.statsEnabled();
        this.log=log;
    }


    public boolean done() {return done;}

    
    /**
     * Creates all bridges from site_config and connects them (joining the bridge clusters)
     * @param bridge_configs A list of bridge configurations
     * @param bridge_name The name of the local bridge channel, prefixed with '_'.
     * @param my_site_id The ID of this site
     * @throws Throwable
     */
    public void start(List<RelayConfig.BridgeConfig> bridge_configs, String bridge_name, final String my_site_id)
      throws Throwable {
        if(done) {
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
                log.trace(relay.getLocalAddress() + ": stop() was called while starting the relayer; stopping the relayer now");
                stop();
            }
        }
    }



    /**
     * Disconnects and destroys all bridges
     */
    public void stop() {
        done=true;
        for(Bridge bridge: bridges)
            bridge.stop();
        bridges.clear();
    }


    public synchronized String printRoutes() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,List<Route>> entry: routes.entrySet()) {
            List<Route> list=entry.getValue();
            if(list != null && !list.isEmpty())
                sb.append(entry.getKey() + " --> ").append(Util.print(list)).append("\n");
        }
        return sb.toString();
    }



    /**
     * Grabs a random route
     * @param site
     * @return
     */
    protected synchronized Route getRoute(String site) {
        List<Route> list=routes.get(site);
        return list == null? null : Util.pickRandomElement(list);
    }

    protected List<String> getSiteNames() {
        return new ArrayList<>(routes.keySet());
    }

    protected synchronized List<Route> getRoutes(String ... excluded_sites) {
        List<Route> retval=new ArrayList<>(routes.size());
        for(List<Route> list: routes.values()) {
            for(Route route: list) {
                if(route != null) {
                    if(!isExcluded(route, excluded_sites)) {
                        retval.add(route);
                        break;
                    }
                }
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

    protected static boolean isExcluded(Route route, String... excluded_sites) {
        if(excluded_sites == null)
            return false;
        String site=((SiteUUID)route.site_master).getSite();
        for(String excluded_site: excluded_sites)
            if(site.equals(excluded_site))
                return true;
        return false;
    }




    /**
     * Includes information about the site master of the route and the channel to be used
     */
    public class Route implements Comparable<Route> {
        /** SiteUUID: address of the site master */
        protected final Address   site_master;
        protected final JChannel  bridge;

        public Route(Address site_master, JChannel bridge) {
            this.site_master=site_master;
            this.bridge=bridge;
        }

        public JChannel           bridge()                               {return bridge;}
        public Address            siteMaster()                           {return site_master;}

        public void send(Address final_destination, Address original_sender, final Message msg) {
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
                log.error(Util.getMessage("FailureRelayingMessage"), e);
            }
        }

        public int compareTo(Route o) {
            return site_master.compareTo(o.siteMaster());
        }

        public boolean equals(Object obj) {
            return compareTo((Route)obj) == 0;
        }

        public int hashCode() {
            return site_master.hashCode();
        }

        public String toString() {
            return (site_master != null? site_master.toString() : "");
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
            channel.addAddressGenerator(addr_generator);
            this.cluster_name=cluster_name;
        }

        protected void start() throws Exception {
            channel.connect(cluster_name);
            log.info("%s: joined bridge cluster '%s'", channel.getAddress(), cluster_name);
        }

        protected void stop() {
            log.info("%s: leaving bridge cluster '%s'", channel.getAddress(), channel.getClusterName());
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

        /** The view contains a list of SiteUUIDs. Adjust the routing table based on the SiteUUIDs UUID and site
         */
        public void viewAccepted(View new_view) {
            this.view=new_view;
            log.trace("[Relayer " + channel.getAddress() + "] view: " + new_view);

            RouteStatusListener       listener=relay.getRouteStatusListener();
            Map<String,List<Address>> tmp=extract(new_view);
            Set<String>               down=listener != null? new HashSet<>(routes.keySet()) : null;
            Set<String>               up=listener != null? new HashSet<String>() : null;

            if(listener != null)
                down.removeAll(tmp.keySet());

            routes.keySet().retainAll(tmp.keySet()); // remove all sites which are not in the view

            for(Map.Entry<String,List<Address>> entry: tmp.entrySet()) {
                String key=entry.getKey();
                List<Address> val=entry.getValue();
                if(!routes.containsKey(key)) {
                    routes.put(key, new ArrayList<Route>());
                    if(up != null)
                        up.add(key);
                }

                List<Route> list=routes.get(key);

                // Remove routes not in the view anymore:
                for(Iterator<Route> it=list.iterator(); it.hasNext();) {
                    Route route=it.next();
                    if(!val.contains(route.siteMaster()))
                        it.remove();
                }

                // Add routes that aren't yet in the routing table:
                for(Address addr: val) {
                    if(!contains(list, addr))
                        list.add(new Route(addr, channel));
                }

                if(list.isEmpty()) {
                    routes.remove(key);
                    if(listener != null) {
                        down.add(key);
                        up.remove(key);
                    }
                }
            }

            if(listener != null) {
                if(!down.isEmpty())
                    listener.sitesDown(down.toArray(new String[down.size()]));
                if(!up.isEmpty())
                    listener.sitesUp(up.toArray(new String[up.size()]));
            }
        }

        protected boolean contains(List<Route> routes, Address addr) {
            for(Route route: routes) {
                if(route.siteMaster().equals(addr))
                    return true;
            }
            return false;
        }

        /** Returns a map containing the site keys and addresses as values */
        protected Map<String,List<Address>> extract(View view) {
            Map<String,List<Address>> map=new HashMap<>(view.size());
            for(Address mbr: view) {
                SiteAddress member=(SiteAddress)mbr;
                String key=member.getSite();
                List<Address> list=map.get(key);
                if(list == null) {
                    list=new ArrayList<>();
                    map.put(key, list);
                }
                if(!list.contains(member))
                    list.add(member);
            }
            return map;
        }
    }
}
