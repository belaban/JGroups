package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Maintains bridges and routing table. Does the routing of outgoing messages and dispatches incoming messages to
 * the right members.<p/>
 * A Relayer cannot be reused once it is stopped, but a new Relayer instance must be created.
 * @author Bela Ban
 * @since 3.2
 */
public class Relayer2 extends Relayer {
    /** The bridges which are used to connect to different sites */
    protected final Collection<Bridge> bridges=new ConcurrentLinkedQueue<>();


    public Relayer2(RELAY2 relay, Log log) {
        super(relay, log);
    }


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
            log.trace(relay.getAddress() + ": will not start the Relayer as stop() has been called");
            return;
        }
        try {
            for(RelayConfig.BridgeConfig bridge_config: bridge_configs) {
                Bridge bridge=new Bridge(bridge_config.createChannel(), bridge_config.getClusterName(), bridge_name,
                                         new AddressGenerator() {
                                             @Override public Address generateAddress() {
                                                 return new SiteUUID(UUID.randomUUID(), bridge_name, my_site_id);
                                             }

                                             @Override public Address generateAddress(String name) {
                                                 return new SiteUUID(UUID.randomUUID(), name, my_site_id);
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
                log.trace(relay.getAddress() + ": stop() was called while starting the relayer; stopping the relayer now");
                stop();
            }
        }
    }



    /**
     * Disconnects and destroys all bridges
     */
    public void stop() {
        done=true;
        bridges.forEach(Bridge::stop);
        bridges.clear();
    }


    protected View getBridgeView(String cluster_name) {
        if(cluster_name == null || bridges == null)
            return null;
        for(Bridge bridge: bridges) {
            if(Objects.equals(bridge.cluster_name, cluster_name))
                return bridge.view;
        }
        return null;
    }



    protected class Bridge implements Receiver {
        protected JChannel     channel;
        protected final String cluster_name;
        protected View         view;

        protected Bridge(final JChannel ch, final String cluster, String name, AddressGenerator gen) throws Exception {
            this.channel=ch;
            this.channel.setName(name);
            this.channel.setReceiver(this);
            this.channel.addAddressGenerator(gen);
            this.cluster_name=cluster;
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
            RelayHeader hdr=msg.getHeader(relay.getId());
            if(hdr == null) {
                log.warn("received a message without a relay header; discarding it");
                return;
            }
            switch(hdr.type) {
                case RelayHeader.TOPO_REQ:
                    RelayHeader rsp_hdr=new RelayHeader(RelayHeader.TOPO_RSP)
                      .addToSites(((RELAY2)relay).printLocalTopology());
                    Message topo_rsp=new EmptyMessage(msg.src()).putHeader(relay.getId(), rsp_hdr);
                    try {
                        channel.send(topo_rsp);
                    }
                    catch(Exception e) {
                        log.warn("%s: failed sending TOPO-RSP message to %s: %s", channel.getAddress(), msg.src(), e);
                    }
                    return; // not relayed
                case RelayHeader.TOPO_RSP:
                    Set<String> sites=hdr.getSites();
                    if(sites != null && !sites.isEmpty())
                        ((RELAY2)relay).topo_collector.add(msg.src(), sites.iterator().next());
                    return;
            }
            relay.handleRelayMessage(msg);
        }

        /** The view contains a list of SiteUUIDs. Adjust the routing table based on the SiteUUIDs UUID and site */
        public void viewAccepted(View new_view) {
            this.view=new_view;
            log.trace("[Relayer " + channel.getAddress() + "] view: " + new_view);

            Map<String,List<Address>> tmp=extract(new_view);
            Set<String>               down=new HashSet<>(routes.keySet());
            Set<String>               up=new HashSet<>();

            down.removeAll(tmp.keySet());

            routes.keySet().retainAll(tmp.keySet()); // remove all sites which are not in the view

            for(Map.Entry<String,List<Address>> entry: tmp.entrySet()) {
                String key=entry.getKey();
                List<Address> val=entry.getValue();

                List<Route> newRoutes;
                if(routes.containsKey(key))
                    newRoutes=new ArrayList<>(routes.get(key));
                else {
                    newRoutes=new ArrayList<>();
                    if(up != null)
                        up.add(key);
                }

                // Remove routes not in the view anymore:
                newRoutes.removeIf(route -> !val.contains(route.siteMaster()));

                // Add routes that aren't yet in the routing table:
                val.stream().filter(addr -> !contains(newRoutes, addr))
                  .forEach(addr -> newRoutes.add(new Route(addr, channel, relay, log).stats(stats)));

                if(newRoutes.isEmpty()) {
                    routes.remove(key);
                    down.add(key);
                    up.remove(key);
                }
                else
                    routes.put(key, Collections.unmodifiableList(newRoutes));
            }
            if(!down.isEmpty())
                relay.sitesChange(true, down);
            if(!up.isEmpty())
                relay.sitesChange(false, up);
        }

        protected boolean contains(List<Route> routes, Address addr) {
            return routes.stream().anyMatch(route -> route.siteMaster().equals(addr));
        }

        /** Returns a map containing the site keys and addresses as values */
        protected Map<String,List<Address>> extract(View view) {
            Map<String,List<Address>> map=new HashMap<>(view.size());
            for(Address mbr: view) {
                SiteAddress member=(SiteAddress)mbr;
                String key=member.getSite();
                List<Address> list=map.computeIfAbsent(key, k -> new ArrayList<>());
                if(!list.contains(member))
                    list.add(member);
            }
            return map;
        }
    }
}
