package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.UUID;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Maintains bridges and routing table. Does the routing of outgoing messages and dispatches incoming messages to
 * the right members.<p>
 * A Relayer cannot be reused once it is stopped, but a new Relayer instance must be created.
 * @author Bela Ban
 * @since 3.2
 */
public class Relayer3 extends Relayer {
    /** The bridges which are used to connect to different sites */
    protected final Collection<Bridge> bridges=new ConcurrentLinkedQueue<>();


    public Relayer3(RELAY relay, Log log) {
        super(relay, log);
    }


    /**
     * Creates all bridges from site_config and connects them (joining the bridge clusters)
     * @param site_cfg The SiteConfiguration
     * @param bridge_name The name of the local bridge channel, prefixed with '_'.
     * @param my_site_id The ID of this site
     */
    public CompletableFuture<Relayer> start(RelayConfig.SiteConfig site_cfg, String bridge_name, final String my_site_id) throws Throwable {
        if(done) {
            log.trace(relay.getAddress() + ": will not start the Relayer as stop() has been called");
            return CompletableFuture.completedFuture(this);
        }
        try {
            // Add configured forward routes:
            List<RelayConfig.ForwardConfig> forward_configs=site_cfg.getForwards();
            for(RelayConfig.ForwardConfig cfg: forward_configs) {
                ForwardingRoute fr=new ForwardingRoute(cfg.to(), cfg.gateway());
                this.forward_routes.add(fr);
            }

            // Add configured bridges
            List<RelayConfig.BridgeConfig> bridge_configs=site_cfg.getBridges();
            for(RelayConfig.BridgeConfig cfg: bridge_configs) {
                Bridge bridge=new Bridge(cfg.createChannel(), this, cfg.getClusterName(), bridge_name,
                                         new AddressGenerator() {
                                             @Override
                                             public Address generateAddress() {return generateAddress(null);}
                                             @Override
                                             public Address generateAddress(String name) {
                                                 return new SiteUUID(UUID.randomUUID(), name, my_site_id);
                                             }
                                         });
                bridges.add(bridge);
            }
            for(Bridge bridge: bridges)
                bridge.start();
            return CompletableFuture.completedFuture(this);
        }
        catch(Throwable t) {
            stop();
            return CompletableFuture.failedFuture(t);
            // throw t;
        }
        finally {
            if(done)
                stop();
        }
    }

    /** Disconnects and destroys all bridges */
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



}
