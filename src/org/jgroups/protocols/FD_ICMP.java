package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Map;

/**
 * Protocol which uses InetAddress.isReachable() to check whether a given host
 * is up or not, taking 1 argument; the host name of the host to be pinged.
 * <em>Note that this protocol only works with a JDK version >= 5 !</em> The implementation
 * of this may or may not use ICMP ! An alternative is to create a TCP
 * connection to port 7 (echo service) and see whether it works ! This is
 * done in JDK 5, so unless an echo service is configured to run, this
 * won't work...
 * 
 * @author Bela Ban
 */
@Experimental
public class FD_ICMP extends FD {

    /** network interface to be used to send the ICMP packets */
    private NetworkInterface intf=null;

    @LocalAddress
    @Property(name="bind_addr",
              description="The NIC on which the ServerSocket should listen on. " +
                      "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK", 
              systemProperty={Global.BIND_ADDR})
    private InetAddress bind_addr=null ;
    
    @Property(name="bind_interface", converter=PropertyConverters.BindInterface.class,
    		description="The interface (NIC) which should be used by this transport", dependsUpon="bind_addr")
    protected String bind_interface_str=null;
     
    /** Time-to-live for InetAddress.isReachable() */
    @Property
    protected int ttl=32;


    public void init() throws Exception {
        super.init();
        if(bind_addr != null)
            intf=NetworkInterface.getByInetAddress(bind_addr);
    }

    @SuppressWarnings("unchecked")
    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.CONFIG:
                if(bind_addr == null) {
                    Map<String,Object> config=(Map<String,Object>)evt.getArg();
                    bind_addr=(InetAddress)config.get("bind_addr");
                }
                break;
        }
        return super.up(evt);
    }


    protected Monitor createMonitor() {
        return new FD_ICMP.PingMonitor();
    }


    /**
     * Runs InetAddress.isReachable(). Each time the command fails, we increment num_tries. If num_tries > max_tries, we
     * emit a SUSPECT message. If ping_dest changes, or we do receive traffic from ping_dest, we reset num_tries to 0.
     */
    protected class PingMonitor extends Monitor {
        long start, stop;

        public void run() {
            if(ping_dest == null) {
                if(log.isWarnEnabled())
                    log.warn("ping_dest is null: members=" + members + ", pingable_mbrs=" +
                               pingable_mbrs + ", local_addr=" + local_addr);
                return;
            }

            // 1. execute ping command
            InetAddress host=null;
            PhysicalAddress physical_addr;
            if(ping_dest instanceof PhysicalAddress) {
                physical_addr=(PhysicalAddress)ping_dest;
            }
            else {
                physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, ping_dest));
            }

            if(physical_addr == null)
                log.warn("failed fetching physical address for " + ping_dest);
            else
                host=((IpAddress)physical_addr).getIpAddress();

            if(host == null)
                throw new IllegalArgumentException("ping_dest is not of type IpAddress - FD_ICMP only works with these");
            try {
                if(log.isTraceEnabled())
                    log.trace("pinging " + host + " (ping_dest=" + ping_dest + ") using interface " + intf);
                start=System.currentTimeMillis();
                boolean rc=host.isReachable(intf, ttl,(int)timeout);
                stop=System.currentTimeMillis();
                num_heartbeats++;
                if(rc) {
                    num_tries=0;
                    if(log.isTraceEnabled())
                        log.trace("successfully received response from " + host + " (after " + (stop-start) + "ms)");
                }
                else {
                    num_tries++;
                    if(log.isDebugEnabled())
                        log.debug("could not ping " + ping_dest + " (tries=" + num_tries + ") after " + (stop-start) + "ms)");
                }

                if(num_tries >= max_tries) {
                    if(log.isDebugEnabled())
                        log.debug("[" + local_addr + "]: could not ping " + ping_dest + " for " + (num_tries +1) +
                                " times (" + ((num_tries+1) * timeout) + " milliseconds), suspecting it");
                    // broadcast a SUSPECT message to all members - loop until
                    // unsuspect or view change is received
                    bcast_task.addSuspectedMember(ping_dest);
                    num_tries=0;
                    if(stats) {
                        num_suspect_events++;
                        suspect_history.add(ping_dest);
                    }
                }
            }
            catch(Exception ex) {
                if(log.isErrorEnabled())
                    log.error("failed pinging " + ping_dest, ex);
            }
        }
    }
}
