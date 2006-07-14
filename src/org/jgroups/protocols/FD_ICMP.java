package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

/**
 * Protocol which uses InetAddress.isReachable() to check whether a given host is up or not,
 * taking 1 argument; the host name of the host to be pinged.
 * <em>Note that this protocol only works with JDK 5 !</em>
 * @author Bela Ban
 * @version $Id: FD_ICMP.java,v 1.2 2006/07/14 15:34:09 belaban Exp $
 */
public class FD_ICMP extends FD {

    /** network interface to be used to send the ICMP packets */
    private NetworkInterface intf=null;

    private InetAddress bind_addr;

    private Method is_reacheable;

    /** Time-to-live for InetAddress.isReachable() */
    private int ttl=32;



    public String getName() {
        return "FD_ICMP";
    }


    public boolean setProperties(Properties props) {
        String str, tmp=null;

        // PropertyPermission not granted if running in an untrusted environment with JNLP.
        try {
            tmp=System.getProperty("bind.address");
            if(Util.isBindAddressPropertyIgnored()) {
                tmp=null;
            }
        }
        catch (SecurityException ex){
        }

        if(tmp != null)
            str=tmp;
        else
            str=props.getProperty("bind_addr");
        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("bind_addr");
        }

        str=props.getProperty("ttl");
        if(str != null) {
            ttl=Integer.parseInt(str);
            props.remove("ttl");
        }

        super.setProperties(props);

        try {
            Class is_reacheable_class=Util.loadClass("java.net.InetAddress", this.getClass());
            is_reacheable=is_reacheable_class.getMethod("isReachable", new Class[]{NetworkInterface.class, int.class, int.class});
        }
        catch(ClassNotFoundException e) {
            // log.error("failed checking for InetAddress.isReachable() method - requires JDK 5 or higher");
            Error error=new NoClassDefFoundError("failed checking for InetAddress.isReachable() method - requires JDK 5 or higher");
            error.initCause(e);
            throw error;
        }
        catch(NoSuchMethodException e) {
            // log.error("didn't find InetAddress.isReachable() method - requires JDK 5 or higher");
            Error error= new NoSuchMethodError("didn't find InetAddress.isReachable() method - requires JDK 5 or higher");
            error.initCause(e);
            throw error;
        }


        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    public void init() throws Exception {
        super.init();
        if(bind_addr != null)
            intf=NetworkInterface.getByInetAddress(bind_addr);
    }

    public void up(Event evt) {
        switch(evt.getType()) {
            case Event.CONFIG:
                if(bind_addr == null) {
                    Map config=(Map)evt.getArg();
                    bind_addr=(InetAddress)config.get("bind_addr");
                }
                break;
        }
        super.up(evt);
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
                if(warn)
                    log.warn("ping_dest is null: members=" + members + ", pingable_mbrs=" +
                            pingable_mbrs + ", local_addr=" + local_addr);
                return;
            }

            // 1. execute ping command
            InetAddress host=ping_dest instanceof IpAddress? ((IpAddress)ping_dest).getIpAddress() : null;
            if(host == null)
                throw new IllegalArgumentException("ping_dest is not of type IpAddress - FD_ICMP only works with these");
            try {
                if(trace)
                    log.trace("pinging " + host + " (ping_dest=" + ping_dest + ")");
                start=System.currentTimeMillis();
                Boolean rc=(Boolean)is_reacheable.invoke(host, new Object[]{intf, new Integer(ttl), new Integer((int)timeout)});
                stop=System.currentTimeMillis();
                num_heartbeats++;
                if(rc.booleanValue()) { // success
                    num_tries=0;
                    if(trace)
                        log.trace("successfully received response from " + host + " (after " + (stop-start) + "ms)");
                }
                else { // failure
                    num_tries++;
                    if(log.isDebugEnabled())
                        log.debug("could not ping " + ping_dest + " (tries=" + num_tries + ')');
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
