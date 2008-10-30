package org.jgroups.stack;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.util.*;


/**
 * Local stub for clients to access one (or more) GossipRouters. Will use proprietary protocol
 * (using GossipData PDUs) based on TCP to connect to GossipRouter.<p>
 * Requires JDK >= 1.3 due to the use of Timer.
 * 
 * @author Bela Ban Oct 4 2001
 * @version $Id: GossipClient.java,v 1.18.2.3 2008/10/30 14:01:15 belaban Exp $
 */
public class GossipClient {
    Timer timer=new Timer(true);

    /** Hashtable<String,List<Address>> */
    final Map<String,List<Address>> groups=new Hashtable<String,List<Address>>();               // groups - List of Addresses
    private Refresher refresher_task=new Refresher();
    final Vector<Address> gossip_servers=new Vector<Address>();          // a list of GossipRouters (IpAddress)
    boolean timer_running=false;
    boolean refresher_enabled=true;
    long refresh_interval=20000;     // interval for re-registering; must be less than in GossipRouter
    int sock_conn_timeout=2000;      // max number of ms to wait for socket establishment to GossipRouter
    int sock_read_timeout=0;         // max number of ms to wait for socket reads (0 means block forever, or until the sock is closed)

    protected final Log log=LogFactory.getLog(this.getClass());


    /**
     * Creates the GossipClient
     * @param gossip_host The address and port of the host on which the GossipRouter is running
     * @param expiry Interval (in msecs) for the refresher task
     */
    public GossipClient(IpAddress gossip_host, long expiry) {
        init(gossip_host, expiry);
    }


    /**
     Creates the GossipClient
     @param gossip_hosts List of IpAddresses
     @param expiry Interval (in msecs) for the refresher task
     */
    public GossipClient(Vector<IpAddress> gossip_hosts, long expiry) {
        if(gossip_hosts == null) {
            if(log.isErrorEnabled()) log.error("empty set of GossipRouters given");
            return;
        }
        for(IpAddress host: gossip_hosts)
            init(host, expiry);
    }


    public GossipClient(Vector<IpAddress> gossip_hosts, long expiry, int sock_conn_timeout) {
        this(gossip_hosts, expiry);
        this.sock_conn_timeout=sock_conn_timeout;
    }

    public boolean isRefresherEnabled() {
        return refresher_enabled;
    }

    public void setRefresherEnabled(boolean refresher_enabled) {
        this.refresher_enabled=refresher_enabled;
    }


    public int getSockConnectionTimeout() {
        return sock_conn_timeout;
    }

    public void setSocketConnectionTimeout(int sock_conn_timeout) {
        this.sock_conn_timeout=sock_conn_timeout;
    }

    public int getSocketReadTimeout() {
        return sock_read_timeout;
    }

    public void setSocketReadTimeout(int sock_read_timeout) {
        this.sock_read_timeout=sock_read_timeout;
    }

    public long getRefreshInterval() {
        return refresh_interval;
    }

    public void setRefreshInterval(long refresh_interval) {
        this.refresh_interval=refresh_interval;
    }

    public void stop() {
        timer_running=false;
        if(refresher_task != null)
            refresher_task.cancel();
        timer.cancel();
        groups.clear();
        // provide another refresh tools in case the channel gets reconnected
        // timer=new Timer();
        // refresher_task=new Refresher();
    }


    public void destroy() {
        timer_running=false;
        timer.cancel();
        groups.clear();
    }


    /**
     * Adds a GossipRouter to be accessed.
     */
    public void addGossipRouter(IpAddress gossip_host) {
        if(!gossip_servers.contains(gossip_host))
            gossip_servers.addElement(gossip_host);
    }


    /**
     Adds the member to the given group. If the group already has an entry for the member,
     its timestamp will be updated, preventing the cache cleaner from removing the entry.<p>
     The entry will be registered <em>with all GossipRouters that GossipClient is configured to access</em>
     */
    public void register(String group, Address mbr) {
        if(group == null || mbr == null) {
            if(log.isErrorEnabled()) log.error("group or mbr is null");
            return;
        }

        List<Address> mbrs=groups.get(group);
        if(mbrs == null) {
            mbrs=new LinkedList<Address>();
            mbrs.add(mbr);
            groups.put(group, mbrs);
        }
        else {
            if(!mbrs.contains(mbr))
                mbrs.add(mbr);
        }

        _register(group, mbr); // update entry in GossipRouter

        if(refresher_enabled) {
            if(!timer_running) {
                timer=new Timer(true);
                refresher_task=new Refresher();
                timer.schedule(refresher_task, refresh_interval, refresh_interval);
                timer_running=true;
            }
        }
    }


    public void unregister(String group, Address mbr) {
        if(group == null || mbr == null) {
            if(log.isErrorEnabled()) log.error("group or mbr is null");
            return;
        }

        _unregister(group, mbr); // remove entry from GossipRouter
    }


    /**
     Returns all members of a given group
     @param group The group name
     @return List A list of Addresses
     */
    public List<Address> getMembers(String group) {
        if(group == null) {
            if(log.isErrorEnabled()) log.error("group is null");
            return null;
        }
        List<Address> result=_getMembers(group);
        if(log.isTraceEnabled())
            log.trace("GET(" + group + ") --> " + result);
        return result;
    }



    /* ------------------------------------- Private methods ----------------------------------- */


    final void init(IpAddress gossip_host, long refresh_interval) {
        this.refresh_interval=refresh_interval;
        addGossipRouter(gossip_host);
    }


    /**
     * Registers the group|mbr with *all* GossipRouters.
     */
    void _register(String group, Address mbr) {
        Socket sock=null;
        DataOutputStream out=null;
        IpAddress entry;
        GossipData gossip_req;

        for(int i=0; i < gossip_servers.size(); i++) {
            entry=(IpAddress) gossip_servers.elementAt(i);
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                if(log.isErrorEnabled()) log.error("entry.host or entry.port is null");
                continue;
            }
            try {
                if(log.isTraceEnabled())
                    log.trace("REGISTER(" + group + ", " + mbr + ") with GossipRouter at " + entry.getIpAddress() + ':' + entry.getPort());
                sock=new Socket();
                if(sock_read_timeout > 0)
                    sock.setSoTimeout(sock_read_timeout);
                sock.connect(new InetSocketAddress(entry.getIpAddress(), entry.getPort()), sock_conn_timeout);
                out=new DataOutputStream(sock.getOutputStream());
                gossip_req=new GossipData(GossipRouter.REGISTER, group, mbr, null);
                // must send GossipData as fast as possible, otherwise the request might be rejected
                gossip_req.writeTo(out);
                out.flush();
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception connecting to host " + entry);
            }
            finally {
                Util.close(out);
                Util.close(sock);
            }
        }
    }


    void _unregister(String group, Address mbr) {
        Socket sock=null;
        DataOutputStream out=null;
        IpAddress entry;
        GossipData gossip_req;

        for(int i=0; i < gossip_servers.size(); i++) {
            entry=(IpAddress) gossip_servers.elementAt(i);
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                if(log.isErrorEnabled()) log.error("entry.host or entry.port is null");
                continue;
            }
            try {
                if(log.isTraceEnabled())
                    log.trace("UNREGISTER(" + group + ", " + mbr + ") with GossipRouter at " + entry.getIpAddress() + ':' + entry.getPort());
                sock=new Socket();
                if(sock_read_timeout > 0)
                    sock.setSoTimeout(sock_read_timeout);
                sock.connect(new InetSocketAddress(entry.getIpAddress(), entry.getPort()), sock_conn_timeout);
                out=new DataOutputStream(sock.getOutputStream());
                gossip_req=new GossipData(GossipRouter.UNREGISTER, group, mbr, null);
                // must send GossipData as fast as possible, otherwise the
                // request might be rejected
                gossip_req.writeTo(out);
                out.flush();
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception connecting to host " + entry);
            }
            finally {
                Util.close(out);
                if(sock != null) {
                    try {sock.close();} catch(IOException e) {}
                }
            }
        }
    }


    /**
     * Sends a GET_MBR_REQ to *all* GossipRouters, merges responses.
     */
    private List<Address> _getMembers(String group) {
        List<Address> ret=new LinkedList<Address>();
        Socket sock=null;
        SocketAddress destAddr;
        DataOutputStream out=null;
        DataInputStream in=null;
        IpAddress entry;
        GossipData gossip_req, gossip_rsp;
        Address mbr;

        for(int i=0; i < gossip_servers.size(); i++) {
            entry=(IpAddress) gossip_servers.elementAt(i);
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                if(log.isErrorEnabled()) log.error("entry.host or entry.port is null");
                continue;
            }
            
            try {
                sock=new Socket();
                if(sock_read_timeout > 0)
                    sock.setSoTimeout(sock_read_timeout);
                destAddr=new InetSocketAddress(entry.getIpAddress(), entry.getPort());
                sock.connect(destAddr, sock_conn_timeout);
                out=new DataOutputStream(sock.getOutputStream());

                gossip_req=new GossipData(GossipRouter.GOSSIP_GET, group, null, null);
                // must send GossipData as fast as possible, otherwise the
                // request might be rejected
                gossip_req.writeTo(out);
                out.flush();

                in=new DataInputStream(sock.getInputStream());
                gossip_rsp=new GossipData();
                gossip_rsp.readFrom(in);
                if(gossip_rsp.mbrs != null) { // merge with ret
                    for(Iterator it=gossip_rsp.mbrs.iterator(); it.hasNext();) {
                        mbr=(Address)it.next();
                        if(!ret.contains(mbr))
                            ret.add(mbr);
                    }
                }
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception connecting to host " + entry);
            }
            finally {
                Util.close(out);
                Util.close(in);
                Util.close(sock);
            }
        }

        return ret;
    }



    /* ---------------------------------- End of Private methods ------------------------------- */



    /**
     * Periodically iterates through groups and refreshes all registrations with GossipRouter
     */
    private class Refresher extends TimerTask {

        public void run() {
            int num_items=0;
            String group;
            List<Address> mbrs;

            if(log.isTraceEnabled()) log.trace("refresher task is run");
            for(Map.Entry<String,List<Address>> entry: groups.entrySet()) {
                group=entry.getKey();
                mbrs=entry.getValue();
                if(mbrs != null) {
                    for(Address mbr: mbrs) {
                        if(log.isTraceEnabled()) log.trace("registering " + group + " : " + mbr);
                        register(group, mbr);
                        num_items++;
                    }
                }
            }
            if(log.isTraceEnabled()) log.trace("refresher task done. Registered " + num_items + " items");
        }

    }


    public static void main(String[] args) {
        Vector<IpAddress> gossip_hosts=new Vector<IpAddress>();
        String host;
        InetAddress ip_addr;
        int port;
        boolean get=false, register=false, keep_running=false;
        String register_host=null;
        int register_port=0;
        String get_group=null, register_group=null;
        GossipClient gossip_client=null;
        List mbrs;
        long expiry=20000;
        int sock_conn_timeout=2000;
        int sock_read_timeout=3000;


        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                usage();
                return;
            }
            if("-expiry".equals(args[i])) {
                expiry=Long.parseLong(args[++i]);
                continue;
            }
            if("-sock_read_timeout".equals(args[i])) {
                sock_read_timeout=Integer.parseInt(args[++i]);
                continue;
            }
            if("-sock_conn_timeout".equals(args[i])) {
                sock_conn_timeout=Integer.parseInt(args[++i]);
                continue;
            }
            if("-host".equals(args[i])) {
                host=args[++i];
                port=Integer.parseInt(args[++i]);
                try {
                    ip_addr=InetAddress.getByName(host);
                    gossip_hosts.add(new IpAddress(ip_addr, port));
                }
                catch(Exception ex) {
                    System.err.println(ex);
                }
                continue;
            }
            if("-keep_running".equals(args[i])) {
                keep_running=true;
                continue;
            }
            if("-get".equals(args[i])) {
                get=true;
                get_group=args[++i];
                continue;
            }
            if("-register".equals(args[i])) {
                register_group=args[++i];
                register_host=args[++i];
                register_port=Integer.parseInt(args[++i]);
                register=true;
                continue;
            }
            usage();
            return;
        }

        if(gossip_hosts.isEmpty()) {
            System.err.println("At least 1 GossipRouter has to be given");
            return;
        }

        if(!register && !get) {
            System.err.println("Neither get nor register command given, will not do anything");
            return;
        }

        try {
            gossip_client=new GossipClient(gossip_hosts, expiry);
            gossip_client.setSocketConnectionTimeout(sock_conn_timeout);
            gossip_client.setSocketReadTimeout(sock_read_timeout);
            if(register) {
                System.out.println("Registering " + register_group + " --> " + register_host + ':' + register_port);
                gossip_client.register(register_group, new IpAddress(register_host, register_port));
            }

            if(get) {
                System.out.println("Getting members for group " + get_group);
                mbrs=gossip_client.getMembers(get_group);
                System.out.println("Members for group " + get_group + " are " + mbrs);
            }
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
        if(!keep_running)
            gossip_client.stop();
    }


    static void usage() {
        System.out.println("GossipClient [-help] [-host <hostname> <port>]+ " +
                "[-sock_conn_timeout <timeout>] [-sock_read_timeout <timeout>] " +
                " [-get <groupname>] [-register <groupname hostname port>] [-expiry <msecs>] " +
                "[-keep_running]]");
    }

}
