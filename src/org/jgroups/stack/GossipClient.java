package org.jgroups.stack;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Local stub for clients to access one (or more) GossipRouters. Will use proprietary protocol
 * (using GossipData PDUs) based on TCP to connect to GossipRouter.<p>
 * 
 * @author Bela Ban Oct 4 2001
 * @version $Id: GossipClient.java,v 1.27 2008/12/10 15:15:56 vlada Exp $
 */
public class GossipClient {
    TimeScheduler timer=null;

    /** Hashtable<String,List<Address>> */
    final Map<String,List<Address>> groups=new Hashtable<String,List<Address>>();               // groups - List of Addresses
    private Future<?> refresher_task=null;
    final Vector<IpAddress> gossip_servers=new Vector<IpAddress>();          // a list of GossipRouters (IpAddress)
    boolean refresher_enabled=true;
    long refresh_interval=20000;          // must be less than in GossipRouter
    int sock_conn_timeout=2000;      // max number of ms to wait for socket establishment to GossipRouter
    int sock_read_timeout=0;         // max number of ms to wait for socket reads (0 means block forever, or until the sock is closed)

    protected final Log log=LogFactory.getLog(this.getClass());


    public GossipClient(IpAddress gossip_host, long expiry, int sock_conn_timeout, TimeScheduler timer) {
        this(new Vector<IpAddress>(Arrays.asList(gossip_host)), expiry, sock_conn_timeout, timer);
    }

    /**
     Creates the GossipClient
     @param gossip_hosts List of IpAddresses
     @param expiry Interval (in msecs) for the refresher task
     */
    public GossipClient(List<IpAddress> gossip_hosts, long expiry, int sock_conn_timeout, TimeScheduler timer) {
        this.sock_conn_timeout=sock_conn_timeout;
        setTimer(timer);
        if(gossip_hosts == null) {
            if(log.isErrorEnabled()) log.error("empty set of GossipRouters given");
            return;
        }
        for(IpAddress host: gossip_hosts)
            init(host, expiry);
    }


    public boolean isRefresherEnabled() {
        return refresher_enabled;
    }

    public void setRefresherEnabled(boolean refresher_enabled) {
        this.refresher_enabled=refresher_enabled;
    }

    public int getSocketConnectionTimeout() {
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

    public void setTimer(TimeScheduler timer) {
        if(this.timer != null) {
            try {
                this.timer.stop();
            }
            catch(InterruptedException e) {
            }
        }
        this.timer=timer;
        if(this.timer == null)
            this.timer=new TimeScheduler();
    }

    public void stop() {
        synchronized(this) {
            if(refresher_task != null) {
                refresher_task.cancel(true);
                refresher_task=null;
            }
        }
        groups.clear();
    }

    public void stopTimer() {
        if(timer != null) {
            try {timer.stop();} catch(InterruptedException e) {}
        }
    }


    public void destroy() {
        stop(); // needed ?
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
    public void register(String group, Address mbr, boolean synchronous) {
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

        _register(group, mbr, synchronous); // update entry in GossipRouter

        if(refresher_enabled) {
            synchronized(this) {
                if(refresher_task == null || refresher_task.isDone()) {
                    Refresher tmp=new Refresher();
                    refresher_task=timer.scheduleWithFixedDelay(tmp, refresh_interval, refresh_interval, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    public void register(String group, Address mbr) {
        register(group, mbr, false);
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
    public List<Address> getMembers(String group, long timeout) {
        if(group == null) {
            if(log.isErrorEnabled()) log.error("group is null");
            return null;
        }
        List<Address> result=_getMembers(group, timeout);
        if(log.isTraceEnabled())
            log.trace("GET(" + group + ") --> " + result);
        return result;
    }

    public List<Address> getMembers(String group) {
        return getMembers(group, 0);
    }



    /* ------------------------------------- Private methods ----------------------------------- */


    final void init(IpAddress gossip_host, long expiry) {
        refresh_interval=expiry;
        addGossipRouter(gossip_host);
    }


    /**
     * Registers the group|mbr with *all* GossipRouters.
     */
    void _register(final String group, final Address mbr, boolean synchronous) {
        List<Future<?>> futures=null;
        if(synchronous)
            futures=new ArrayList<Future<?>>();

        for(final IpAddress entry: gossip_servers) {
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                if(log.isErrorEnabled()) log.error("entry.host or entry.port is null");
                continue;
            }
            Future<?> future=timer.submit(new Runnable() {
                public void run() {
                    Socket sock=null;
                    DataOutputStream out=null;
                    try {
                        if(log.isTraceEnabled())
                            log.trace("REGISTER(" + group + ", " + mbr + ") with GossipRouter at " + entry.getIpAddress() + ':' + entry.getPort());
                        sock=new Socket();
                        if(sock_read_timeout > 0)
                            sock.setSoTimeout(sock_read_timeout);
                        sock.connect(new InetSocketAddress(entry.getIpAddress(), entry.getPort()), sock_conn_timeout);
                        out=new DataOutputStream(sock.getOutputStream());
                        GossipData gossip_req=new GossipData(GossipRouter.REGISTER, group, mbr, null);
                        // must send GossipData as fast as possible, otherwise the request might be rejected
                        gossip_req.writeTo(out);
                        out.flush();
                    }
                    catch(Exception ex) {
                        if(log.isErrorEnabled())
                            log.error("register(" + group + ", " + mbr + "): exception connecting to host " + entry);
                    }
                    finally {
                        Util.close(out);
                        if(sock != null) {
                            try {
                                sock.close();
                            }
                            catch(IOException e) {
                            }
                        }
                    }
                }
            });

            if(futures != null)
                futures.add(future);
        }
        if(futures != null) {
            for(Future<?> f: futures) {
                try {
                    f.get();
                }
                catch(Throwable t) {
                }
            }
        }
    }


    void _unregister(final String group, final Address mbr) {
        for(final IpAddress entry: gossip_servers) {
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                if(log.isErrorEnabled()) log.error("entry.host or entry.port is null");
                continue;
            }
            timer.submit(new Runnable() {
                public void run() {
                    Socket sock=null;
                    DataOutputStream out=null;
                    try {
                        if(log.isTraceEnabled())
                            log.trace("UNREGISTER(" + group + ", " + mbr + ") with GossipRouter at " + entry.getIpAddress() + ':' + entry.getPort());
                        sock=new Socket();
                        if(sock_read_timeout > 0)
                            sock.setSoTimeout(sock_read_timeout);
                        sock.connect(new InetSocketAddress(entry.getIpAddress(), entry.getPort()), sock_conn_timeout);
                        out=new DataOutputStream(sock.getOutputStream());
                        GossipData gossip_req=new GossipData(GossipRouter.UNREGISTER, group, mbr, null);
                        // must send GossipData as fast as possible, otherwise the request might be rejected
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
            });
        }
    }


    /**
     * Sends a GET_MBR_REQ to *all* GossipRouters, merges responses.
     */
/*    private List<Address> _getMembers(final String group) {
        final List<Address> ret=new LinkedList<Address>();

        List<Callable<List<Address>>> tasks=new ArrayList<Callable<List<Address>>>(gossip_servers.size());
        for(int i=0; i < gossip_servers.size(); i++) {
            final IpAddress entry=(IpAddress) gossip_servers.elementAt(i);
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                if(log.isErrorEnabled()) log.error("entry.host or entry.port is null");
                continue;
            }

            tasks.add(new Callable<List<Address>>() {
                public List<Address> call() throws Exception {
                    Socket sock=null;
                    DataOutputStream out=null;
                    DataInputStream in=null;
                    try {
                        sock=new Socket();
                        System.out.println("CONNECTING to " + entry);
                        sock.connect(new InetSocketAddress(entry.getIpAddress(), entry.getPort()), sock_conn_timeout);
                        out=new DataOutputStream(sock.getOutputStream());
                        GossipData gossip_req=new GossipData(GossipRouter.GOSSIP_GET, group, null, null);
                        // must send GossipData as fast as possible, otherwise the request might be rejected
                        gossip_req.writeTo(out);
                        out.flush();
                        in=new DataInputStream(sock.getInputStream());
                        GossipData gossip_rsp=new GossipData();
                        gossip_rsp.readFrom(in);
                        System.out.println("gossip_rsp = " + gossip_rsp);
                        return gossip_rsp.mbrs;
                    }
                    catch(Exception ex) {
                        if(log.isErrorEnabled()) log.error("exception connecting to host " + entry);
                        return null;
                    }
                    finally {
                        Util.close(out);
                        Util.close(in);
                        if(sock != null) {
                            try {sock.close();} catch(IOException e) {}
                        }
                    }
                }
            });
        }

        try {
            List<Future<List<Address>>> responses=timer.invokeAll(tasks);
            for(Future<List<Address>> future: responses) {
                if(!future.isCancelled()) {
                    try {
                        List<Address> addrs=future.get();
                        if(addrs != null) {
                            for(Address addr: addrs) {
                                synchronized(ret) {
                                    if(!ret.contains(addr)) {
                                        System.out.println("adding " + addr);
                                        ret.add(addr);
                                    }
                                }
                            }
                        }
                    }
                    catch(Throwable t) {
                    }
                }
            }
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }

        return ret;
    }*/

    private List<Address> _getMembers(final String group, long timeout) {
        final List<Address> ret=new LinkedList<Address>();
        final AtomicInteger num_rsps=new AtomicInteger(0);
        final long stop_time=System.currentTimeMillis() + timeout;

        for(final IpAddress entry: gossip_servers) {
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                if(log.isErrorEnabled()) log.error("entry.host or entry.port is null");
                continue;
            }

            timer.execute(new Runnable() {
                public void run() {
                    Socket sock=null;
                    DataOutputStream out=null;
                    DataInputStream in=null;
                    try {
                        sock=new Socket();
                        if(sock_read_timeout > 0)
                            sock.setSoTimeout(sock_read_timeout);
                        sock.connect(new InetSocketAddress(entry.getIpAddress(), entry.getPort()), sock_conn_timeout);
                        out=new DataOutputStream(sock.getOutputStream());
                        GossipData gossip_req=new GossipData(GossipRouter.GOSSIP_GET, group, null, null);
                        // must send GossipData as fast as possible, otherwise the request might be rejected
                        gossip_req.writeTo(out);
                        out.flush();
                        in=new DataInputStream(sock.getInputStream());
                        GossipData gossip_rsp=new GossipData();
                        gossip_rsp.readFrom(in);
                        if(gossip_rsp.mbrs != null) {
                            for(Address addr: gossip_rsp.mbrs) {
                                synchronized(ret) {
                                    if(!ret.contains(addr)) {
                                        ret.add(addr);
                                    }
                                }
                            }
                        }
                        synchronized(ret) {
                            num_rsps.incrementAndGet();
                            ret.notifyAll();
                        }
                    }
                    catch(Exception ex) {
                        if(log.isErrorEnabled()) log.error("getMembers(" + group + "): exception connecting to host " + entry);
                    }
                    finally {
                        Util.close(out);
                        Util.close(in);
                        if(sock != null) {
                            try {sock.close();} catch(IOException e) {}
                        }
                    }
                }
            });
        }

        synchronized(ret) {
            if(timeout <= 0) {
                if(num_rsps.get() == 0)
                    try {ret.wait();} catch(InterruptedException e) {}
            }
            else {
                long curr_time;
                while(num_rsps.get() == 0 && (curr_time=System.currentTimeMillis()) < stop_time) {
                    long wait_time=stop_time - curr_time;
                    if(wait_time <= 0)
                        break;
                    try {ret.wait(wait_time);} catch(InterruptedException e) {}
                }
            }
        }

        return ret;
    }

    /* ---------------------------------- End of Private methods ------------------------------- */



    /**
     * Periodically iterates through groups and refreshes all registrations with GossipRouter
     */
    private class Refresher implements Runnable {

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


        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                usage();
                return;
            }
            if("-expiry".equals(args[i])) {
                expiry=Long.parseLong(args[++i]);
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
            gossip_client=new GossipClient(gossip_hosts, expiry, 1000, null);
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
        if(!keep_running) {
            gossip_client.stop();
            gossip_client.stopTimer();
        }
    }


    static void usage() {
        System.out.println("GossipClient [-help] [-host <hostname> <port>]+ " +
                           " [-get <groupname>] [-register <groupname hostname port>] [-expiry <msecs>] " +
                           "[-keep_running]]");
    }

}
