// $Id: GossipClient.java,v 1.2 2003/10/15 20:21:44 ovidiuf Exp $

package org.jgroups.stack;


import org.jgroups.Address;
import org.jgroups.log.Trace;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;


/**
 * Local stub for clients to access one (or more) GossipServers. Will use proprietary protocol
 * (using GossipData PDUs) based on TCP to connect to GossipServer.<p>
 * Requires JDK >= 1.3 due to the use of Timer<p>
 * todo: make access to multiple GossipServer concurrent (1 thread/GossipServer)
 * @author Bela Ban Oct 4 2001
 */
public class GossipClient {
    Timer timer=new Timer();
    Hashtable groups=new Hashtable();               // groups - Vector of Addresses
    Refresher refresher_task=new Refresher();
    Vector gossip_servers=new Vector();          // a list of GossipServers (IpAddress)
    boolean timer_running=false;
    long EXPIRY_TIME=20000;                    // must be less than in GossipServer


    /**
     * Creates the GossipClient
     * @param gossip_host The address and port of the host on which the GossipServer is running
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
    public GossipClient(Vector gossip_hosts, long expiry) {
        if(gossip_hosts == null) {
            Trace.error("GossipClient.GossipClient()", "empty set of GossipServers given");
            return;
        }
        for(int i=0; i < gossip_hosts.size(); i++)
            init((IpAddress) gossip_hosts.elementAt(i), expiry);
    }


    public void stop() {
        timer_running=false;
        timer.cancel();
        groups.clear();
        // provide another refresh tools in case the channel gets reconnected
        timer=new Timer();
        refresher_task=new Refresher();

    }


    /**
     * Adds a GossipServer to be accessed.
     */
    public void addGossipServer(IpAddress gossip_host) {
        if(!gossip_servers.contains(gossip_host))
            gossip_servers.addElement(gossip_host);
    }


    /**
     Adds the member to the given group. If the group already has an entry for the member,
     its timestamp will be updated, preventing the cache cleaner from removing the entry.<p>
     The entry will be registered <em>with all GossipServers that GossipClient is configured to access</em>
     */
    public void register(String group, Address mbr) {
        Vector mbrs;

        if(group == null || mbr == null) {
            Trace.error("GossipClient.register()", "group or mbr is null");
            return;
        }
        mbrs=(Vector) groups.get(group);
        if(mbrs == null) {
            mbrs=new Vector();
            mbrs.addElement(mbr);
            groups.put(group, mbrs);
        }
        else {
            if(!mbrs.contains(mbr))
                mbrs.addElement(mbr);
        }

        _register(group, mbr); // update entry in GossipServer

        if(!timer_running) {
            timer.schedule(refresher_task, EXPIRY_TIME, EXPIRY_TIME);
            timer_running=true;
        }
    }


    /**
     Returns all members of a given group
     @param group The group name
     @return Vector A list of Addresses
     */
    public Vector getMembers(String group) {
        if(group == null) {
            Trace.error("GossipClient.getMembers()", "group is null");
            return null;
        }

        return _getMembers(group);
    }



    /* ------------------------------------- Private methods ----------------------------------- */


    void init(IpAddress gossip_host, long expiry) {
        EXPIRY_TIME=expiry;
        addGossipServer(gossip_host);
    }


    /**
     * Registers the group|mbr with *all* GossipServers
     * todo Parallelize GossipServer access
     */
    void _register(String group, Address mbr) {
        Socket sock;
        ObjectOutputStream out;
        IpAddress entry;
        GossipData gossip_req;

        for(int i=0; i < gossip_servers.size(); i++) {
            entry=(IpAddress) gossip_servers.elementAt(i);
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                Trace.error("GossipClient._register()", "entry.host or entry.port is null");
                continue;
            }
            try {
                if(Trace.trace)
                    Trace.info("GossipClient._register()", "REGISTER_REQ --> " +
                                                           entry.getIpAddress() + ":" + entry.getPort());
                sock=new Socket(entry.getIpAddress(), entry.getPort());
                out=new ObjectOutputStream(sock.getOutputStream());
                gossip_req=new GossipData(GossipData.REGISTER_REQ, group, mbr, null);
                // must send GossipData as fast as possible, otherwise the
                // request might be rejected
                out.writeObject(gossip_req);
                out.flush();
                sock.close();
            }
            catch(Exception ex) {
                Trace.error("GossipClient._register()", "exception connecting to host " + entry + ": " + ex);
            }
        }
    }

    /**
     * Sends a GET_MBR_REQ to *all* GossipServers, merges responses.
     */
    Vector _getMembers(String group) {
        Vector ret=new Vector();
        Socket sock;
        ObjectOutputStream out;
        ObjectInputStream in;
        IpAddress entry;
        GossipData gossip_req, gossip_rsp;
        Address mbr;

        for(int i=0; i < gossip_servers.size(); i++) {
            entry=(IpAddress) gossip_servers.elementAt(i);
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
                Trace.error("GossipClient._getMembers()", "entry.host or entry.port is null");
                continue;
            }
            try {
                if(Trace.trace)
                    Trace.info("GossipClient._getMembers()", "GET_REQ --> " +
                                                             entry.getIpAddress() + ":" + entry.getPort());
                sock=new Socket(entry.getIpAddress(), entry.getPort());
                out=new ObjectOutputStream(sock.getOutputStream());

                gossip_req=new GossipData(GossipData.GET_REQ, group, null, null);
                // must send GossipData as fast as possible, otherwise the
                // request might be rejected
                out.writeObject(gossip_req);
                out.flush();

                in=new ObjectInputStream(sock.getInputStream());
                gossip_rsp=(GossipData) in.readObject();
                if(gossip_rsp.mbrs != null) { // merge with ret
                    for(int j=0; j < gossip_rsp.mbrs.size(); j++) {
                        mbr=(Address) gossip_rsp.mbrs.elementAt(j);
                        if(!ret.contains(mbr))
                            ret.addElement(mbr);
                    }
                }


                sock.close();
            }
            catch(Exception ex) {
                Trace.error("GossipClient._getMembers()", "exception connecting to host " + entry + ": " + ex);
            }
        }

        return ret;
    }

    /* ---------------------------------- End of Private methods ------------------------------- */



    /**
     * Periodically iterates through groups and refreshes all registrations with GossipServer
     */
    private class Refresher extends TimerTask {

        public void run() {
            int num_items=0;
            String group;
            Vector mbrs;
            Address mbr;

            if(Trace.trace) Trace.info("GossipClient.Refresher.run()", "refresher task is run");
            for(Enumeration e=groups.keys(); e.hasMoreElements();) {
                group=(String) e.nextElement();
                mbrs=(Vector) groups.get(group);
                if(mbrs != null) {
                    for(int i=0; i < mbrs.size(); i++) {
                        mbr=(Address) mbrs.elementAt(i);
                        if(Trace.trace)
                            Trace.info("GossipClient.Refresher.run()",
                                       "registering " + group + " : " + mbr);
                        register(group, mbr);
                        num_items++;
                    }
                }
            }

            if(Trace.trace)
                Trace.info("GossipClient.Refresher.run()", "refresher task done. Registered " + num_items + " items");
        }

    }


    public static void main(String[] args) {
        Vector gossip_hosts=new Vector();
        String host;
        InetAddress ip_addr;
        int port;
        boolean get=false, register=false, keep_running=false;
        String register_host=null;
        int register_port=0;
        String get_group=null, register_group=null;
        GossipClient gossip_client=null;
        Vector mbrs;
        long expiry=20000;


        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-help")) {
                usage();
                return;
            }
            if(args[i].equals("-expiry")) {
                expiry=new Long(args[++i]).longValue();
                continue;
            }
            if(args[i].equals("-host")) {
                host=args[++i];
                port=new Integer(args[++i]).intValue();
                try {
                    ip_addr=InetAddress.getByName(host);
                    gossip_hosts.addElement(new IpAddress(ip_addr, port));
                }
                catch(Exception ex) {
                    System.err.println(ex);
                }
                continue;
            }
            if(args[i].equals("-keep_running")) {
                keep_running=true;
                continue;
            }
            if(args[i].equals("-get")) {
                get=true;
                get_group=args[++i];
                continue;
            }
            if(args[i].equals("-register")) {
                register_group=args[++i];
                register_host=args[++i];
                register_port=new Integer(args[++i]).intValue();
                register=true;
                continue;
            }
            usage();
            return;
        }

        if(gossip_hosts.size() == 0) {
            System.err.println("At least 1 GossipServer has to be given");
            return;
        }

        if(!register && !get) {
            System.err.println("Neither get nor register command given, will not do anything");
            return;
        }

        try {
            Trace.init();
        }
        catch(Throwable ex) {
            System.err.println("GossipClient.main(): error initailizing JGroups Trace: " + ex);
        }

        try {
            gossip_client=new GossipClient(gossip_hosts, expiry);
            if(register) {
                System.out.println("Registering " + register_group + " --> " + register_host + ":" + register_port);
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
                           " [-get <groupname>] [-register <groupname hostname port>] [-expiry <msecs>] " +
                           "[-keep_running]]");
    }

}
