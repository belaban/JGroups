// $Id: GossipServer.java,v 1.2 2003/10/15 20:18:32 ovidiuf Exp $

package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.log.Trace;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


/**
 * Maintains a cache of member addresses for each group. There are essentially 2 functions: get the members for
 * a given group and register a new member for a given group. Clients have to periodically renew their
 * registrations (like in JINI leasing), otherwise the cache will be cleaned periodically (oldest entries first).<p>
 * The server should be running at a well-known port. This can be done by for example adding an entry to
 * /etc/inetd.conf on UNIX systems, e.g. <code>gossipsrv stream tcp nowait root /bin/start-gossip-server</code>.
 * <code>gossipsrv</code> has to be defined in /etc/services and <code>start-gossip-server</code> is a script
 * which starts the GossipServer at the well-known port (define in /etc/services). The protocol between GossipServer
 * and GossipClient consists of REGISTER_REQ, GET_MEMBERS_REQ and GET_MEMBERS_RSP protocol data units.<p>
 * The server does not spawn a thread/request, but does all of its processing on the main thread. This should not
 * be a problem as all requests are short-lived. However, the server would essentially cease processing requests
 * if a telnet connected to it.<p>
 * Requires JDK >= 1.3 due to the use of Timer
 * @author Bela Ban Oct 4 2001
 */
public class GossipServer {
    Hashtable groups=new Hashtable();  // groupname - vector of Entry's
    int port=7500;
    ServerSocket srv_sock=null;
    long EXPIRY_TIME=30000;       // time (in msecs) until a cache entry expires
    CacheCleaner cache_cleaner=null;      // task that is periodically invoked to sweep old entries from the cache
    Timer timer=new Timer(true);   // start as daemon thread, so we won't block on it upon termination
    InetAddress bind_address=null;


    public GossipServer(int port) throws Exception {
        this.port=port;
        init();
    }


    public GossipServer(int port, long expiry_time) throws Exception {
        this.port=port;
        EXPIRY_TIME=expiry_time;
        init();
    }

    public GossipServer(int port, long expiry_time, InetAddress bind_address) throws Exception {
        this.port=port;
        this.bind_address=bind_address;
        EXPIRY_TIME=expiry_time;
        init();
    }


    public void run() {
        Socket sock;
        ObjectInputStream input;
        ObjectOutputStream output=null;
        GossipData gossip_req, gossip_rsp;

        while(true) {
            try {
                sock=srv_sock.accept();
                if(Trace.trace)
                    Trace.info("GossipServer.run()", "accepted connection from " + sock.getInetAddress() +
                                                     ":" + sock.getPort());
                sock.setSoLinger(true, 500);
                input=new ObjectInputStream(sock.getInputStream());
                gossip_req=(GossipData) input.readObject();
                gossip_rsp=processGossip(gossip_req);
                if(gossip_rsp != null) {
                    output=new ObjectOutputStream(sock.getOutputStream());
                    output.writeObject(gossip_rsp);
                    output.flush();
                    output.close();
                }
                input.close();
                sock.close();
            }
            catch(Exception ex) {
                Trace.error("GossipServer.run()", "exception=" + ex);
                ex.printStackTrace(); // +++ remove
                continue;
            }
        }
    }


    /* ----------------------------------- Private methods ----------------------------------- */



    void init() throws Exception {
        if(bind_address == null) {
            srv_sock=new ServerSocket(port, 20);  // backlog of 20 connections
            bind_address=srv_sock.getInetAddress();
        }
        else
            srv_sock=new ServerSocket(port, 20, bind_address);  // backlog of 20 connections
        if(Trace.trace) {
            Trace.info("GossipServer.init()", "GossipServer was created at " + new Date());
            Trace.info("GossipServer.init()", "Listening on port " + port + " bound on address " + bind_address);
        }
        cache_cleaner=new CacheCleaner();
        timer.schedule(cache_cleaner, EXPIRY_TIME, EXPIRY_TIME);
    }


    /**
     Process the gossip request. Return a gossip response or null if none.
     */
    GossipData processGossip(GossipData gossip) {
        String group;
        Address mbr;

        if(gossip == null) return null;
        if(Trace.trace) Trace.info("GossipServer.processGossip()", gossip.toString());
        switch(gossip.getType()) {
            case GossipData.REGISTER_REQ:
                group=gossip.getGroup();
                mbr=gossip.getMbr();
                if(group == null || mbr == null) {
                    Trace.error("GossipServer.processGossip()", "group or member is null, cannot register member");
                    return null;
                }
                return processRegisterRequest(group, mbr);

            case GossipData.GET_REQ:
                group=gossip.getGroup();
                if(group == null) {
                    Trace.error("GossipServer.processGossip()", "group is null, cannot get membership");
                    return null;
                }
                return processGetRequest(group);

            case GossipData.GET_RSP:  // should not be received
                Trace.warn("GossipServer.processGossip()", "received a GET_RSP. Should not be received by server");
                return null;

            default:
                Trace.warn("GossipServer.processGossip()", "received unkown gossip request (gossip=" + gossip + ")");
                return null;
        }
    }


    GossipData processRegisterRequest(String group, Address mbr) {
        addMember(group, mbr);
        return null;
    }


    GossipData processGetRequest(String group) {
        GossipData ret=null;
        Vector mbrs=getMembers(group);

        ret=new GossipData(GossipData.GET_RSP, group, null, mbrs);
        if(Trace.trace)
            Trace.info("GossipServer.processGetRequest()", "members are " + mbrs +
                                                           ", gossip_rsp=" + ret);
        return ret;
    }


    /**
     Adds a member to the list for the given group. If the group doesn't exist, it will be created. If the member
     is already present, its timestamp will be updated. Otherwise the member will be added.
     @param group The group name. Guaranteed to be non-null
     @param mbr The member's address. Guaranteed to be non-null
     */
    void addMember(String group, Address mbr) {
        Vector mbrs=(Vector) groups.get(group);
        Entry entry;

        if(mbrs == null) {
            mbrs=new Vector();
            mbrs.addElement(new Entry(mbr));
            groups.put(group, mbrs);
            if(Trace.trace) Trace.info("GossipServer.addMember()", "added " + mbr + " to " + group + " (new group)");
        }
        else {
            entry=findEntry(mbrs, mbr);
            if(entry == null) {
                entry=new Entry(mbr);
                mbrs.addElement(entry);
                if(Trace.trace) Trace.info("GossipServer.addMember()", "added " + mbr + " to " + group);
            }
            else {
                entry.update();
                if(Trace.trace) Trace.info("GossipServer.addMember()", "updated entry " + entry);
            }
        }
    }


    Vector getMembers(String group) {
        Vector ret=null;
        Vector mbrs=(Vector) groups.get(group);

        if(mbrs == null)
            return null;
        ret=new Vector();
        for(int i=0; i < mbrs.size(); i++)
            ret.addElement(((Entry) mbrs.elementAt(i)).mbr);
        return ret;
    }


    Entry findEntry(Vector mbrs, Address mbr) {
        Entry entry=null;

        for(int i=0; i < mbrs.size(); i++) {
            entry=(Entry) mbrs.elementAt(i);
            if(entry.mbr != null && entry.mbr.equals(mbr))
                return entry;
        }
        return null;
    }


    /**
     * Remove expired entries (entries older than EXPIRY_TIME msec).
     */
    void sweep() {
        long current_time=System.currentTimeMillis(), diff;
        int num_entries_removed=0;
        String key=null;
        Vector val;
        Entry entry;

        for(Enumeration e=groups.keys(); e.hasMoreElements();) {
            key=(String) e.nextElement();
            val=(Vector) groups.get(key);
            if(val != null) {
                for(Iterator it=val.listIterator(); it.hasNext();) {
                    entry=(Entry) it.next();
                    diff=current_time - entry.timestamp;
                    if(entry.timestamp + EXPIRY_TIME < current_time) {
                        it.remove();
                        if(Trace.trace)
                            Trace.info("GossipServer.sweep()", "removed member " + entry +
                                                               " from group " + key + "(" + diff + " msecs old)");
                        num_entries_removed++;
                    }
                }
            }
        }

        if(Trace.trace && num_entries_removed > 0)
            Trace.info("GossipServer.sweep()", "done (removed " + num_entries_removed + " entries)");
    }


    /* -------------------------------- End of Private methods ----------------------------------- */



    /**
     *   Maintains the member address plus a timestamp. Used by CacheCleaner thread to remove old entries.
     */
    private static class Entry {
        Address mbr=null;
        long timestamp=0;

        private Entry(Address mbr) {
            this.mbr=mbr;
            update();
        }

        void update() {
            timestamp=System.currentTimeMillis();
        }

        public boolean equals(Object other) {
            if(mbr != null && other != null && other instanceof Address)
                return mbr.equals(other);
            return false;
        }

        public String toString() {
            return "mbr=" + mbr;
        }
    }


    /**
     * Periodically sweeps the cache and removes old items (items that are older than EXPIRY_TIME msecs)
     */
    private class CacheCleaner extends TimerTask {

        public void run() {
            sweep();
        }

    }


    public static void main(String[] args)
            throws java.net.UnknownHostException {
        String arg;
        int port=7500;
        long expiry_time=30000;
        GossipServer gossip_server=null;
        InetAddress address=null;
        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if(arg.equals("-help")) {
                System.out.println("GossipServer [-port <port>] [-expiry <msecs>] [-bindaddress <address>]");
                return;
            }
            if(arg.equals("-port")) {
                port=new Integer(args[++i]).intValue();
                continue;
            }
            if(arg.equals("-expiry")) {
                expiry_time=new Long(args[++i]).longValue();
                continue;
            }
            if(arg.equals("-bindaddress")) {
                address=InetAddress.getByName(args[++i]);
                continue;
            }
            System.out.println("GossipServer [-port <port>] [-expiry <msecs>]");
            return;
        }

        try {
            Trace.init();
        }
        catch(Throwable ex) {
            System.err.println("GossipServer.main(): " + ex);
        }

        try {
            gossip_server=new GossipServer(port, expiry_time, address);
            gossip_server.run();
        }
        catch(Exception e) {
            System.err.println("GossipServer.main(): " + e);
        }
    }


}
