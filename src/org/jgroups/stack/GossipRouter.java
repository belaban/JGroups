
package org.jgroups.stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ShutdownRejectedExecutionHandler;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.ThreadManagerThreadPoolExecutor;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Router for TCP based group comunication (using layer TCP instead of UDP).
 * Instead of the TCP layer sending packets point-to-point to each other
 * member, it sends the packet to the router which - depending on the target
 * address - multicasts or unicasts it to the group / or single member.<p>
 * This class is especially interesting for applets which cannot directly make
 * connections (neither UDP nor TCP) to a host different from the one they were
 * loaded from. Therefore, an applet would create a normal channel plus
 * protocol stack, but the bottom layer would have to be the TCP layer which
 * sends all packets point-to-point (over a TCP connection) to the router,
 * which in turn forwards them to their end location(s) (also over TCP). A
 * centralized router would therefore have to be running on the host the applet
 * was loaded from.<p>
 * An alternative for running JGroups in an applet (IP multicast is not allows
 * in applets as of 1.2), is to use point-to-point UDP communication via the
 * gossip server. However, then the appplet has to be signed which involves
 * additional administrative effort on the part of the user.<p>
 * @author Bela Ban
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Id: GossipRouter.java,v 1.40 2008/12/18 12:23:37 vlada Exp $
 * @since 2.1.1
 */
public class GossipRouter {
    public static final byte CONNECT=1; // CONNECT(group, addr) --> local address
    public static final byte DISCONNECT=2; // DISCONNECT(group, addr)
    public static final byte REGISTER=3; // REGISTER(group, addr)
    public static final byte GOSSIP_GET=4; // GET(group) --> List<addr> (members)
    public static final byte ROUTER_GET=5; // GET(group) --> List<addr> (members)
    public static final byte GET_RSP=6; // GET_RSP(List<addr>)
    public static final byte UNREGISTER=7; // UNREGISTER(group, addr)
    public static final byte DUMP=8; // DUMP
    public static final byte SHUTDOWN=9;

    public static final int PORT=12001;
    public static final long EXPIRY_TIME=30000;
    public static final long GOSSIP_REQUEST_TIMEOUT=1000;
    public static final long ROUTING_CLIENT_REPLY_TIMEOUT=120000;

    @ManagedAttribute(description="server port on which the GossipRouter accepts client connections", writable=true)
    private int port;

    @ManagedAttribute(description="address to which the GossipRouter should bind", writable=true, name="bindAddress")
    private String bindAddressString;

    @ManagedAttribute(description="time (in msecs) until a cached 'gossip' member entry expires", writable=true)
    private long expiryTime=30000;

    @ManagedAttribute(description="number of millisecs the main thread waits to receive a gossip request " +
            "after connection was established; upon expiration, the router initiates " +
            "the routing protocol on the connection. Don't set the interval too big, " +
            "otherwise the router will appear slow in answering routing requests.", writable=true)
    private long gossipRequestTimeout;

    @ManagedAttribute(description="time (in ms) main thread waits for a router client to send the routing " +
            "request type and the group afiliation before it declares the request " +
            "failed.", writable=true)
    private long routingClientReplyTimeout;

    // Maintains associations between groups and their members
    private final ConcurrentMap<String,ConcurrentMap<Address,AddressEntry>> routingTable=new ConcurrentHashMap<String,ConcurrentMap<Address,AddressEntry>>();

    private ServerSocket srvSock=null;
    private InetAddress bindAddress=null;

    @Property(description="Time (in millis) for setting SO_LINGER on sockets returned from accept(). " +
            "0 means do not set SO_LINGER")
    private long linger_timeout=2000L;

    @Property(description="Time (in millis) for SO_TIMEOUT on sockets returned from accept(). " +
            "0 means don't set SO_TIMEOUT")
    private long sock_read_timeout=3000L;

    @Property(description="The max queue size of backlogged connections")
    private int backlog=1000;

    @ManagedAttribute(description="operational status", name="running")
    private boolean up=true;

    @ManagedAttribute(description="whether to discard message sent to self", writable=true)
    private boolean discard_loopbacks=false;
    
    @ManagedAttribute(description="Minimum thread pool size for incoming connections. Default is 2")
    protected int thread_pool_min_threads=2;

	@ManagedAttribute(description="Maximum thread pool size for incoming connections. Default is 10")
    protected int thread_pool_max_threads=10;
   
    
    @ManagedAttribute(description="Timeout in milliseconds to remove idle thread from regular pool. Default is 30000")
    protected long thread_pool_keep_alive_time=30000;

    @ManagedAttribute(description="Switch for enabling thread pool for incoming connections. Default true")
    protected boolean thread_pool_enabled=false;
  
    @ManagedAttribute(description="Use queue to enqueue incoming connections")
    protected boolean thread_pool_queue_enabled=true;

    
    @ManagedAttribute(description="Maximum queue size for incoming connections")
    protected int thread_pool_queue_max_size=50;

    @ManagedAttribute(description="Thread rejection policy. Possible values are Abort, Discard, DiscardOldest and Run Default is Run")
    protected String thread_pool_rejection_policy="Run";
    
    protected ExecutorService thread_pool;
    
    protected BlockingQueue<Runnable> thread_pool_queue=null;
    
    protected ThreadFactory default_thread_factory = new DefaultThreadFactory(Util.getGlobalThreadGroup(), "gossip-handlers", true, true);


    // the cache sweeper
    protected Timer timer=null;

    protected final Log log=LogFactory.getLog(this.getClass());

    private boolean jmx=false;
	private boolean registered= false;
   


    public GossipRouter() {
        this(PORT);
    }

    public GossipRouter(int port) {
        this(port, null);
    }

    public GossipRouter(int port, String bindAddressString) {
        this(port, bindAddressString, EXPIRY_TIME);
    }

    public GossipRouter(int port, String bindAddressString, long expiryTime) {
        this(port, bindAddressString, expiryTime, GOSSIP_REQUEST_TIMEOUT, ROUTING_CLIENT_REPLY_TIMEOUT);
    }

    public GossipRouter(int port, String bindAddressString,
                        long expiryTime, long gossipRequestTimeout,
                        long routingClientReplyTimeout) {
        this.port=port;
        this.bindAddressString=bindAddressString;
        this.expiryTime=expiryTime;
        this.gossipRequestTimeout=gossipRequestTimeout;
        this.routingClientReplyTimeout=routingClientReplyTimeout;
    }

    public GossipRouter(int port, String bindAddressString,
                        long expiryTime, long gossipRequestTimeout,
                        long routingClientReplyTimeout, boolean jmx) {
        this(port, bindAddressString, expiryTime, gossipRequestTimeout, routingClientReplyTimeout);
        this.jmx=jmx;
    }


    public void setPort(int port) {
        this.port=port;
    }

    public int getPort() {
        return port;
    }

    public void setBindAddress(String bindAddress) {
        bindAddressString=bindAddress;
    }

    public String getBindAddress() {
        return bindAddressString;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog=backlog;
    }

    public void setExpiryTime(long expiryTime) {
        this.expiryTime=expiryTime;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    public void setGossipRequestTimeout(long gossipRequestTimeout) {
        this.gossipRequestTimeout=gossipRequestTimeout;
    }

    public long getGossipRequestTimeout() {
        return gossipRequestTimeout;
    }

    public void setRoutingClientReplyTimeout(long routingClientReplyTimeout) {
        this.routingClientReplyTimeout=routingClientReplyTimeout;
    }

    public long getRoutingClientReplyTimeout() {
        return routingClientReplyTimeout;
    }

    @ManagedAttribute(description="status")
    public boolean isStarted() {
        return srvSock != null;
    }

    public boolean isDiscardLoopbacks() {
        return discard_loopbacks;
    }

    public void setDiscardLoopbacks(boolean discard_loopbacks) {
        this.discard_loopbacks=discard_loopbacks;
    }

    public long getLingerTimeout() {
        return linger_timeout;
    }

    public void setLingerTimeout(long linger_timeout) {
        this.linger_timeout=linger_timeout;
    }

    public long getSocketReadTimeout() {
        return sock_read_timeout;
    }

    public void setSocketReadTimeout(long sock_read_timeout) {
        this.sock_read_timeout=sock_read_timeout;
    }
    
    public ThreadFactory getDefaultThreadPoolThreadFactory() {
        return default_thread_factory;
    }

    public void setDefaultThreadPoolThreadFactory(ThreadFactory factory) {
        default_thread_factory=factory;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setThreadFactory(factory);
    }
    
    public int getThreadPoolMinThreads() {
		return thread_pool_min_threads;
	}

	public void setThreadPoolMinThreads(int thread_pool_min_threads) {
		this.thread_pool_min_threads = thread_pool_min_threads;
	}

	public int getThreadPoolMaxThreads() {
		return thread_pool_max_threads;
	}

	public void setThreadPoolMaxThreads(int thread_pool_max_threads) {
		this.thread_pool_max_threads = thread_pool_max_threads;
	}

	public long getThreadPoolKeepAliveTime() {
		return thread_pool_keep_alive_time;
	}

	public void setThreadPoolKeepAliveTime(long thread_pool_keep_alive_time) {
		this.thread_pool_keep_alive_time = thread_pool_keep_alive_time;
	}

	public boolean isThreadPoolEnabled() {
		return thread_pool_enabled;
	}

	public void setThreadPoolEnabled(boolean thread_pool_enabled) {
		this.thread_pool_enabled = thread_pool_enabled;
	}

	public boolean isThreadPoolQueueEnabled() {
		return thread_pool_queue_enabled;
	}

	public void setThreadPoolQueueEnabled(boolean thread_pool_queue_enabled) {
		this.thread_pool_queue_enabled = thread_pool_queue_enabled;
	}

	public int getThreadPoolQueueMaxSize() {
		return thread_pool_queue_max_size;
	}

	public void setThreadPoolQueueMaxSize(int thread_pool_queue_max_size) {
		this.thread_pool_queue_max_size = thread_pool_queue_max_size;
	}

	public String getThreadPoolRejectionPolicy() {
		return thread_pool_rejection_policy;
	}

	public void setThreadPoolRejectionPolicy(String thread_pool_rejection_policy) {
		this.thread_pool_rejection_policy = thread_pool_rejection_policy;
	}


    public static String type2String(int type) {
        switch(type) {
            case CONNECT:
                return "CONNECT";
            case DISCONNECT:
                return "DISCONNECT";
            case REGISTER:
                return "REGISTER";
            case GOSSIP_GET:
                return "GOSSIP_GET";
            case ROUTER_GET:
                return "ROUTER_GET";
            case GET_RSP:
                return "GET_RSP";
            case UNREGISTER:
                return "UNREGISTER";
            case DUMP:
                return "DUMP";
            case SHUTDOWN:
                return "SHUTDOWN";
            default:
                return "unknown";
        }
    }


    public void create() throws Exception {
    }

    /**
     * Lifecycle operation. Called after create(). When this method is called, the managed attributes have already
     * been set.<br> Brings the Router into a fully functional state.
     */
    @ManagedOperation(description="Lifecycle operation. Called after create(). When this method is called, " +
            "the managed attributes have already been set. Brings the Router into a fully functional state.")
    public void start() throws Exception {
        if(srvSock != null) {
            throw new Exception("Router already started.");
        }

        if(jmx && !registered) {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.register(this, server, "jgroups:name=GossipRouter");
            registered = true;
        }

        if(bindAddressString != null) {
            bindAddress=InetAddress.getByName(bindAddressString);
            srvSock=new ServerSocket(port, backlog, bindAddress);
        }
        else {
            srvSock=new ServerSocket(port, backlog);
        }

        up=true;
        
        if (thread_pool_enabled) {
			if (thread_pool_queue_enabled) {
				thread_pool_queue = new LinkedBlockingQueue<Runnable>(
						thread_pool_queue_max_size);
			} 
			else {
				thread_pool_queue = new SynchronousQueue<Runnable>();
			}
			thread_pool = createThreadPool(thread_pool_min_threads,
					thread_pool_max_threads, thread_pool_keep_alive_time,
					thread_pool_rejection_policy, thread_pool_queue,
					default_thread_factory);
		} 
        else { 
			thread_pool = Executors.newSingleThreadExecutor(default_thread_factory);
		}

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                cleanup();
                GossipRouter.this.stop();
            }
        });

        // start the main server thread
        new Thread(new Runnable() {
            public void run() {
                    mainLoop();
                    cleanup();
                }
        }, "GossipRouter").start();

        // starts the cache sweeper as daemon thread, so we won't block on it
        // upon termination
        timer=new Timer(true);
        timer.schedule(new TimerTask() {
            public void run() {
                sweep();
            }
        }, expiryTime, expiryTime);
    }

    /**
     * Always called before destroy(). Close connections and frees resources.
     */
    @ManagedOperation(description="Always called before destroy(). Closes connections and frees resources")
    public void stop() {
        up=false;
        thread_pool.shutdownNow();

        timer.cancel();
        if(srvSock != null) {
            shutdown();
            Util.close(srvSock);
            // exiting the mainLoop will clean the tables
            srvSock=null;
        }
        if(log.isInfoEnabled()) log.info("router stopped");
    }

    public void destroy() {
    }



    @ManagedOperation(description="dumps the contents of the routing table")
    public String dumpRoutingTable() {
        String label="routing";
        StringBuilder sb=new StringBuilder();

        if(routingTable.isEmpty()) {
            sb.append("empty ").append(label).append(" table");
        }
        else {
            for(Iterator<String> i=routingTable.keySet().iterator(); i.hasNext();) {
                String gname=i.next();
                sb.append("GROUP: '" + gname + "'\n");
                Map<Address,AddressEntry> map=routingTable.get(gname);
                if(map == null) {
                    sb.append("\tnull list of addresses\n");
                }
                else if(map.isEmpty()) {
                    sb.append("\tempty list of addresses\n");
                }
                else {
                    for(Iterator<AddressEntry> j=map.values().iterator(); j.hasNext();) {
                        sb.append('\t').append(i.next()).append('\n');
                    }
                }
            }
        }
        return sb.toString();
    }


    private void mainLoop() {

		if (bindAddress == null) {
			bindAddress = srvSock.getInetAddress();
		}
		System.out.println("GossipRouter started at " + new Date()
				+ "\nListening on port " + port + " bound on address "
				+ bindAddress + '\n');

		while (up && srvSock != null) {
			try {
				final Socket sock = srvSock.accept();
				if (linger_timeout > 0) {
					int linger = Math.min(1, (int) (linger_timeout / 1000));
					sock.setSoLinger(true, linger);
				}
				if (sock_read_timeout > 0) {
					sock.setSoTimeout((int) sock_read_timeout);
				}

				final DataInputStream input = new DataInputStream(sock.getInputStream());
				Runnable task = new Runnable(){

					public void run() {
						DataOutputStream output =null;
						Address peer_addr = null, mbr, logical_addr;
						String group;
						GossipData req = new GossipData();
						try {
							req.readFrom(input);

							switch (req.getType()) {
							case GossipRouter.REGISTER:
								mbr = req.getAddress();
								group = req.getGroup();
								if (log.isTraceEnabled())
									log.trace("REGISTER(" + group + ", " + mbr + ")");
								if (group == null || mbr == null) {
									if (log.isErrorEnabled())
										log.error("group or member is null, cannot register member");
								} else
									addGossipEntry(group, mbr, new AddressEntry(mbr));
								Util.close(input);
								Util.close(sock);
								break;

							case GossipRouter.UNREGISTER:
								mbr = req.getAddress();
								group = req.getGroup();
								if (log.isTraceEnabled())
									log.trace("UNREGISTER(" + group + ", " + mbr + ")");
								if (group == null || mbr == null) {
									if (log.isErrorEnabled())
										log.error("group or member is null, cannot unregister member");
								} else
									removeGossipEntry(group, mbr);
								Util.close(input);
								Util.close(output);
								Util.close(sock);
								break;

							case GossipRouter.GOSSIP_GET:
								group = req.getGroup();
								List<Address> mbrs = null;
								Map<Address, AddressEntry> map;
								map = routingTable.get(group);
								if (map != null) {
									mbrs = new LinkedList<Address>(map.keySet());
								}

								if (log.isTraceEnabled())
									log.trace("GOSSIP_GET(" + group + ") --> " + mbrs);
								output = new DataOutputStream(sock.getOutputStream());
								GossipData rsp = new GossipData(GossipRouter.GET_RSP,group, null, mbrs);
								rsp.writeTo(output);
								Util.close(input);
								Util.close(output);
								Util.close(sock);
								break;

							case GossipRouter.ROUTER_GET:
								group = req.getGroup();
								output = new DataOutputStream(sock.getOutputStream());

								List<Address> ret = null;
								map = routingTable.get(group);
								if (map != null) {
									ret = new LinkedList<Address>(map.keySet());
								} else
									ret = new LinkedList<Address>();
								if (log.isTraceEnabled())
									log.trace("ROUTER_GET(" + group + ") --> " + ret);
								rsp = new GossipData(GossipRouter.GET_RSP, group, null,
										ret);
								rsp.writeTo(output);
								Util.close(input);
								Util.close(output);
								Util.close(sock);
								break;

							case GossipRouter.DUMP:
								output = new DataOutputStream(sock.getOutputStream());
								output.writeUTF(dumpRoutingTable());
								Util.close(input);
								Util.close(output);
								Util.close(sock);
								break;

							case GossipRouter.CONNECT:
								sock.setSoTimeout(0); // we have to disable this here
								output = new DataOutputStream(sock.getOutputStream());
								peer_addr = new IpAddress(sock.getInetAddress(), sock.getPort());
								logical_addr = req.getAddress();
								String group_name = req.getGroup();

								if (log.isTraceEnabled())
									log.trace("CONNECT(" + group_name + ", "+ logical_addr + ")");
								SocketThread st = new SocketThread(sock, input,group_name, logical_addr);
								addEntry(group_name, logical_addr, new AddressEntry(logical_addr, peer_addr, sock, st, output));
								st.start();
								break;

							case GossipRouter.DISCONNECT:
								Address addr = req.getAddress();
								group_name = req.getGroup();
								removeEntry(group_name, addr);
								if (log.isTraceEnabled())
									log.trace("DISCONNECT(" + group_name + ", " + addr+ ")");
								Util.close(input);
								Util.close(output);
								Util.close(sock);
								break;

							case GossipRouter.SHUTDOWN:
								if (log.isInfoEnabled())
									log.info("router shutting down");
								Util.close(input);
								Util.close(output);
								Util.close(sock);
								up = false;
								break;
							default:
								if (log.isWarnEnabled())
									log.warn("received unkown gossip request (gossip="+ req + ')');
								break;
							}
						} catch (Exception e) {
							if (up)
								if (log.isErrorEnabled())
									log.error("failure handling a client request", e);
							Util.close(input);
							Util.close(output);
							Util.close(sock);
						}
					}};
					if(!thread_pool.isShutdown())
						thread_pool.submit(task);
					else
						task.run();
			} catch (Exception exc) {
				if (log.isErrorEnabled())
					log.error("failure receiving and setting up a client request", exc);
			}
		}
	}
    
    protected ExecutorService createThreadPool(int min_threads,
			int max_threads, long keep_alive_time, String rejection_policy,
			BlockingQueue<Runnable> queue, final ThreadFactory factory) {

		ThreadPoolExecutor pool = new ThreadManagerThreadPoolExecutor(
				min_threads, max_threads, keep_alive_time,
				TimeUnit.MILLISECONDS, queue);
		pool.setThreadFactory(factory);

		// default
		RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();
		if (rejection_policy != null) {
			if (rejection_policy.equals("abort"))
				handler = new ThreadPoolExecutor.AbortPolicy();
			else if (rejection_policy.equals("discard"))
				handler = new ThreadPoolExecutor.DiscardPolicy();
			else if (rejection_policy.equals("discardoldest"))
				handler = new ThreadPoolExecutor.DiscardOldestPolicy();
		}
		pool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(
				handler));

		return pool;
	}



    /**
     * Cleans the routing tables while the Router is going down.
     */
    private void cleanup() {
        // shutdown the routing threads and cleanup the tables
        for(Map<Address,AddressEntry> map: routingTable.values()) {
            if(map != null) {
                for(AddressEntry entry: map.values()) {
                    entry.destroy();
                }
            }
        }
        routingTable.clear();
    }

    /**
     * Connects to the ServerSocket and sends the shutdown header.
     */
    private void shutdown() {
        Socket s=null;
        DataOutputStream dos=null;
        try {
            s=new Socket(srvSock.getInetAddress(),srvSock.getLocalPort());
            dos=new DataOutputStream(s.getOutputStream());
            dos.writeInt(SHUTDOWN);
            dos.writeUTF("");
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("shutdown failed: " + e);
        }
        finally {
            Util.close(s);
            Util.close(dos);
        }
    }




    /**
     * Removes expired gossip entries (entries older than EXPIRY_TIME msec).
     * @since 2.2.1
     */
    private void sweep() {
        long diff, currentTime=System.currentTimeMillis();
        int num_entries_removed=0;
       
        for(Iterator<Entry<String,ConcurrentMap<Address,AddressEntry>>> it=routingTable.entrySet().iterator(); it.hasNext();) {
        	Entry <String,ConcurrentMap<Address,AddressEntry>> entry=it.next();
        	Map <Address,AddressEntry> map=entry.getValue();
            if(map == null || map.isEmpty()) {
                it.remove();
                continue;
            }
            for(Iterator<Entry<Address,AddressEntry>> it2=map.entrySet().iterator(); it2.hasNext();) {
            	Entry<Address,AddressEntry>entry2=it2.next();
                AddressEntry ae=entry2.getValue();
                diff=currentTime - ae.timestamp;
                if(diff > expiryTime) {
                    it2.remove();
                    if(log.isTraceEnabled())
                        log.trace("removed " + ae.logical_addr + " (" + diff + " msecs old)");
                    num_entries_removed++;
                }
            }
        }

        if(num_entries_removed > 0) {
            if(log.isTraceEnabled()) log.trace("done (removed " + num_entries_removed + " entries)");
        }
    }




    private void route(Address dest, String dest_group, byte[] msg, Address sender) {
        //if(log.isTraceEnabled()) {
          //  int len=msg != null? msg.length : 0;
            //log.trace("routing request from " + sender + " for " + dest_group + " to " +
              //      (dest == null? "ALL" : dest.toString()) + ", " + len + " bytes");
        //}

        if(dest == null) { // send to all members in group dest.getChannelName()
            if(dest_group == null) {
                if(log.isErrorEnabled()) log.error("both dest address and group are null");
            }
            else {
                sendToAllMembersInGroup(dest_group, msg, sender);
            }
        }
        else {
            // send to destination address
            AddressEntry ae=findAddressEntry(dest_group, dest);
            if(ae == null) {
                if(log.isTraceEnabled())
                    log.trace("cannot find " + dest + " in the routing table, \nrouting table=\n" + dumpRoutingTable());
                return;
            }
            if(ae.output == null) {
                if(log.isErrorEnabled()) log.error(dest + " is associated with a null output stream");
                return;
            }
            try {
                sendToMember(dest, ae.output, msg, sender);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("failed sending message to " + dest + ": " + e.getMessage());
                removeEntry(dest_group, dest); // will close socket
            }
        }
    }



     private void addEntry(String groupname, Address logical_addr, AddressEntry entry) {
         addEntry(groupname, logical_addr, entry, false);
     }


    /**
     * Adds a new member to the routing group.
     */
    private void addEntry(String groupname, Address logical_addr, AddressEntry entry, boolean update_only) {
        if(groupname == null || logical_addr == null) {
            if(log.isErrorEnabled()) log.error("groupname or logical_addr was null, entry was not added");
            return;
        }

        ConcurrentMap<Address,AddressEntry> mbrs=routingTable.get(groupname);
        if(mbrs == null) {
            mbrs=new ConcurrentHashMap<Address,AddressEntry>();
            mbrs.put(logical_addr, entry);
            routingTable.putIfAbsent(groupname, mbrs);
        }
        else {
            AddressEntry tmp=mbrs.get(logical_addr);
            if(tmp != null) { // already present
                if(update_only) {
                    tmp.update();
                    return;
                }
                tmp.destroy();
            }
            mbrs.put(logical_addr, entry);
        }
    }



    private void removeEntry(String groupname, Address logical_addr) {
        Map<Address,AddressEntry>val=routingTable.get(groupname);
        if(val == null)
            return;
        synchronized(val) {
            AddressEntry entry=val.get(logical_addr);
            if(entry != null) {
                entry.destroy();
                val.remove(logical_addr);
            }
        }
    }


    /**
     * @return null if not found
     */
    private AddressEntry findAddressEntry(String group_name, Address logical_addr) {
        if(group_name == null || logical_addr == null)
            return null;
        Map<Address,AddressEntry> val=routingTable.get(group_name);
        if(val == null)
            return null;
        return (AddressEntry)val.get(logical_addr);
    }



    /**
      * Adds a new member to the group in the gossip table or renews the
      * membership where is the case.
      * @since 2.2.1
      */
     private void addGossipEntry(String groupname, Address logical_addr, AddressEntry e) {
         addEntry(groupname, logical_addr, e, true);
     }


     private void removeGossipEntry(String groupname, Address mbr) {
         removeEntry(groupname, mbr);
     }



    private void sendToAllMembersInGroup(String groupname, byte[] msg, Address sender) {
        Map<Address,AddressEntry> val = routingTable.get(groupname);
        if(val == null || val.isEmpty())
            return;
        
        synchronized(val) {
            for(Iterator<Entry<Address,AddressEntry>> i=val.entrySet().iterator(); i.hasNext();) {
            	Entry<Address,AddressEntry> tmp=i.next();
                AddressEntry entry=tmp.getValue();
                DataOutputStream dos=entry.output;

                if(dos != null) {
                    // send only to 'connected' members
                    try {
                        sendToMember(null, dos, msg, sender);
                    }
                    catch(Exception e) {
                        if(log.isWarnEnabled()) log.warn("cannot send to " + entry.logical_addr + ": " + e.getMessage());
                        entry.destroy(); // this closes the socket
                        i.remove();
                    }
                }
            }
        }
    }


    /**
     * @throws IOException
     */
    private void sendToMember(Address dest, DataOutputStream out, byte[] msg, Address sender) throws IOException {
        if(out == null)
            return;

        if(discard_loopbacks && dest != null && dest.equals(sender)) {
            return;
        }

        synchronized(out) {
            Util.writeAddress(dest, out);
            out.writeInt(msg.length);
            out.write(msg, 0, msg.length);
        }
    }


    /**
     * Class used to store Addresses in both routing and gossip tables.
     * If it is used for routing, sock and output have valid values, otherwise
     * they're null and only the timestamp counts.
     */
    class AddressEntry {
        Address logical_addr=null, physical_addr=null;
        Socket sock=null;
        DataOutputStream output=null;
        long timestamp=0;
        final SocketThread thread;

        /**
         * AddressEntry for a 'gossip' membership.
         */
        public AddressEntry(Address addr) {
            this(addr, null, null, null, null);
        }

        public AddressEntry(Address logical_addr, Address physical_addr, Socket sock, SocketThread thread, DataOutputStream output) {
            this.logical_addr=logical_addr;
            this.physical_addr=physical_addr;
            this.sock=sock;
            this.thread=thread;
            this.output=output;
            this.timestamp=System.currentTimeMillis();
        }

        void destroy() {
            if(thread != null) {
                thread.finish();
            }
            Util.close(output);
            output=null;
            Util.close(sock);
            sock=null;
            timestamp=0;
        }

        public void update() {
            timestamp=System.currentTimeMillis();
        }

        public boolean equals(Object other) {
            return other instanceof AddressEntry && logical_addr.equals(((AddressEntry)other).logical_addr);
        }

        public String toString() {
            StringBuilder sb=new StringBuilder("logical addr=");
            sb.append(logical_addr).append(" (").append(physical_addr).append(")");
            //if(sock != null) {
              //  sb.append(", sock=");
                //sb.append(sock);
            //}
            if(timestamp > 0) {
                long diff=System.currentTimeMillis() - timestamp;
                sb.append(", ").append(diff).append(" ms old");
            }
            return sb.toString();
        }
    }


    private static int threadCounter=0;


    /**
     * A SocketThread manages one connection to a client. Its main task is message routing.
     */
    class SocketThread extends Thread {
        private volatile boolean active=true;
        Socket sock=null;
        DataInputStream input=null;
        Address logical_addr=null;
        String group_name=null;


        public SocketThread(Socket sock, DataInputStream ois, String group_name, Address logical_addr) {
            super(Util.getGlobalThreadGroup(), "SocketThread " + (threadCounter++));
            this.sock=sock;
            input=ois;
            this.group_name=group_name;
            this.logical_addr=logical_addr;
        }

        void closeSocket() {
            Util.close(input);
            Util.close(sock);
        }

        void finish() {
            active=false;
        }


        public void run() {
            byte[] buf;
            int len;
            Address dst_addr=null;
            String gname;
            
            DataOutputStream output = null;
            try {
                output=new DataOutputStream(sock.getOutputStream());
                output.writeBoolean(true);
            }
            catch(IOException e1) {
                try {
                    output.writeBoolean(false);
                }
                catch(IOException e) {}
            }
            
            while(active) {
                try {
                    // 1. Group name is first
                    gname=input.readUTF();

                    // 2. Second is the destination address
                    dst_addr=Util.readAddress(input);

                    // 3. Then the length of the byte buffer representing the message
                    len=input.readInt();
                    if(len == 0) {
                        if(log.isWarnEnabled()) log.warn("received null message");
                        continue;
                    }

                    // 4. Finally the message itself, as a byte buffer
                    buf=new byte[len];
                    input.readFully(buf, 0, buf.length);  // message
                }
                catch(Exception io_ex) {
                    if(log.isTraceEnabled())
                        log.trace(sock.getInetAddress().getHostName() + ':' + sock.getPort() +
                                " closed connection; removing it from routing table");
                    removeEntry(group_name, logical_addr); // will close socket
                    return;
                }

                try {
                    route(dst_addr, gname, buf, logical_addr);
                }
                catch(Exception e) {
                    if(log.isErrorEnabled()) log.error("failed routing request to " + dst_addr, e);
                    break;
                }
            }
            closeSocket();
        }

    }


    public static void main(String[] args) throws Exception {
        String arg;
        int port=12001;
        long expiry=GossipRouter.EXPIRY_TIME;
        long timeout=GossipRouter.GOSSIP_REQUEST_TIMEOUT;
        long routingTimeout=GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT;
        GossipRouter router=null;
        String bind_addr=null;
        boolean jmx=false;

        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if("-port".equals(arg)) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-bindaddress".equals(arg) || "-bind_addr".equals(arg)) {
                bind_addr=args[++i];
                continue;
            }
            if("-expiry".equals(arg)) {
                expiry=Long.parseLong(args[++i]);
                continue;
            }
            if("-timeout".equals(arg)) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            if("-rtimeout".equals(arg)) {
                routingTimeout=Long.parseLong(args[++i]);
                continue;
            }
             if("-jmx".equals(arg)) {
                jmx=true;
                continue;
            }
            help();
            return;
        }
        System.out.println("GossipRouter is starting. Enter quit to exit JVM");

        try {
            router=new GossipRouter(port, bind_addr, expiry, timeout, routingTimeout, jmx);
            router.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
        String quit = "";
        while (!quit.startsWith("quit")) {
			Scanner in = new Scanner(System.in);
			try{
				quit = in.nextLine();
			}
			catch(Exception e){}
		}
        
        router.stop();
        router.cleanup();
    }

    static void help() {
        System.out.println();
        System.out.println("GossipRouter [-port <port>] [-bind_addr <address>] [options]");
        System.out.println("Options: ");
        System.out.println("        -expiry <msecs>   - Time until a gossip cache entry expires.");
        System.out.println("        -timeout <msecs>  - Number of millisecs the router waits to receive");
        System.out.println("                            a gossip request after connection was established;");
        System.out.println("                            upon expiration, the router initiates the routing");
        System.out.println("                            protocol on the connection.");
        System.out.println("        -rtimeout  <mcsec> - Routing timeout");
        System.out.println("        -jmx               - Expose attrs and operations via JMX");
    }


}
