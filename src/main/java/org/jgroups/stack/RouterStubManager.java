
package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Manages a list of RouterStubs (e.g. health checking, reconnecting).
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class RouterStubManager implements Runnable, RouterStub.CloseListener {
    protected final List<RouterStub> stubs=new CopyOnWriteArrayList<>();
    protected final TimeScheduler    timer;
    protected final String           cluster_name;
    protected final Address          local_addr;
    protected final String           logical_name;
    protected final PhysicalAddress  phys_addr;
    protected final long             reconnect_interval; // reconnect interval (ms)
    protected boolean                use_nio=true;       // whether to use TcpClient or NioClient
    protected Future<?>              reconnector_task, heartbeat_task, timeout_checker_task;
    protected final Log              log;
    protected SocketFactory          socket_factory;
    // Sends a heartbeat to the GossipRouter every heartbeat_interval ms (0 disables this)
    protected long                   heartbeat_interval;
    // Max time (ms) with no received message or heartbeat after which the connection to a GossipRouter is closed.
    // Ignored when heartbeat_interval is 0.
    protected long                   heartbeat_timeout;
    protected final Runnable         send_heartbeat=this::sendHeartbeat;
    protected final Runnable         check_timeouts=this::checkTimeouts;

    // Use bounded queues for sending (https://issues.redhat.com/browse/JGRP-2759)") in TCP
    protected boolean                non_blocking_sends;

    // When sending and non_blocking, how many messages to queue max
    protected int                    max_send_queue=128;


    public RouterStubManager(Log log, TimeScheduler timer, String cluster_name, Address local_addr,
                             String logical_name, PhysicalAddress phys_addr, long reconnect_interval) {
        this.log=log != null? log : LogFactory.getLog(RouterStubManager.class);
        this.timer=timer;
        this.cluster_name=cluster_name;
        this.local_addr=local_addr;
        this.logical_name=logical_name;
        this.phys_addr=phys_addr;
        this.reconnect_interval=reconnect_interval;
    }

    public static RouterStubManager emptyGossipClientStubManager(Log log, TimeScheduler timer) {
        return new RouterStubManager(log, timer,null,null,null, null,0L);
    }
    

    public RouterStubManager useNio(boolean flag) {use_nio=flag; return this;}
    public boolean           reconnectorRunning() {return reconnector_task != null && !reconnector_task.isDone();}
    public boolean           heartbeaterRunning() {return heartbeat_task != null && !heartbeat_task.isDone();}
    public boolean           timeouterRunning()   {return timeout_checker_task != null && !timeout_checker_task.isDone();}
    public boolean           nonBlockingSends()          {return non_blocking_sends;}
    public RouterStubManager nonBlockingSends(boolean b) {this.non_blocking_sends=b; return this;}
    public int               maxSendQueue()              {return max_send_queue;}
    public RouterStubManager maxSendQueue(int s)         {this.max_send_queue=s; return this;}


    public RouterStubManager socketFactory(SocketFactory socket_factory) {
        this.socket_factory=socket_factory;
        return this;
    }

    public RouterStubManager heartbeat(long heartbeat_interval, long heartbeat_timeout) {
        if(heartbeat_interval <= 0) {
            // disable heartbeating
            stopHeartbeatTask();
            stopTimeoutChecker();
            stubs.forEach(s -> s.handleHeartbeats(false));
            this.heartbeat_interval=0;
            return this;
        }
        if(heartbeat_interval >= heartbeat_timeout)
            throw new IllegalArgumentException(String.format("heartbeat_interval (%d) must be < than heartbeat_timeout (%d)",
                                                             heartbeat_interval, heartbeat_timeout));
        // enable heartbeating
        this.heartbeat_interval=heartbeat_interval;
        this.heartbeat_timeout=heartbeat_timeout;
        stubs.forEach(s -> s.handleHeartbeats(true));
        startHeartbeatTask();
        startTimeoutChecker();
        return this;
    }


    /**
     * Applies action to all connected RouterStubs
     */
    public void forEach(Consumer<RouterStub> action) {
        stubs.stream().filter(RouterStub::isConnected).forEach(action);
    }

    /**
     * Applies action to a randomly picked RouterStub that's connected
     * @param action
     */
    public void forAny(Consumer<RouterStub> action) {
        RouterStub stub=findRandomConnectedStub();
        if(stub != null)
            action.accept(stub);
    }


    public RouterStub createAndRegisterStub(InetSocketAddress local, InetSocketAddress router_addr) {
        return createAndRegisterStub(local, router_addr, -1);
    }

    public RouterStub createAndRegisterStub(InetSocketAddress local, InetSocketAddress router_addr, int linger) {
        RouterStub stub=new RouterStub(local, router_addr, use_nio, this, socket_factory, linger, non_blocking_sends, max_send_queue)
          .handleHeartbeats(heartbeat_interval > 0);
        this.stubs.add(stub);
        return stub;
    }

    public RouterStub unregisterStub(InetSocketAddress router_addr_sa) {
        RouterStub s=stubs.stream().filter(st -> Objects.equals(st.remote_sa, router_addr_sa)).findFirst().orElse(null);
        if(s != null) {
            s.destroy();
            stubs.remove(s);
        }
        return s;
    }


    public void connectStubs() {
        boolean failed_connect_attempts=false;
        for(RouterStub stub: stubs) {
            if(!stub.isConnected()) {
                try {
                    stub.connect(cluster_name, local_addr, logical_name, phys_addr);
                }
                catch(Exception ex) {
                    failed_connect_attempts=true;
                }
            }
        }
        if(failed_connect_attempts)
            startReconnector();
    }

    
    public void disconnectStubs() {
        stopReconnector();
        for(RouterStub stub: stubs) {
            try {
                stub.disconnect(cluster_name, local_addr);
            }
            catch(Throwable ignored) {
            }
        }
    }
    
    public void destroyStubs() {
        stopReconnector();
        stubs.forEach(RouterStub::destroy);
        stubs.clear();
    }

    public String printStubs() {
        return Util.printListWithDelimiter(stubs, ", ");
    }

    public String printReconnectList() {
        return stubs.stream().filter(s -> !s.isConnected())
          .map(s -> String.format("%s:%d", s.remote_sa.getHostString(), s.remote_sa.getPort()))
          .collect(Collectors.joining(", "));
    }

    public String print() {
        return String.format("Stubs: %s\nReconnect list: %s", printStubs(), printReconnectList());
    }

    public void run() {
        int failed_reconnect_attempts=0;
        for(RouterStub stub: stubs) {
            if(!stub.isConnected()) {
                try {
                    stub.connect(this.cluster_name, this.local_addr, this.logical_name, this.phys_addr);
                    log.debug("%s: re-established connection to GossipRouter %s (group: %s)",
                              local_addr, stub.remote(), this.cluster_name);
                }
                catch(Exception ex) {
                    failed_reconnect_attempts++;
                }
            }
        }
        if(failed_reconnect_attempts == 0)
            stopReconnector();
    }

    @Override
    public void closed(RouterStub stub) {
        try {
            if(log.isDebugEnabled())
                log.debug("%s: GossipRouter %s closed connection; starting reconnector task", local_addr, stub.remote());
            stub.destroy();
        }
        catch(Exception ignored) {

        }
        startReconnector();
    }


    protected synchronized void startReconnector() {
        if(reconnector_task == null || reconnector_task.isDone())
            reconnector_task=timer.scheduleWithFixedDelay(this, reconnect_interval, reconnect_interval, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopReconnector() {
        if(reconnector_task != null)
            reconnector_task.cancel(true);
    }

    protected synchronized void startHeartbeatTask() {
        if(heartbeat_task == null || heartbeat_task.isDone())
            heartbeat_task=timer.scheduleWithFixedDelay(this.send_heartbeat, heartbeat_interval, heartbeat_interval, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopHeartbeatTask() {
        stopTimeoutChecker();
        if(heartbeat_task != null)
            heartbeat_task.cancel(true);
    }

    protected synchronized void startTimeoutChecker() {
        if(timeout_checker_task == null || timeout_checker_task.isDone())
            timeout_checker_task=timer.scheduleWithFixedDelay(this.check_timeouts, heartbeat_timeout, heartbeat_timeout, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopTimeoutChecker() {
        if(timeout_checker_task != null)
            timeout_checker_task.cancel(true);
    }

    protected RouterStub findRandomConnectedStub() {
        RouterStub stub=null;
        while(connectedStubs() > 0) {
            RouterStub tmp=Util.pickRandomElement(stubs);
            if(tmp != null && tmp.isConnected())
                return tmp;
        }
        return stub;
    }

    protected void sendHeartbeat() {
        GossipData hb=new GossipData(GossipType.HEARTBEAT);
        forEach(s -> {
            try {
                s.writeRequest(hb);
            }
            catch(Exception ex) {
                log.error("failed sending heartbeat", ex);
            }
        });
    }

    protected void checkTimeouts() {
        forEach(st -> {
            long timeout=System.currentTimeMillis() - st.lastHeartbeat();
            if(timeout > heartbeat_timeout) {
                log.debug("%s: closed connection to GossipRouter %s as no heartbeat has been received for %s",
                          local_addr, st.remote(),
                          Util.printTime(timeout, TimeUnit.MILLISECONDS));
                st.destroy();
            }
        });
        if(disconnectedStubs())
            startReconnector();
    }

    public int connectedStubs() {
        return (int)stubs.stream().filter(RouterStub::isConnected).count();
    }

    public boolean disconnectedStubs() {
        return stubs.stream().anyMatch(st -> !st.isConnected());
    }

}
