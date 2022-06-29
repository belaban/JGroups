
package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Manages a list of RouterStubs (e.g. health checking, reconnecting etc.
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class RouterStubManager implements Runnable, RouterStub.CloseListener {
    protected volatile List<RouterStub> stubs;
    protected final TimeScheduler       timer;
    protected final String              cluster_name;
    protected final Address             local_addr;
    protected final String              logical_name;
    protected final PhysicalAddress     phys_addr;
    protected final long                interval;      // reconnect interval (ms)
    protected boolean                   use_nio=true;  // whether to use RouterStubTcp or RouterStubNio
    protected Future<?>                 reconnector_task;
    protected final Log                 log;
    private SocketFactory               socket_factory;


    public RouterStubManager(Protocol owner, String cluster_name, Address local_addr,
                             String logical_name, PhysicalAddress phys_addr, long interval) {
        this.stubs = new ArrayList<>();
        this.log = LogFactory.getLog(owner.getClass());
        this.timer = owner.getTransport().getTimer();
        this.cluster_name=cluster_name;
        this.local_addr=local_addr;
        this.logical_name=logical_name;
        this.phys_addr=phys_addr;
        this.interval = interval;
    }

    public static RouterStubManager emptyGossipClientStubManager(Protocol p) {
        return new RouterStubManager(p,null,null,null, null,0L);
    }
    

    public RouterStubManager useNio(boolean flag) {use_nio=flag; return this;}


    public RouterStubManager socketFactory(SocketFactory socket_factory) {
        this.socket_factory=socket_factory;
        return this;
    }

    /**
     * Applies action to all RouterStubs that are connected
     * @param action
     */
    public void forEach(Consumer<RouterStub> action) {
        stubs.stream().filter(RouterStub::isConnected).forEach(action);
    }

    /**
     * Applies action to a randomly picked RouterStub that's connected
     * @param action
     */
    public void forAny(Consumer<RouterStub> action) {
        while(!stubs.isEmpty()) {
            RouterStub stub=Util.pickRandomElement(stubs);
            if(stub != null && stub.isConnected()) {
                action.accept(stub);
                return;
            }
        }
    }


    public RouterStub createAndRegisterStub(InetSocketAddress local, InetSocketAddress router_addr) {
        RouterStub stub=new RouterStub(local, router_addr, use_nio, this, socket_factory);
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
                    stub.connect(cluster_name,local_addr,logical_name,phys_addr);
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
        stopReconnector();
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
                    log.debug("%s: re-established connection to %s successfully for group %s",
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
            stub.destroy();
        }
        catch(Exception ignored) {

        }
        startReconnector();
    }


    protected synchronized void startReconnector() {
        if(reconnector_task == null || reconnector_task.isDone())
            reconnector_task=timer.scheduleWithFixedDelay(this, interval, interval, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopReconnector() {
        if(reconnector_task != null)
            reconnector_task.cancel(true);
    }



}
