
package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Manages a list of RouterStubs (e.g. health checking, reconnecting etc.
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class RouterStubManager implements Runnable, RouterStub.CloseListener {
    @GuardedBy("reconnectorLock")
    protected final ConcurrentMap<RouterStub,Future<?>> futures=new ConcurrentHashMap<>();

    // List of currently connected RouterStubs
    protected volatile List<RouterStub>                 stubs;

    // List of destinations that the reconnect task needs to create and connect
    protected volatile Set<Target>                      reconnect_list;

    protected final Protocol                            owner;
    protected final TimeScheduler                       timer;
    protected final String                              cluster_name;
    protected final Address                             local_addr;
    protected final String                              logical_name;
    protected final PhysicalAddress                     phys_addr;
    protected final long                                interval;      // reconnect interval (ms)
    protected boolean                                   use_nio=true;  // whether to use RouterStubTcp or RouterStubNio
    protected Future<?>                                 reconnector_task;
    protected final Log                                 log;


    public RouterStubManager(Protocol owner, String cluster_name, Address local_addr,
                             String logical_name, PhysicalAddress phys_addr, long interval) {
        this.owner = owner;
        this.stubs = new ArrayList<>();
        this.reconnect_list=new HashSet<>();
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


    public RouterStub createAndRegisterStub(IpAddress local, IpAddress router_addr) {
        RouterStub stub=new RouterStub(local, router_addr, use_nio, this);
        RouterStub old_stub=unregisterStub(router_addr);
        if(old_stub != null)
            old_stub.destroy();
        add(stub);
        return stub;
    }


    public RouterStub unregisterStub(IpAddress router_addr) {
        RouterStub stub=find(router_addr);
        if(stub != null)
            remove(stub);
        return stub;
    }


    public void connectStubs() {
        for(RouterStub stub : stubs) {
            try {
                if(!stub.isConnected())
                    stub.connect(cluster_name, local_addr, logical_name, phys_addr);
            }
            catch (Throwable e) {
                moveStubToReconnects(stub);
            }
        }
    }

    
    public void disconnectStubs() {
        stopReconnector();
        for(RouterStub stub : stubs) {
            try {
                stub.disconnect(cluster_name, local_addr);
            }
            catch (Throwable e) {
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
        return Util.printListWithDelimiter(reconnect_list, ", ");
    }

    public String print() {
        return String.format("Stubs: %s\nReconnect list: %s", printStubs(), printReconnectList());
    }

    public void run() {
        if(reconnect_list.removeIf(this::reconnect) && reconnect_list.isEmpty())
            stopReconnector();
    }

    @Override
    public void closed(RouterStub stub) {
        moveStubToReconnects(stub);
    }

    protected boolean reconnect(Target target) {
        RouterStub stub=new RouterStub(target.bind_addr, target.router_addr, this.use_nio, this).receiver(target.receiver);
        if(!add(stub))
            return false;
        try {
            stub.connect(this.cluster_name, this.local_addr, this.logical_name, this.phys_addr);
            log.debug("%s: re-established connection to %s successfully for group %s",
                      local_addr, stub.remote(), this.cluster_name);
            return true;
        }
        catch(Throwable t) {
            remove(stub);
            return false;
        }
    }

    protected void moveStubToReconnects(RouterStub stub) {
        if(stub == null) return;
        remove(stub);
        if(add(new Target(stub.local(), stub.remote(), stub.receiver()))) {
            log.debug("%s: connection to %s closed, trying to re-establish connection", local_addr, stub.remote());
            startReconnector();
        }
    }

    protected boolean add(RouterStub stub) {
        if(stub == null) return false;
        List<RouterStub> new_stubs=new ArrayList<>(stubs);
        boolean retval=!new_stubs.contains(stub) && new_stubs.add(stub);
        this.stubs=new_stubs;
        return retval;
    }


    protected boolean add(Target target) {
        if(target == null) return false;
        Set<Target> new_set=new HashSet<>(reconnect_list);
        if(new_set.add(target)) {
            this.reconnect_list=new_set;
            return true;
        }
        return false;
    }

    protected boolean remove(RouterStub stub) {
        if(stub == null) return false;
        stub.destroy();
        List<RouterStub> new_stubs=new ArrayList<>(stubs);
        boolean retval=new_stubs.remove(stub);
        this.stubs=new_stubs;
        return retval;
    }

    protected boolean remove(Target target) {
        if(target == null) return false;
        Set<Target> new_set=new HashSet<>(reconnect_list);
        if(new_set.remove(target)) {
            this.reconnect_list=new_set;
            return true;
        }
        return false;
    }


    protected RouterStub find(IpAddress router_addr) {
        for(RouterStub stub: stubs) {
            IpAddress addr=stub.gossipRouterAddress();
            if(Objects.equals(addr, router_addr))
                return stub;
        }
        return null;
    }

    protected synchronized void startReconnector() {
        if(reconnector_task == null || reconnector_task.isDone())
            reconnector_task=timer.scheduleWithFixedDelay(this, interval, interval, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopReconnector() {
        if(reconnector_task != null)
            reconnector_task.cancel(true);
    }







    protected static class Target implements Comparator<Target>, Serializable {
        private static final long serialVersionUID = 1L;

        protected final IpAddress               bind_addr, router_addr;
        protected final RouterStub.StubReceiver receiver;

        public Target(IpAddress bind_addr, IpAddress router_addr, RouterStub.StubReceiver receiver) {
            this.bind_addr=bind_addr;
            this.router_addr=router_addr;
            this.receiver=receiver;
        }

        @Override
        public int compare(Target o1, Target o2) {
            return o1.router_addr.compareTo(o2.router_addr);
        }

        public int hashCode() {
            return router_addr.hashCode();
        }

        public boolean equals(Object obj) {
            return compare(this, (Target)obj) == 0;
        }

        public String toString() {
            return String.format("%s -> %s", bind_addr, router_addr);
        }
    }

}
