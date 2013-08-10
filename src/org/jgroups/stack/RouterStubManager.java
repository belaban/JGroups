
package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.TimeScheduler;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Manages a list of RouterStubs (e.g. health checking, reconnecting etc.
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class RouterStubManager implements RouterStub.ConnectionListener {

    @GuardedBy("reconnectorLock")
    private final ConcurrentMap<RouterStub,Future<?>> futures=new ConcurrentHashMap<RouterStub,Future<?>>();
    private final List<RouterStub> stubs;
    
    private final Protocol owner;
    private final TimeScheduler timer;
    private final String channelName;
    private final Address logicalAddress;
    private final long interval;

    protected final Log log;

    public RouterStubManager(Protocol owner, String channelName, Address logicalAddress, long interval) {
        this.owner = owner;
        this.stubs = new CopyOnWriteArrayList<RouterStub>();
        this.log = LogFactory.getLog(owner.getClass());     
        this.timer = owner.getTransport().getTimer();
        this.channelName = channelName;
        this.logicalAddress = logicalAddress;
        this.interval = interval;
    }
    
    private RouterStubManager(Protocol p) {
       this(p,null,null,0L);
    }
    
    public List<RouterStub> getStubs(){
        return stubs;
    }
    
    public RouterStub createAndRegisterStub(String routerHost, int routerPort, InetAddress bindAddress) {
        RouterStub s = new RouterStub(routerHost,routerPort,bindAddress,this);
        unregisterAndDestroyStub(s);
        stubs.add(s);   
        return s;
    }
    
    public void registerStub(RouterStub s) {        
        unregisterAndDestroyStub(s);
        stubs.add(s);           
    }
    
    public RouterStub unregisterStub(final RouterStub stub) {
        if(stub == null)
            throw new IllegalArgumentException("Cannot remove null stub");
        RouterStub found=null;
        for (RouterStub s: stubs) {
            if (s.equals(stub)) {
                found=s;
                break;
            }
        }
        if(found != null)
            stubs.remove(found);
        return found;
    }
    
    public boolean unregisterAndDestroyStub(final RouterStub stub) {
        RouterStub unregisteredStub = unregisterStub(stub);
        if(unregisteredStub != null) {
            unregisteredStub.destroy();
            return true;
        }
        return false;
    }
    
    public void disconnectStubs() {
        for (RouterStub stub : stubs) {
            try {
                stub.disconnect(channelName, logicalAddress);                
            } catch (Exception e) {
            }
        }       
    }
    
    public void destroyStubs() {
        for (RouterStub s : stubs) {
            stopReconnecting(s);
            s.destroy();            
        }
        stubs.clear();
    }

    public void startReconnecting(final RouterStub stub) {
        Future<?> f = futures.remove(stub);
        if (f != null)
            f.cancel(true);

        final Runnable reconnector = new Runnable() {
            public void run() {
                try {
                    if (log.isTraceEnabled()) log.trace("Reconnecting " + stub);
                    String logical_name = org.jgroups.util.UUID.get(logicalAddress);
                    PhysicalAddress physical_addr = (PhysicalAddress) owner.down(new Event(
                      Event.GET_PHYSICAL_ADDRESS, logicalAddress));
                    List<PhysicalAddress> physical_addrs = Arrays.asList(physical_addr);
                    stub.connect(channelName, logicalAddress, logical_name, physical_addrs);
                    if (log.isTraceEnabled()) log.trace("Reconnected " + stub);
                } catch (Throwable ex) {
                    if (log.isWarnEnabled())
                        log.warn("failed reconnecting stub to GR at "+ stub.getGossipRouterAddress() + ": " + ex);
                }
            }

            public String toString() {
                return RouterStubManager.class.getSimpleName() + ": Reconnector";
            }
        };
        f = timer.scheduleWithFixedDelay(reconnector, 0, interval, TimeUnit.MILLISECONDS);
        futures.putIfAbsent(stub, f);
    }

    public void stopReconnecting(final RouterStub stub) {
        Future<?> f = futures.get(stub);
        if (f != null) {
            f.cancel(true);
            futures.remove(stub);
        }

        final Runnable pinger = new Runnable() {
            public void run() {
                try {
                    if(log.isTraceEnabled()) log.trace("Pinging " + stub);
                    stub.checkConnection();
                    if(log.isTraceEnabled()) log.trace("Pinged " + stub);
                } catch (Throwable ex) {
                    if (log.isWarnEnabled())
                        log.warn("failed pinging stub, GR at " + stub.getGossipRouterAddress()+ ": " + ex);
                }
            }

            public String toString() {
                return RouterStubManager.class.getSimpleName() + ": Pinger";
            }
        };
        f = timer.scheduleWithFixedDelay(pinger, 1000, interval, TimeUnit.MILLISECONDS);
        futures.putIfAbsent(stub, f);
    }
   

    public void connectionStatusChange(RouterStub stub, RouterStub.ConnectionStatus newState) {
        if (newState == RouterStub.ConnectionStatus.CONNECTION_BROKEN) {
            stub.interrupt();
            stub.destroy();
            startReconnecting(stub);
        } else if (newState == RouterStub.ConnectionStatus.CONNECTED) {
            stopReconnecting(stub);
        } else if (newState == RouterStub.ConnectionStatus.DISCONNECTED) {
            // wait for disconnect ack;
            try {
                stub.join(interval);
            } catch (InterruptedException e) {
            }
        }
    }
    
    public static RouterStubManager emptyGossipClientStubManager(Protocol p) {
        return new RouterStubManager(p);
    }
}
