/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jgroups.stack;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.TimeScheduler;

public class RouterStubManager implements RouterStub.ConnectionListener {

    @GuardedBy("reconnectorLock")
    private final Map<InetSocketAddress, Future<?>> reconnectFutures = new HashMap<InetSocketAddress, Future<?>>();
    private final Lock reconnectorLock = new ReentrantLock();    
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
    
    public RouterStub createStub(String routerHost, int routerPort, InetAddress bindAddress) {
        RouterStub s = new RouterStub(routerHost,routerPort,bindAddress,this);
        stubs.add(s);   
        return s;
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
        reconnectorLock.lock();
        try {
            Future<?> reconnectorFuture = reconnectFutures.get(stub.getGossipRouterAddress());
            if (reconnectorFuture == null || reconnectorFuture.isDone()) {
                final Runnable reconnector = new Runnable() {
                    public void run() {
                        try {
                            if (log.isTraceEnabled()) {
                                log.trace("Reconnecting " + stub);
                            }

                            String logical_name = org.jgroups.util.UUID.get(logicalAddress);
                            PhysicalAddress physical_addr = (PhysicalAddress) owner.down(new Event(
                                            Event.GET_PHYSICAL_ADDRESS, logicalAddress));
                            List<PhysicalAddress> physical_addrs = Arrays.asList(physical_addr);
                            stub.connect(channelName, logicalAddress, logical_name, physical_addrs);
                            if (log.isTraceEnabled()) {
                                log.trace("Reconnected " + stub);
                            }

                        } catch (Throwable ex) {
                            if (log.isWarnEnabled())
                                log.warn("failed reconnecting stub to GR at " + stub.getGossipRouterAddress() + ": " + ex);
                        }
                    }
                };
                reconnectorFuture = timer.scheduleWithFixedDelay(reconnector, 0, interval,TimeUnit.MILLISECONDS);
                reconnectFutures.put(stub.getGossipRouterAddress(), reconnectorFuture);
            }
        } finally {
            reconnectorLock.unlock();
        }
    }

    public void stopReconnecting(final RouterStub stub) {
        reconnectorLock.lock();
        InetSocketAddress address = stub.getGossipRouterAddress();
        try {
            Future<?> reconnectorFuture = reconnectFutures.get(address);
            if (reconnectorFuture != null) {
                reconnectorFuture.cancel(true);
                reconnectFutures.remove(address);
            }
        } finally {
            reconnectorLock.unlock();
        }
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
