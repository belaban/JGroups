package org.jgroups.protocols;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.util.Util;


/**
 * This is a central executor service where each request is sent to the coordinator
 * for either a task or a current waiting thread.
 * 
 * @author wburns
 * @since 2.12.0
 */
@Experimental
public class CENTRAL_EXECUTOR extends Executing {

    @Property(description="Number of backups to the coordinator.  Queue State gets replicated to these nodes as well")
    protected int num_backups=1;

    protected Address coord;

    @ManagedAttribute
    protected boolean is_coord;

    protected final List<Address> backups=new ArrayList<Address>();


    public CENTRAL_EXECUTOR() {
        super();
    }

    public Address getCoord() {
        return coord;
    }

    public boolean isCoord() {
        return is_coord;
    }

    @ManagedAttribute
    public String getCoordinator() {
        return coord != null? coord.toString() : "n/a";
    }

    public int getNumberOfBackups() {
        return num_backups;
    }

    public void setNumberOfBackups(int num_backups) {
        this.num_backups=num_backups;
    }

    @ManagedAttribute
    public String getBackups() {
        return backups != null? backups.toString() : null;
    }

    public void handleView(View view) {
        super.handleView(view);
        if(view.size() > 0) {
            coord=view.getMembers().firstElement();
            is_coord=coord.equals(local_addr);
            if(log.isDebugEnabled())
                log.debug("local_addr=" + local_addr + ", coord=" + coord + ", is_coord=" + is_coord);
        }
        
        // TODO: need to send consumer/task queue to the new coordinator if they went down

        if(num_backups > 0) {
            if (is_coord) {
                List<Address> new_backups=Util.pickNext(view.getMembers(), local_addr, num_backups);
                List<Address> new_members=null;
                synchronized(backups) {
                    if(!backups.equals(new_backups)) {
                        new_members=new ArrayList<Address>(new_backups);
                        new_members.removeAll(backups);
                        backups.clear();
                        backups.addAll(new_backups);
                    }
                }
                
                if(new_members != null && !new_members.isEmpty())
                    copyQueueTo(new_members);
            }
            // We keep what backups we have ourselves, so that when we become
            // the coordinator we don't update them again since we can't send
            // updates multiple times
            else {
                List<Address> possiblebackups = Util.pickNext(view.getMembers(), 
                    coord, num_backups);
                
                boolean foundMyself = false;
                List<Address> myBackups = new ArrayList<Address>();
                for (Address backup : possiblebackups) {
                    if (foundMyself) {
                        myBackups.add(backup);
                    }
                    else if (backup.equals(local_addr)) {
                        foundMyself = true;
                    }
                }
                
                synchronized (backups) {
                    backups.clear();
                    backups.addAll(myBackups);
                }
            }
        }
    }

    protected void updateBackups(Type type, Owner obj) {
        synchronized(backups) {
            for(Address backup: backups)
                sendRequest(backup, type, obj.getRequestId(), obj.getAddress());
        }
    }
    
    protected void copyQueueTo(List<Address> new_joiners) {
        Set<Owner> copyRequests;
        Set<Owner> copyConsumers;
        
        _consumerLock.lock();
        try {
            copyRequests = new HashSet<Owner>(_runRequests);
            copyConsumers = new HashSet<Owner>(_consumersAvailable);
        }
        finally {
            _consumerLock.unlock();
        }

        if(log.isTraceEnabled())
            log.trace("copying queue to " + new_joiners);
        for(Address joiner: new_joiners) {
            for(Owner address: copyRequests) {
                sendRequest(joiner, Type.CREATE_RUN_REQUEST, 
                    address.getRequestId(), address.getAddress());
            }
            
            for(Owner address: copyConsumers) {
                sendRequest(joiner, Type.CREATE_CONSUMER_READY, 
                    address.getRequestId(), address.getAddress());
            }
        }
    }

    // @see org.jgroups.protocols.Executing#sendToCoordinator(org.jgroups.protocols.Executing.Type, java.lang.Object)
    @Override
    protected void sendToCoordinator(Type type, Object value) {
        sendRequest(coord, type, (short)-1, value);
    }

    // @see org.jgroups.protocols.Executing#sendNewRunRequest(org.jgroups.Address)
    @Override
    protected void sendNewRunRequest(Owner sender) {
        if(is_coord)
            updateBackups(Type.CREATE_RUN_REQUEST, sender);
    }

    // @see org.jgroups.protocols.Executing#sendRemoveRunRequest(org.jgroups.Address)
    @Override
    protected void sendRemoveRunRequest(Owner sender) {
        if(is_coord)
            updateBackups(Type.DELETE_RUN_REQUEST, sender);
    }

    // @see org.jgroups.protocols.Executing#sendNewConsumerRequest(org.jgroups.Address)
    @Override
    protected void sendNewConsumerRequest(Owner sender) {
        if(is_coord)
            updateBackups(Type.CREATE_CONSUMER_READY, sender);
    }

    // @see org.jgroups.protocols.Executing#sendRemoveConsumerRequest(org.jgroups.Address)
    @Override
    protected void sendRemoveConsumerRequest(Owner sender) {
        if(is_coord)
            updateBackups(Type.DELETE_CONSUMER_READY, sender);
    }
}
