package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.ReceiverAdapter;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

public class RpcDispatcherAnycastServerObject extends ReceiverAdapter {
    int i=0;
    private final Channel c;
    private final RpcDispatcher d;

    public RpcDispatcherAnycastServerObject(Channel channel) throws ChannelException {
        c=channel;
        d=new RpcDispatcher(c, this, this, this);
    }

    public void doSomething() {
        System.out.println("doSomething invoked on " + c.getLocalAddress() + ".  i = " + i);
        i++;
        System.out.println("Now i = " + i);
    }

    public void callRemote(boolean useAnycast, boolean excludeSelf) {
        // we need to copy the vector, otherwise the modification below will throw an exception because the underlying
        // vector is unmodifiable
        Vector<Address> v=new Vector<Address>(c.getView().getMembers());
        if(excludeSelf) v.remove(c.getLocalAddress());
        RspList rsps=d.callRemoteMethods(v, "doSomething", new Object[]{}, new Class[]{}, GroupRequest.GET_ALL, 10000, useAnycast);
        Map.Entry entry;
        for(Iterator it=rsps.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            Address member=(Address)entry.getKey();
            Rsp rsp=(Rsp)entry.getValue();
            if(!rsp.wasReceived())
                throw new RuntimeException("response from " + member + " was not received, rsp=" + rsp);
        }

    }

    public void shutdown() {
        c.close();
    }


}