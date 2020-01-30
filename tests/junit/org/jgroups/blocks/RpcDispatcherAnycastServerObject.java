package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Receiver;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RpcDispatcherAnycastServerObject implements Receiver {
    int i=0;
    private final JChannel c;
    private final RpcDispatcher d;

    public RpcDispatcherAnycastServerObject(JChannel channel) throws Exception {
        c=channel;
        d=new RpcDispatcher(c, this).setReceiver(this);
    }

    public void doSomething() {
        // System.out.println("doSomething invoked on " + c.getLocalAddress() + ".  i = " + i);
        i++;
        // System.out.println("Now i = " + i);
    }

    public void callRemote(boolean useAnycast, boolean excludeSelf) throws Exception {
        // we need to copy the vector, otherwise the modification below will throw an exception because the underlying
        // vector is unmodifiable
        List<Address> v=new ArrayList<>(c.getView().getMembers());
        if(excludeSelf) v.remove(c.getAddress());
        RspList rsps=d.callRemoteMethods(v, "doSomething", new Object[]{}, new Class[]{}, new RequestOptions(ResponseMode.GET_ALL, 20000, useAnycast));
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