// $Id: RpcDispatcher.java,v 1.27 2006/12/11 08:24:13 belaban Exp $

package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;




/**
 * This class allows a programmer to invoke remote methods in all (or single) 
 * group members and optionally wait for the return value(s). 
 * An application will typically create a channel and layer the
 * RpcDispatcher building block on top of it, which allows it to 
 * dispatch remote methods (client role) and at the same time be 
 * called by other members (server role).
 * This class is derived from MessageDispatcher. 
*  Is the equivalent of RpcProtocol on the application rather than protocol level.
 * @author Bela Ban
 */
public class RpcDispatcher extends MessageDispatcher implements ChannelListener {
    protected Object        server_obj=null;
    /** Marshaller to marshall requests at the caller and unmarshal requests at the receiver(s) */
    protected Marshaller    req_marshaller=null;

    /** Marshaller to marshal responses at the receiver(s) and unmarshal responses at the caller */
    protected Marshaller    rsp_marshaller=null;
    protected final List    additionalChannelListeners=new ArrayList();
    protected MethodLookup  method_lookup=null;


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj) {
        super(channel, l, l2);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection) {
        super(channel, l, l2, deadlock_detection);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }

    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection, boolean concurrent_processing) {
        super(channel, l, l2, deadlock_detection, concurrent_processing);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }



    public RpcDispatcher(PullPushAdapter adapter, Serializable id,
                         MessageListener l, MembershipListener l2, Object server_obj) {
        super(adapter, id, l, l2);

        // Fixes bug #804956
        // channel.setChannelListener(this);
        if(this.adapter != null) {
            Transport t=this.adapter.getTransport();
            if(t != null && t instanceof Channel) {
                ((Channel)t).addChannelListener(this);
            }
        }

        this.server_obj=server_obj;
    }


    public interface Marshaller {
        byte[] objectToByteBuffer(Object obj) throws Exception;
        Object objectFromByteBuffer(byte[] buf) throws Exception;
    }


    public String getName() {return "RpcDispatcher";}

    public Marshaller getRequestMarshaller()             {return req_marshaller;}

    public void setRequestMarshaller(Marshaller m) {
        this.req_marshaller=m;
    }

    public Marshaller getResponseMarshaller()             {return rsp_marshaller;}

    public void setResponseMarshaller(Marshaller m) {
        this.rsp_marshaller=m;
        if(corr != null)
            corr.setMarshaller(m);
    }

    public Marshaller getMarshaller() {return req_marshaller;}
    
    public void setMarshaller(Marshaller m) {req_marshaller=m;}

    public Object getServerObject() {return server_obj;}

    public void setServerObject(Object server_obj) {
        this.server_obj=server_obj;
    }

    public MethodLookup getMethodLookup() {
        return method_lookup;
    }

    public void setMethodLookup(MethodLookup method_lookup) {
        this.method_lookup=method_lookup;
    }


    public RspList castMessage(Vector dests, Message msg, int mode, long timeout) {
        if(log.isErrorEnabled()) log.error("this method should not be used with " +
                    "RpcDispatcher, but MessageDispatcher. Returning null");
        return null;
    }

    public Object sendMessage(Message msg, int mode, long timeout) throws TimeoutException, SuspectedException {
        if(log.isErrorEnabled()) log.error("this method should not be used with " +
                    "RpcDispatcher, but MessageDispatcher. Returning null");
        return null;
    }


    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     Class[] types, int mode, long timeout) {
        return callRemoteMethods(dests, method_name, args, types, mode, timeout, false);
    }


    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     Class[] types, int mode, long timeout, boolean use_anycasting) {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, mode, timeout, use_anycasting);
    }


    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     String[] signature, int mode, long timeout) {
        return callRemoteMethods(dests, method_name, args, signature, mode, timeout, false);
    }


    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     String[] signature, int mode, long timeout, boolean use_anycasting) {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethods(dests, method_call, mode, timeout, use_anycasting);
    }


    public RspList callRemoteMethods(Vector dests, MethodCall method_call, int mode, long timeout) {
        return callRemoteMethods(dests, method_call, mode, timeout, false);
    }

    public RspList callRemoteMethods(Vector dests, MethodCall method_call, int mode, long timeout, boolean use_anycasting) {
        if(dests != null && dests.size() == 0) {
            // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuffer("destination list of ").append(method_call.getName()).
                          append("() is empty: no need to send message"));
            return new RspList();
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuffer("dests=").append(dests).append(", method_call=").append(method_call).
                      append(", mode=").append(mode).append(", timeout=").append(timeout));

        byte[] buf;
        try {
            buf=req_marshaller != null? req_marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            // if(log.isErrorEnabled()) log.error("exception", e);
            // we will change this in 2.4 to add the exception to the signature
            // (see http://jira.jboss.com/jira/browse/JGRP-193). The reason for a RTE is that we cannot change the
            // signature in 2.3, otherwise 2.3 would be *not* API compatible to prev releases
            throw new RuntimeException("failure to marshal argument(s)", e);
        }

        Message msg=new Message(null, null, buf);
        RspList  retval=super.castMessage(dests, msg, mode, timeout, use_anycasting);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }



    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   Class[] types, int mode, long timeout) throws Throwable {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   String[] signature, int mode, long timeout) throws Throwable {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    public Object callRemoteMethod(Address dest, MethodCall method_call, int mode, long timeout) throws Throwable {
        byte[]   buf=null;
        Message  msg=null;
        Object   retval=null;

        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + method_call + ", mode=" + mode + ", timeout=" + timeout);

        buf=req_marshaller != null? req_marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
        msg=new Message(dest, null, buf);
        retval=super.sendMessage(msg, mode, timeout);
        if(log.isTraceEnabled()) log.trace("retval: " + retval);
        if(retval instanceof Throwable)
            throw (Throwable)retval;
        return retval;
    }


    protected void correlatorStarted() {
        if(corr != null)
           corr.setMarshaller(rsp_marshaller);
    }


    /**
     * Message contains MethodCall. Execute it against *this* object and return result.
     * Use MethodCall.invoke() to do this. Return result.
     */
    public Object handle(Message req) {
        Object      body=null;
        MethodCall  method_call;

        if(server_obj == null) {
            if(log.isErrorEnabled()) log.error("no method handler is registered. Discarding request.");
            return null;
        }

        if(req == null || req.getLength() == 0) {
            if(log.isErrorEnabled()) log.error("message or message buffer is null");
            return null;
        }

        try {
            body=req_marshaller != null? req_marshaller.objectFromByteBuffer(req.getBuffer()) : req.getObject();
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("exception marshalling object", e);
            return e;
        }

        if(body == null || !(body instanceof MethodCall)) {
            if(log.isErrorEnabled()) log.error("message does not contain a MethodCall object");
            return null;
        }

        method_call=(MethodCall)body;

        try {
            if(log.isTraceEnabled())
                log.trace("[sender=" + req.getSrc() + "], method_call: " + method_call);

            if(method_call.getMode() == MethodCall.ID) {
                if(method_lookup == null)
                    throw new Exception("MethodCall uses ID=" + method_call.getId() + ", but method_lookup has not been set");
                Method m=method_lookup.findMethod(method_call.getId());
                if(m == null)
                    throw new Exception("no method foudn for " + method_call.getId());
                method_call.setMethod(m);
            }
            
            return method_call.invoke(server_obj);
        }
        catch(Throwable x) {
            return x;
        }
    }

    /**
     * Add a new channel listener to be notified on the channel's state change.
     *
     * @return true if the listener was added or false if the listener was already in the list.
     */
    public boolean addChannelListener(ChannelListener l) {

        synchronized(additionalChannelListeners) {
            if (additionalChannelListeners.contains(l)) {
               return false;
            }
            additionalChannelListeners.add(l);
            return true;
        }
    }


    /**
     *
     * @return true if the channel was removed indeed.
     */
    public boolean removeChannelListener(ChannelListener l) {

        synchronized(additionalChannelListeners) {
            return additionalChannelListeners.remove(l);
        }
    }



    /* --------------------- Interface ChannelListener ---------------------- */

    public void channelConnected(Channel channel) {

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelConnected(channel);
                }
                catch(Throwable t) {
                    log.warn("channel listener failed", t);
                }
            }
        }
    }

    public void channelDisconnected(Channel channel) {

        stop();

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelDisconnected(channel);
                }
                catch(Throwable t) {
                    log.warn("channel listener failed", t);
                }
            }
        }
    }

    public void channelClosed(Channel channel) {

        stop();

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelClosed(channel);
                }
                catch(Throwable t) {
                    log.warn("channel listener failed", t);
                }
            }
        }
    }

    public void channelShunned() {

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelShunned();
                }
                catch(Throwable t) {
                    log.warn("channel listener failed", t);
                }
            }
        }
    }

    public void channelReconnected(Address new_addr) {
        if(log.isTraceEnabled())
            log.trace("channel has been rejoined, old local_addr=" + local_addr + ", new local_addr=" + new_addr);
        this.local_addr=new_addr;
        start();

        synchronized(additionalChannelListeners) {
            for(Iterator i = additionalChannelListeners.iterator(); i.hasNext(); ) {
                ChannelListener l = (ChannelListener)i.next();
                try {
                    l.channelReconnected(new_addr);
                }
                catch(Throwable t) {
                   log.warn("channel listener failed", t);
                }
            }
        }
    }
    /* ----------------------------------------------------------------------- */

}
