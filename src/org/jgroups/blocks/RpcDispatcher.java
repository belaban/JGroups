
package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.util.*;

import java.lang.reflect.Method;
import java.util.*;


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
    protected final List<ChannelListener> additionalChannelListeners=new ArrayList<ChannelListener>();
    protected MethodLookup  method_lookup=null;


    public RpcDispatcher() {
    }


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj) {
        super(channel, l, l2);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }





    public interface Marshaller {
        /**
         * Marshals the object into a byte[] buffer and returns a Buffer with a ref to the underlying byte[] buffer,
         * offset and length.<br/>
         * <em>
         * Note that the underlying byte[] buffer must not be changed as this would change the buffer of a message which
         * potentially can get retransmitted, and such a retransmission would then carry a ref to a changed byte[] buffer !
         * </em>
         * @param obj
         * @return
         * @throws Exception
         */
        Buffer objectToBuffer(Object obj) throws Exception;

        Object objectFromBuffer(byte[] buf, int offset, int length) throws Exception;
    }




    public static String getName() {return "RpcDispatcher";}

    public Marshaller getRequestMarshaller()             {return req_marshaller;}

    public void setRequestMarshaller(Marshaller m) {
        this.req_marshaller=m;
    }

    public Marshaller getResponseMarshaller()             {return rsp_marshaller;}

    public void setResponseMarshaller(Marshaller m) {
        this.rsp_marshaller=m;

        if(corr != null)
            corr.setMarshaller(this.rsp_marshaller);
    }

    public Marshaller getMarshaller() {return req_marshaller;}
    
    public void setMarshaller(Marshaller m) {setRequestMarshaller(m);}

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




    public RspList callRemoteMethods(Collection<Address> dests, String method_name, Object[] args,
                                     Class[] types, RequestOptions options) {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, options);
    }



    /**
     * Invokes a method in all members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, the method will be invoked on all cluster members
     * @param method_call The method (plus args) to be invoked
     * @param options A collection of call options, e.g. sync versus async, timeout etc
     * @return RspList A list of return values and flags (suspected, not received) per member
     * @since 2.9
     */
    public RspList callRemoteMethods(Collection<Address> dests, MethodCall method_call, RequestOptions options) {
        if(dests != null && dests.isEmpty()) { // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("destination list of ").append(method_call.getName()).
                        append("() is empty: no need to send message"));
            return RspList.EMPTY_RSP_LIST;
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("dests=").append(dests).append(", method_call=").append(method_call).
                    append(", options=").append(options));

        Object buf;
        try {
            buf=req_marshaller != null? req_marshaller.objectToBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            // if(log.isErrorEnabled()) log.error("exception", e);
            // we will change this in 3.0 to add the exception to the signature
            // (see http://jira.jboss.com/jira/browse/JGRP-193). The reason for a RTE is that we cannot change the
            // signature in 2.3, otherwise 2.3 would be *not* API compatible to prev releases
            throw new RuntimeException("failure to marshal argument(s)", e);
        }

        Message msg=new Message();
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);

        msg.setFlag(options.getFlags());
        if(options.getScope() > 0)
            msg.setScope(options.getScope());

        RspList retval=super.castMessage(dests, msg, options);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }



    public NotifyingFuture<RspList> callRemoteMethodsWithFuture(Collection<Address> dests, MethodCall method_call, RequestOptions options) {
        if(dests != null && dests.isEmpty()) { // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("destination list of ").append(method_call.getName()).
                        append("() is empty: no need to send message"));
            return new NullFuture<RspList>(RspList.EMPTY_RSP_LIST);
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("dests=").append(dests).append(", method_call=").append(method_call).
                    append(", options=").append(options));

        Object buf;
        try {
            buf=req_marshaller != null? req_marshaller.objectToBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            // if(log.isErrorEnabled()) log.error("exception", e);
            // we will change this in 2.4 to add the exception to the signature
            // (see http://jira.jboss.com/jira/browse/JGRP-193). The reason for a RTE is that we cannot change the
            // signature in 2.3, otherwise 2.3 would be *not* API compatible to prev releases
            throw new RuntimeException("failure to marshal argument(s)", e);
        }

        Message msg=new Message();
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);
        msg.setFlag(options.getFlags());
        if(options.getScope() > 0)
            msg.setScope(options.getScope());
        
        NotifyingFuture<RspList>  retval=super.castMessageWithFuture(dests, msg, options);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }


    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   Class[] types, RequestOptions options) throws Throwable {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethod(dest, method_call, options);
    }


    public Object callRemoteMethod(Address dest, MethodCall call, RequestOptions options) throws Throwable {
        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + call + ", options=" + options);

        Object buf=req_marshaller != null? req_marshaller.objectToBuffer(call) : Util.objectToByteBuffer(call);
        Message msg=new Message(dest, null, null);
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);
        msg.setFlag(options.getFlags());
        if(options.getScope() > 0)
            msg.setScope(options.getScope());

        Object retval=super.sendMessage(msg, options);
        if(log.isTraceEnabled()) log.trace("retval: " + retval);
        if(retval instanceof Throwable)
            throw (Throwable)retval;
        return retval;
    }


    public <T> NotifyingFuture<T> callRemoteMethodWithFuture(Address dest, MethodCall call, RequestOptions options) throws Throwable {
        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + call + ", options=" + options);

        Object buf=req_marshaller != null? req_marshaller.objectToBuffer(call) : Util.objectToByteBuffer(call);
        Message msg=new Message(dest, null, null);
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);
        msg.setFlag(options.getFlags());
        if(options.getScope() > 0)
            msg.setScope(options.getScope());
        return super.sendMessageWithFuture(msg, options);
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
        Object      body;
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
            body=req_marshaller != null?
                    req_marshaller.objectFromBuffer(req.getBuffer(), req.getOffset(), req.getLength())
                    : req.getObject();
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("exception marshalling object", e);
            return e;
        }

        if(!(body instanceof MethodCall)) {
            if(log.isErrorEnabled()) log.error("message does not contain a MethodCall object");
            
            // create an exception to represent this and return it
            return  new IllegalArgumentException("message does not contain a MethodCall object") ;
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
                    throw new Exception("no method found for " + method_call.getId());
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

    /* ----------------------------------------------------------------------- */

}
