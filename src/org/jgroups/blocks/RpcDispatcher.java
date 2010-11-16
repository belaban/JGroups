
package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.util.*;

import java.io.Serializable;
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
    protected Marshaller2   req_marshaller=null;

    /** Marshaller to marshal responses at the receiver(s) and unmarshal responses at the caller */
    protected Marshaller2   rsp_marshaller=null;
    protected final List<ChannelListener> additionalChannelListeners=new ArrayList<ChannelListener>();
    protected MethodLookup  method_lookup=null;


    public RpcDispatcher() {
    }


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj) {
        super(channel, l, l2);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }


    @Deprecated
    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection) {
        super(channel, l, l2);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }

    @Deprecated
    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection, boolean concurrent_processing) {
        super(channel, l, l2, false, concurrent_processing);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }


    @Deprecated
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


    public interface Marshaller2 extends Marshaller {
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

        Object objectFromByteBuffer(byte[] buf, int offset, int length) throws Exception;
    }


    /** Used to provide a Marshaller2 interface to a Marshaller. This class is for internal use only, and will be
     * removed in 3.0 when Marshaller and Marshaller2 get merged. Do not use, but provide an implementation of
     * Marshaller directly, e.g. in setRequestMarshaller().
     */
    public static class MarshallerAdapter implements Marshaller2 {
        private final Marshaller marshaller;

        public MarshallerAdapter(Marshaller marshaller) {
            this.marshaller=marshaller;
        }

        public byte[] objectToByteBuffer(Object obj) throws Exception {
            return marshaller.objectToByteBuffer(obj);
        }

        public Object objectFromByteBuffer(byte[] buf) throws Exception {
            return buf == null? null : marshaller.objectFromByteBuffer(buf);
        }

        public Buffer objectToBuffer(Object obj) throws Exception {
            byte[] buf=marshaller.objectToByteBuffer(obj);
            return new Buffer(buf, 0, buf.length);
        }

        public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws Exception {
            if(buf == null || (offset == 0 && length == buf.length))
                return marshaller.objectFromByteBuffer(buf);
            byte[] tmp=new byte[length];
            System.arraycopy(buf, offset, tmp, 0, length);
            return marshaller.objectFromByteBuffer(tmp);
        }

    }


    public static String getName() {return "RpcDispatcher";}

    public Marshaller getRequestMarshaller()             {return req_marshaller;}

    public void setRequestMarshaller(Marshaller m) {
        if(m == null)
            this.req_marshaller=null;
        else if(m instanceof Marshaller2)
            this.req_marshaller=(Marshaller2)m;
        else
            this.req_marshaller=new MarshallerAdapter(m);
    }

    public Marshaller getResponseMarshaller()             {return rsp_marshaller;}

    public void setResponseMarshaller(Marshaller m) {
        if(m == null)
            this.rsp_marshaller=null;
        else if(m instanceof Marshaller2)
            this.rsp_marshaller=(Marshaller2)m;
        else
            this.rsp_marshaller=new MarshallerAdapter(m);

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


    @Deprecated
    public RspList callRemoteMethods(Vector<Address> dests, String method_name, Object[] args,
                                     Class[] types, int mode, long timeout) {
        return callRemoteMethods(dests, method_name, args, types, mode, timeout, false);
    }


    @Deprecated
    public RspList callRemoteMethods(Vector<Address> dests, String method_name, Object[] args,
                                     Class[] types, int mode, long timeout, boolean use_anycasting) {
        return callRemoteMethods(dests, method_name, args, types, mode, timeout, use_anycasting, null);
    }

    @Deprecated
    public RspList callRemoteMethods(Vector<Address> dests, String method_name, Object[] args,
                                     Class[] types, int mode, long timeout, boolean use_anycasting, RspFilter filter) {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call,
                                 new RequestOptions(mode, timeout, use_anycasting, filter, (byte)0));
    }

    public RspList callRemoteMethods(Collection<Address> dests, String method_name, Object[] args,
                                     Class[] types, RequestOptions options) {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, options);
    }

    @Deprecated
    public RspList callRemoteMethods(Vector<Address> dests, String method_name, Object[] args,
                                     String[] signature, int mode, long timeout) {
        return callRemoteMethods(dests, method_name, args, signature, mode, timeout, false);
    }

    @Deprecated
    public RspList callRemoteMethods(Vector<Address> dests, String method_name, Object[] args,
                                     String[] signature, int mode, long timeout, boolean use_anycasting) {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethods(dests, method_call, new RequestOptions(mode, timeout, use_anycasting, null, (byte)0));
    }

    @Deprecated
    public RspList callRemoteMethods(Vector<Address> dests, MethodCall method_call, int mode, long timeout) {
        return callRemoteMethods(dests, method_call,  new RequestOptions().setMode(mode).setTimeout(timeout));
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

    @Deprecated
    public NotifyingFuture<RspList> callRemoteMethodsWithFuture(Vector<Address> dests, MethodCall method_call, int mode, long timeout,
                                                       boolean use_anycasting, boolean oob, RspFilter filter) {
        RequestOptions options=new RequestOptions(mode, timeout, use_anycasting, filter);
        if(oob) options.setFlags(Message.OOB);
        return callRemoteMethodsWithFuture(dests, method_call, options);
    }

    @Deprecated
    public NotifyingFuture<RspList> callRemoteMethodsWithFuture(Vector<Address> dests, MethodCall method_call) {
        return callRemoteMethodsWithFuture(dests, method_call, new RequestOptions());
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


    @Deprecated
    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   Class[] types, int mode, long timeout) throws Throwable {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   Class[] types, RequestOptions options) throws Throwable {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethod(dest, method_call, options);
    }

    @Deprecated
    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   String[] signature, int mode, long timeout) throws Throwable {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    @Deprecated
    public Object callRemoteMethod(Address dest, MethodCall method_call, int mode, long timeout) throws Throwable {
        return callRemoteMethod(dest, method_call, mode, timeout, false);
    }

    @Deprecated
    public Object callRemoteMethod(Address dest, MethodCall method_call, int mode, long timeout, boolean oob) throws Throwable {
        RequestOptions options=new RequestOptions(mode, timeout, false, null);
        if(oob) options.setFlags(Message.OOB);
        return callRemoteMethod(dest, method_call, options);
    }

    @Deprecated
    public Object callRemoteMethod(Address dest, MethodCall call) throws Throwable {
        return callRemoteMethod(dest, call, new RequestOptions());
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

    @Deprecated
    public <T> NotifyingFuture<T> callRemoteMethodWithFuture(Address dest, MethodCall method_call, int mode, long timeout, boolean oob) throws Throwable {
        RequestOptions options=new RequestOptions(mode, timeout, false, null);
        if(oob) options.setFlags(Message.OOB);
        return callRemoteMethodWithFuture(dest, method_call, options);
    }

    @Deprecated
    public <T> NotifyingFuture<T> callRemoteMethodWithFuture(Address dest, MethodCall call) throws Throwable {
        return callRemoteMethodWithFuture(dest, call, new RequestOptions());
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
                    req_marshaller.objectFromByteBuffer(req.getBuffer(), req.getOffset(), req.getLength())
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

    public void channelShunned() {
    }

    public void channelReconnected(Address new_addr) {
    }
    /* ----------------------------------------------------------------------- */

}
