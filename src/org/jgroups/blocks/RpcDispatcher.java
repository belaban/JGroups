// $Id: RpcDispatcher.java,v 1.43 2009/09/21 11:29:02 belaban Exp $

package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.jgroups.util.Buffer;
import org.jgroups.util.NullFuture;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.IllegalArgumentException ;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Future;


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
    protected final List    additionalChannelListeners=new ArrayList();
    protected MethodLookup  method_lookup=null;


    public RpcDispatcher() {
    }


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj) {
        super(channel, l, l2);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection) {
        super(channel, l, l2);
        channel.addChannelListener(this);
        this.server_obj=server_obj;
    }

    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection, boolean concurrent_processing) {
        super(channel, l, l2, false, concurrent_processing);
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


    public String getName() {return "RpcDispatcher";}

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
        return callRemoteMethods(dests, method_name, args, types, mode, timeout, use_anycasting, null);
    }

    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     Class[] types, int mode, long timeout, boolean use_anycasting, RspFilter filter) {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, mode, timeout, use_anycasting, false, filter);
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
        return callRemoteMethods(dests, method_call, mode, timeout, use_anycasting, false);
    }

    public RspList callRemoteMethods(Vector dests, MethodCall method_call, int mode, long timeout,
                                     boolean use_anycasting, boolean oob) {
        return callRemoteMethods(dests, method_call, mode, timeout, use_anycasting, oob, null);
    }


    public RspList callRemoteMethods(Vector dests, MethodCall method_call, int mode, long timeout,
                                     boolean use_anycasting, boolean oob, RspFilter filter) {
        return callRemoteMethods(dests, method_call, mode, timeout, use_anycasting, oob, false, filter);
    }


    public RspList callRemoteMethods(Vector dests, MethodCall method_call, int mode, long timeout,
                                     boolean use_anycasting, boolean oob, boolean dont_bundle, RspFilter filter) {
        if(dests != null && dests.isEmpty()) {
            // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("destination list of ").append(method_call.getName()).
                        append("() is empty: no need to send message"));
            return new RspList();
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("dests=").append(dests).append(", method_call=").append(method_call).
                    append(", mode=").append(mode).append(", timeout=").append(timeout));

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
        if(oob)
            msg.setFlag(Message.OOB);
        if(dont_bundle)
            msg.setFlag(Message.DONT_BUNDLE);
        RspList  retval=super.castMessage(dests, msg, mode, timeout, use_anycasting, filter);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }


    public RspList callRemoteMethods(Vector dests, MethodCall method_call) {
        if(dests != null && dests.isEmpty()) {
            // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("destination list of ").append(method_call.getName()).
                        append("() is empty: no need to send message"));
            return new RspList();
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("dests=").append(dests).append(", method_call=").append(method_call).
                    append(", mode=").append(method_call.getRequestMode()).append(", timeout=").append(method_call.getTimeout()));

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

        byte flags=method_call.getFlags();
        msg.setFlag(flags);

        RspList retval=super.castMessage(dests, msg, method_call.getRequestMode(), method_call.getTimeout(),
                                         method_call.isUseAnycasting(), method_call.getFilter());
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }



    public Future<RspList> callRemoteMethodsWithFuture(Vector dests, MethodCall method_call, int mode, long timeout,
                                                       boolean use_anycasting, boolean oob, RspFilter filter) {
        if(dests != null && dests.isEmpty()) {
            // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("destination list of ").append(method_call.getName()).
                        append("() is empty: no need to send message"));
            return new NullFuture();
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("dests=").append(dests).append(", method_call=").append(method_call).
                    append(", mode=").append(mode).append(", timeout=").append(timeout));

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
        if(oob)
            msg.setFlag(Message.OOB);
        Future<RspList>  retval=super.castMessageWithFuture(dests, msg, mode, timeout, use_anycasting, filter);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }

    public Future<RspList> callRemoteMethodsWithFuture(Vector dests, MethodCall method_call) {
        if(dests != null && dests.isEmpty()) {
            // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("destination list of ").append(method_call.getName()).
                        append("() is empty: no need to send message"));
            return new NullFuture();
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("dests=").append(dests).append(", method_call=").append(method_call).
                    append(", mode=").append(method_call.getMode()).append(", timeout=").append(method_call.getTimeout()));

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
        msg.setFlag(method_call.getFlags());
        Future<RspList> retval=super.castMessageWithFuture(dests, msg, method_call.getRequestMode(), method_call.getTimeout(),
                                                           method_call.isUseAnycasting(), method_call.getFilter());
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
        return callRemoteMethod(dest, method_call, mode, timeout, false);
    }

    public Object callRemoteMethod(Address dest, MethodCall method_call, int mode, long timeout, boolean oob) throws Throwable {
        Object   buf;
        Message  msg;
        Object   retval;

        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + method_call + ", mode=" + mode + ", timeout=" + timeout);

        buf=req_marshaller != null? req_marshaller.objectToBuffer(method_call) : Util.objectToByteBuffer(method_call);
        msg=new Message(dest, null, null);
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);
        if(oob)
            msg.setFlag(Message.OOB);
        retval=super.sendMessage(msg, mode, timeout);
        if(log.isTraceEnabled()) log.trace("retval: " + retval);
        if(retval instanceof Throwable)
            throw (Throwable)retval;
        return retval;
    }

    public Object callRemoteMethod(Address dest, MethodCall call) throws Throwable {
        Object   buf;
        Message  msg;
        Object   retval;

        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + call + ", mode=" + call.getRequestMode() + ", timeout=" + call.getTimeout());

        buf=req_marshaller != null? req_marshaller.objectToBuffer(call) : Util.objectToByteBuffer(call);
        msg=new Message(dest, null, null);
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);
        msg.setFlag(call.getFlags());
        retval=super.sendMessage(msg, call.getRequestMode(), call.getTimeout());
        if(log.isTraceEnabled()) log.trace("retval: " + retval);
        if(retval instanceof Throwable)
            throw (Throwable)retval;
        return retval;
    }

    public <T> Future<T> callRemoteMethodWithFuture(Address dest, MethodCall method_call, int mode, long timeout, boolean oob) throws Throwable {
        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + method_call + ", mode=" + mode + ", timeout=" + timeout);

        Object buf=req_marshaller != null? req_marshaller.objectToBuffer(method_call) : Util.objectToByteBuffer(method_call);
        Message msg=new Message(dest, null, null);
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);
        if(oob)
            msg.setFlag(Message.OOB);
        return super.sendMessageWithFuture(msg, mode, timeout);
    }


    public <T> Future<T> callRemoteMethodWithFuture(Address dest, MethodCall call) throws Throwable {
        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + call + ", mode=" + call.getRequestMode() + ", timeout=" + call.getTimeout());

        Object buf=req_marshaller != null? req_marshaller.objectToBuffer(call) : Util.objectToByteBuffer(call);
        Message msg=new Message(dest, null, null);
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);
        msg.setFlag(call.getFlags());
        return super.sendMessageWithFuture(msg, call.getRequestMode(), call.getTimeout());
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
