// $Id: RpcDispatcher.java,v 1.13 2004/08/14 01:46:25 belaban Exp $

package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.Serializable;
import java.util.Vector;




/**
 * Dispatches and receives remote group method calls. Is the equivalent of RpcProtocol
 * on the application rather than protocol level.
 * @author Bela Ban
 */
public class RpcDispatcher extends MessageDispatcher implements ChannelListener {
    protected Object     server_obj=null;
    protected Marshaller marshaller=null;


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj) {
        super(channel, l, l2);
        channel.setChannelListener(this);
        this.server_obj=server_obj;
    }


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection) {
        super(channel, l, l2, deadlock_detection);
        channel.setChannelListener(this);
        this.server_obj=server_obj;
    }

    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj,
                         boolean deadlock_detection, boolean concurrent_processing) {
        super(channel, l, l2, deadlock_detection, concurrent_processing);
        channel.setChannelListener(this);
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
                ((Channel)t).setChannelListener(this);
            }
        }

        this.server_obj=server_obj;
    }


    public interface Marshaller {
        byte[] objectToByteBuffer(Object obj) throws Exception;
        Object objectFromByteBuffer(byte[] buf) throws Exception;
    }


    public String getName() {return "RpcDispatcher";}

    public void       setMarshaller(Marshaller m) {this.marshaller=m;}

    public Marshaller getMarshaller()             {return marshaller;}

    public Object getServerObject() {return server_obj;}


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
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }

    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     String[] signature, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }


    public RspList callRemoteMethods(Vector dests, MethodCall method_call, int mode, long timeout) {
        byte[]   buf=null;
        Message  msg=null;
        RspList  retval=null;

        if(log.isTraceEnabled())
            log.trace("dests=" + dests + ", method_call=" + method_call + ", mode=" + mode + ", timeout=" + timeout);

        if(dests != null && dests.size() == 0) {
            // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace("destination list is non-null and empty: no need to send message");
            return new RspList();
        }

        try {
            buf=marshaller != null? marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception=" + e);
            return null;
        }

        msg=new Message(null, null, buf);
        retval=super.castMessage(dests, msg, mode, timeout);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }





    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   Class[] types, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   String[] signature, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    public Object callRemoteMethod(Address dest, MethodCall method_call, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        byte[]   buf=null;
        Message  msg=null;
        Object   retval=null;

        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + method_call + ", mode=" + mode + ", timeout=" + timeout);

        try {
            buf=marshaller != null? marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception=" + e);
            return null;
        }

        msg=new Message(dest, null, buf);
        retval=super.sendMessage(msg, mode, timeout);
        if(log.isTraceEnabled()) log.trace("retval: " + retval);
        return retval;
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
            body=marshaller != null? marshaller.objectFromByteBuffer(req.getBuffer()) : req.getObject();
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("exception=" + e);
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
            return method_call.invoke(server_obj);
        }
        catch(Throwable x) {
            log.error("failed invoking method", x);
            return x;
        }
    }


    /* --------------------- Interface ChannelListener ---------------------- */

    public void channelConnected(Channel channel) {
        start();
    }

    public void channelDisconnected(Channel channel) {
        stop();
    }

    public void channelClosed(Channel channel) {
        stop();
    }

    public void channelShunned() {
        
    }

    public void channelReconnected(Address new_addr) {
        
    }
    /* ----------------------------------------------------------------------- */

}
