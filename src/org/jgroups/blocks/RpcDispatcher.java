// $Id: RpcDispatcher.java,v 1.3 2003/12/11 07:18:03 belaban Exp $

package org.jgroups.blocks;


import java.io.Serializable;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.log.Trace;




/**
 * Dispatches and receives remote group method calls. Is the equivalent of RpcProtocol
 * on the application rather than protocol level.
 * @author Bela Ban
 */
public class RpcDispatcher extends MessageDispatcher implements ChannelListener {
    MethodLookup         method_lookup=new MethodLookupClos();
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

    public MethodLookup getMethodLookup() {return method_lookup;}

    public void setMethodLookup(MethodLookup method_lookup) {
        this.method_lookup=method_lookup;
    }

    public void       setMarshaller(Marshaller m) {this.marshaller=m;}

    public Marshaller getMarshaller()             {return marshaller;}



    public RspList castMessage(Vector dests, Message msg, int mode, long timeout) {
        Trace.error("RpcDispatcher.castMessage()", "this method should not be used with " +
                    "RpcDispatcher, but MessageDispatcher. Returning null");
        return null;
    }

    public Object sendMessage(Message msg, int mode, long timeout) throws TimeoutException, SuspectedException {
        Trace.error("RpcDispatcher.sendMessage()", "this method should not be used with " +
                    "RpcDispatcher, but MessageDispatcher. Returning null");
        return null;
    }




    /**
     * @deprecated use callRemoteMethods(Vector,MethodCall, int, long);
     * @see #callRemoteMethod(Address,MethodCall, int, long)
     */
    
    public RspList callRemoteMethods(Vector dests, String method_name, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }


    /**
     * @deprecated use callRemoteMethods(Vector,MethodCall, int, long);
     * @see #callRemoteMethod(Address,MethodCall, int, long)
     */
    public RspList callRemoteMethods(Vector dests, String method_name, Object arg1, 
                                     int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, arg1);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }


    /**
     * @deprecated use callRemoteMethods(Vector,MethodCall, int, long);
     * @see #callRemoteMethod(Address,MethodCall, int, long)
     */
    public RspList callRemoteMethods(Vector dests, String method_name, Object arg1, Object arg2, 
                                     int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }


    /**
     * @deprecated use callRemoteMethods(Vector,MethodCall, int, long);
     * @see #callRemoteMethod(Address,MethodCall, int, long)
     */
    public RspList callRemoteMethods(Vector dests, String method_name, Object arg1, Object arg2,
                                     Object arg3, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2, arg3);
        return callRemoteMethods(dests, method_call, mode, timeout);
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

        try {
            buf=marshaller != null? marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            Trace.error("RpcProtocol.callRemoteMethods()", "exception=" + e);
            return null;
        }

        msg=new Message(null, null, buf);
        return super.castMessage(dests, msg, mode, timeout);
    }





    /**
     * Calls the remote methods in a number of receivers and returns the results asynchronously via
     * the RspCollector interface.
     * @param dests The destination membership. All members if null
     * @param req_id The request id. Used to match requests and responses. has to be unique for this process
     * @param method_call The method to be called
     * @param coll The RspCollector to be called when a message arrives
     */
//    public void callRemoteMethods(Vector dests, long req_id, MethodCall method_call, RspCollector coll) {
//        byte[]   buf=null;
//        Message  msg=null;
//
//        try {
//            buf=marshaller != null? marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
//        }
//        catch(Exception e) {
//            Trace.error("RpcProtocol.callRemoteMethods()", "exception=" + e);
//            return;
//        }
//
//        msg=new Message(null, null, buf);
//        super.castMessage(dests, req_id, msg, coll);
//    }





    /**
     * @deprecated use callRemoteMethod(Address,MethodCall, int, long);
     * @see #callRemoteMethod(Address,MethodCall, int, long)
     */
    public Object callRemoteMethod(Address dest, String method_name, int mode, long timeout) 
        throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }


    /**
     * @deprecated use callRemoteMethod(Address,MethodCall, int, long);
     * @see #callRemoteMethod(Address,MethodCall, int, long)
     */
    public Object callRemoteMethod(Address dest, String method_name, Object arg1, int mode, long timeout) 
        throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, arg1);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    
    /**
     * @deprecated use callRemoteMethod(Address,MethodCall, int, long);
     * @see #callRemoteMethod(Address,MethodCall, int, long)
     */
    public Object callRemoteMethod(Address dest, String method_name, Object arg1, Object arg2, 
                                int mode, long timeout) throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }


    /**
     * @deprecated use callRemoteMethod(Address,MethodCall, int, long);
     * @see #callRemoteMethod(Address,MethodCall, int, long)
     */
    public Object callRemoteMethod(Address dest, String method_name, Object arg1, Object arg2, 
                                   Object arg3, int mode, long timeout) throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2, arg3);
        return callRemoteMethod(dest, method_call, mode, timeout);
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

        try {
            buf=marshaller != null? marshaller.objectToByteBuffer(method_call) : Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            Trace.error("RpcProtocol.callRemoteMethod()", "exception=" + e);
            return null;
        }

        msg=new Message(dest, null, buf);
        return super.sendMessage(msg, mode, timeout);
    }





    /**
     * Message contains MethodCall. Execute it against *this* object and return result.
     * Use MethodCall.invoke() to do this. Return result.
     */
    public Object handle(Message req) {
        Object      body=null;
        MethodCall  method_call;

        if(server_obj == null) {
            Trace.error("RpcDispatcher.handle()", "no method handler is registered. Discarding request.");
            return null;
        }

        if(req == null || req.getBuffer() == null) {
            Trace.error("RpcProtocol.handle()", "message or message buffer is null");
            return null;
        }

        try {
            body=marshaller != null? marshaller.objectFromByteBuffer(req.getBuffer()) :
                    Util.objectFromByteBuffer(req.getBuffer());
        }
        catch(Exception e) {
            Trace.error("RpcDispatcher.handle()", "exception=" + e);
            return e;
        }

        if(body == null || !(body instanceof MethodCall)) {
            Trace.error("RpcDispatcher.handle()", "message does not contain a MethodCall object");
            return null;
        }

        method_call=(MethodCall)body;
        try {
            return method_call.invoke(server_obj, method_lookup);
        }
        catch(Throwable x) {
            Trace.error("RpcDispatcher.handle()", Trace.getStackTrace(x));
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
