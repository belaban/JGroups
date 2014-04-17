
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
public class RpcDispatcher extends MessageDispatcher {
    protected Object        server_obj=null;
    /** Marshaller to marshall requests at the caller and unmarshal requests at the receiver(s) */
    protected Marshaller    req_marshaller=null;

    /** Marshaller to marshal responses at the receiver(s) and unmarshal responses at the caller */
    protected Marshaller    rsp_marshaller=null;

    protected MethodLookup  method_lookup=null;


    public RpcDispatcher() {
    }


    public RpcDispatcher(Channel channel, MessageListener l, MembershipListener l2, Object server_obj) {
        super(channel, l, l2);
        this.server_obj=server_obj;
    }

    public RpcDispatcher(Channel channel, Object server_obj) {
        this(channel, null, null, server_obj);
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


    /**
     * Invokes a method in all members and expects responses from members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, we'll wait for responses from all cluster members
     * @param method_name The name of the target method
     * @param args The arguments to be passed
     * @param types The types of the arguments
     * @param options A collection of call options, e.g. sync versus async, timeout etc
     * @return RspList<T> A response list with results, one for each member in dests
     * @throws Exception If the sending of the message threw an exception. Note that <em>no</em> exception will be
     *                   thrown if any of the target members threw an exception, but this exception will be in the Rsp
     *                   object for the particular member in the RspList
     */
    public <T> RspList<T> callRemoteMethods(Collection<Address> dests, String method_name, Object[] args,
                                            Class[] types, RequestOptions options) throws Exception {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, options);
    }



    /**
     * Invokes a method in all members and expects responses from members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, we'll wait for responses from all cluster members
     * @param method_call The method (plus args) to be invoked
     * @param options A collection of call options, e.g. sync versus async, timeout etc
     * @return RspList A list of return values and flags (suspected, not received) per member
     * @throws Exception If the sending of the message threw an exception. Note that <em>no</em> exception will be
     *                   thrown if any of the target members threw an exception, but this exception will be in the Rsp
     *                   object for the particular member in the RspList
     * @since 2.9
     */
    public <T> RspList<T> callRemoteMethods(Collection<Address> dests,
                                            MethodCall method_call,
                                            RequestOptions options) throws Exception {
        if(dests != null && dests.isEmpty()) { // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace("destination list of " + method_call.getName() + "() is empty: no need to send message");
            return new RspList();
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("dests=").append(dests).append(", method_call=").append(method_call).
              append(", options=").append(options));

        Object buf=req_marshaller != null? req_marshaller.objectToBuffer(method_call) : Util.objectToByteBuffer(method_call);

        Message msg=new Message();
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);

        RspList<T> retval=super.castMessage(dests, msg, options);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }


    /**
     * Invokes a method in all members and expects responses from members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, we'll wait for responses from all cluster members
     * @param method_call The method (plus args) to be invoked
     * @param options A collection of call options, e.g. sync versus async, timeout etc
     * @param listener A FutureListener which will be registered (if non null) with the future <em>before</em> the call is invoked
     * @return NotifyingFuture A future from which the results can be fetched
     * @throws Exception If the sending of the message threw an exception. Note that <em>no</em> exception will be
     *                   thrown if any of the target members threw an exception; such an exception will be in the Rsp
     *                   element for the particular member in the RspList
     */
    public <T> NotifyingFuture<RspList<T>> callRemoteMethodsWithFuture(Collection<Address> dests,
                                                                       MethodCall method_call,
                                                                       RequestOptions options,
                                                                       FutureListener<RspList<T>> listener) throws Exception {
        if(dests != null && dests.isEmpty()) { // don't send if dest list is empty
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("destination list of ").append(method_call.getName()).
                        append("() is empty: no need to send message"));
            return new NullFuture<RspList<T>>(new RspList());
        }

        if(log.isTraceEnabled())
            log.trace(new StringBuilder("dests=").append(dests).append(", method_call=").append(method_call).
                    append(", options=").append(options));

        Object buf=req_marshaller != null? req_marshaller.objectToBuffer(method_call) : Util.objectToByteBuffer(method_call);

        Message msg=new Message();
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);

        NotifyingFuture<RspList<T>>  retval=super.castMessageWithFuture(dests, msg, options, listener);
        if(log.isTraceEnabled()) log.trace("responses: " + retval);
        return retval;
    }


    /**
     * Invokes a method in all members and expects responses from members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, we'll wait for responses from all cluster members
     * @param method_call The method (plus args) to be invoked
     * @param options A collection of call options, e.g. sync versus async, timeout etc
     * @return NotifyingFuture A future from which the results can be fetched
     * @throws Exception If the sending of the message threw an exception. Note that <em>no</em> exception will be
     *                   thrown if any of the target members threw an exception; such an exception will be in the Rsp
     *                   element for the particular member in the RspList
     */
    public <T> NotifyingFuture<RspList<T>> callRemoteMethodsWithFuture(Collection<Address> dests,
                                                                       MethodCall method_call,
                                                                       RequestOptions options) throws Exception {
        return callRemoteMethodsWithFuture(dests, method_call, options, null);
    }


    /**
     * Invokes a method in a cluster member and - if blocking - returns the result
     * @param dest The target member on which to invoke the method
     * @param method_name The name of the method
     * @param args The arguments
     * @param types The types of the arguments
     * @param options The options (e.g. blocking, timeout etc)
     * @return The result
     * @throws Exception Thrown if the method invocation threw an exception, either at the caller or the callee
     */
    public <T> T callRemoteMethod(Address dest, String method_name, Object[] args,
                                   Class[] types, RequestOptions options) throws Exception {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return (T)callRemoteMethod(dest, method_call, options);
    }


    /**
     * Invokes a method in a cluster member and - if blocking - returns the result
     * @param dest The target member on which to invoke the method
     * @param call The call to be invoked, including method are arguments
     * @param options The options (e.g. blocking, timeout etc)
     * @return The result
     * @throws Exception Thrown if the method invocation threw an exception, either at the caller or the callee
     */
    public <T> T callRemoteMethod(Address dest, MethodCall call, RequestOptions options) throws Exception {
        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + call + ", options=" + options);

        Object buf=req_marshaller != null? req_marshaller.objectToBuffer(call) : Util.objectToByteBuffer(call);
        Message msg=new Message(dest, null, null);
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);

        T retval=super.sendMessage(msg, options);
        if(log.isTraceEnabled()) log.trace("retval: " + retval);
        return retval;
    }


    /**
     * Invokes a method in a cluster member and - if blocking - returns the result
     * @param dest The target member on which to invoke the method
     * @param call The call to be invoked, including method are arguments
     * @param options The options (e.g. blocking, timeout etc)
     * @param listener A FutureListener which will be registered (if non null) with the future <em>before</em> the call is invoked
     * @return A future from which the result can be fetched. If the callee threw an invocation, an ExecutionException
     *         will be thrown on calling Future.get().
     * @throws Exception Thrown if the method invocation threw an exception
     */
    public <T> NotifyingFuture<T> callRemoteMethodWithFuture(Address dest, MethodCall call, RequestOptions options,
                                                             FutureListener<T> listener) throws Exception {
        if(log.isTraceEnabled())
            log.trace("dest=" + dest + ", method_call=" + call + ", options=" + options);

        Object buf=req_marshaller != null? req_marshaller.objectToBuffer(call) : Util.objectToByteBuffer(call);
        Message msg=new Message(dest, null, null);
        if(buf instanceof Buffer)
            msg.setBuffer((Buffer)buf);
        else
            msg.setBuffer((byte[])buf);

        return super.sendMessageWithFuture(msg, options, listener);
    }

    /**
     * Invokes a method in a cluster member and - if blocking - returns the result
     * @param dest The target member on which to invoke the method
     * @param call The call to be invoked, including method are arguments
     * @param options The options (e.g. blocking, timeout etc)
     * @return A future from which the result can be fetched. If the callee threw an invocation, an ExecutionException
     *         will be thrown on calling Future.get().
     * @throws Exception Thrown if the method invocation threw an exception
     */
    public <T> NotifyingFuture<T> callRemoteMethodWithFuture(Address dest, MethodCall call, RequestOptions options) throws Exception {
        return callRemoteMethodWithFuture(dest, call, options, null);
    }


    protected void correlatorStarted() {
        if(corr != null)
            corr.setMarshaller(rsp_marshaller);
    }


    /**
     * Message contains MethodCall. Execute it against *this* object and return result.
     * Use MethodCall.invoke() to do this. Return result.
     */
    public Object handle(Message req) throws Exception {
        if(server_obj == null) {
            if(log.isErrorEnabled()) log.error("no method handler is registered. Discarding request.");
            return null;
        }

        if(req == null || req.getLength() == 0) {
            if(log.isErrorEnabled()) log.error("message or message buffer is null");
            return null;
        }

        Object body=req_marshaller != null?
          req_marshaller.objectFromBuffer(req.getRawBuffer(), req.getOffset(), req.getLength()) : req.getObject();

        if(!(body instanceof MethodCall))
            throw new IllegalArgumentException("message does not contain a MethodCall object") ;

        MethodCall method_call=(MethodCall)body;

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


}
