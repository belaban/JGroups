
package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.util.*;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;


/**
 * This class allows a programmer to invoke remote methods in all (or single) group members and optionally wait for
 * the return value(s).<p/>
 * An application will typically create a channel and layer the RpcDispatcher building block on top of it, which
 * allows it to dispatch remote methods (client role) and at the same time be called by other members (server role).<p/>
 * This class is derived from MessageDispatcher. 
 * @author Bela Ban
 */
public class RpcDispatcher extends MessageDispatcher {
    protected Object        server_obj;
    protected MethodLookup  method_lookup;
    protected MethodInvoker method_invoker;


    public RpcDispatcher() {
    }


    public RpcDispatcher(JChannel channel, Object server_obj) {
        super(channel);
        setRequestHandler(this);
        this.server_obj=server_obj;
    }


    public Object        getServerObject()                    {return server_obj;}
    public RpcDispatcher setServerObject(Object obj)          {this.server_obj=obj; return this;}
    public RpcDispatcher setReceiver(Receiver r)              {return (RpcDispatcher)super.setReceiver(r);}
    public MethodLookup  getMethodLookup()                    {return method_lookup;}
    public RpcDispatcher setMethodLookup(MethodLookup ml)     {this.method_lookup=ml; return this;}
    public MethodInvoker getMethodInvoker()                   {return method_invoker;}
    public RpcDispatcher setMethodInvoker(MethodInvoker mi)   {this.method_invoker=mi; return this;}


    /**
     * Invokes a method in all members and expects responses from members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, we'll wait for responses from all cluster members
     * @param method_name The name of the target method
     * @param args The arguments to be passed
     * @param types The types of the arguments
     * @param options A collection of call options, e.g. sync versus async, timeout etc
     * @return RspList<T> A response list with results, one for each member in dests, or null if the RPC is asynchronous
     * @throws Exception If the sending of the message threw an exception. Note that <em>no</em> exception will be
     *                   thrown if any of the target members threw an exception, but this exception will be in the Rsp
     *                   object for the particular member in the RspList
     */
    public <T> RspList<T> callRemoteMethods(Collection<Address> dests, String method_name, Object[] args,
                                            Class<?>[] types, RequestOptions options) throws Exception {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, options);
    }



    /**
     * Invokes a method in all members and expects responses from members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, we'll wait for responses from all cluster members
     * @param method_call The method (plus args) to be invoked
     * @param opts A collection of call options, e.g. sync versus async, timeout etc
     * @return RspList A list of return values and flags (suspected, not received) per member, or null if the RPC is
     *                 asynchronous
     * @throws Exception If the sending of the message threw an exception. Note that <em>no</em> exception will be
     *                   thrown if any of the target members threw an exception, but this exception will be in the Rsp
     *                   object for the particular member in the RspList
     * @since 2.9
     */
    public <T> RspList<T> callRemoteMethods(Collection<Address> dests, MethodCall method_call,
                                            RequestOptions opts) throws Exception {
        if(dests != null && dests.isEmpty()) { // don't send if dest list is empty
            log.trace("destination list of %s() is empty: no need to send message", method_call.getMethodName());
            return empty_rsplist;
        }
        Message msg=new ObjectMessage(null, method_call);
        RspList<T> retval=super.castMessage(dests, msg, opts);
        if(log.isTraceEnabled())
            log.trace("dests=%s, method_call=%s, options=%s, responses: %s", dests, method_call, opts, retval);
        return retval;
    }


    /**
     * Invokes a method in all members and expects responses from members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, we'll wait for responses from all cluster members
     * @param call The method (plus args) to be invoked
     * @param options A collection of call options, e.g. sync versus async, timeout etc
     * @return CompletableFuture A future from which the results can be fetched, or null if the RPC is asynchronous
     * @throws Exception If the sending of the message threw an exception. Note that <em>no</em> exception will be
     *                   thrown if any of the target members threw an exception; such an exception will be in the Rsp
     *                   element for the particular member in the RspList
     */
    public <T> CompletableFuture<RspList<T>> callRemoteMethodsWithFuture(Collection<Address> dests, MethodCall call,
                                                                         RequestOptions options) throws Exception {
        if(dests != null && dests.isEmpty()) { // don't send if dest list is empty
            log.trace("destination list of %s() is empty: no need to send message", call.getMethodName());
            return CompletableFuture.completedFuture(empty_rsplist);
        }
        Message msg=new ObjectMessage(null, call);
        CompletableFuture<RspList<T>> retval=super.castMessageWithFuture(dests, msg, options);
        if(log.isTraceEnabled())
            log.trace("dests=%s, method_call=%s, options=%s", dests, call, options);
        return retval;
    }



    /**
     * Invokes a method in a cluster member and - if blocking - returns the result
     * @param dest The target member on which to invoke the method
     * @param meth The name of the method
     * @param args The arguments
     * @param types The types of the arguments
     * @param opts The options (e.g. blocking, timeout etc)
     * @return The result. Null if the call is asynchronous (non-blocking) or if the method returns void
     * @throws Exception Thrown if the method invocation threw an exception, either at the caller or the callee
     */
    public <T> T callRemoteMethod(Address dest, String meth, Object[] args, Class<?>[] types, RequestOptions opts) throws Exception {
        MethodCall method_call=new MethodCall(meth, args, types);
        return (T)callRemoteMethod(dest, method_call, opts);
    }


    /**
     * Invokes a method in a cluster member and - if blocking - returns the result
     * @param dest The target member on which to invoke the method
     * @param call The call to be invoked, including method are arguments
     * @param options The options (e.g. blocking, timeout etc)
     * @return The result. Null if the call is asynchronous (non-blocking) or if the method returns void
     * @throws Exception Thrown if the method invocation threw an exception, either at the caller or the callee
     */
    public <T> T callRemoteMethod(Address dest, MethodCall call, RequestOptions options) throws Exception {
        Message req=new ObjectMessage(dest, call);
        T retval=super.sendMessage(req, options);
        if(log.isTraceEnabled())
            log.trace("dest=%s, method_call=%s, options=%s, retval: %s", dest, call, options, retval);
        return retval;
    }


    /**
     * Invokes a method in a cluster member and - if blocking - returns the result
     * @param dest The target member on which to invoke the method
     * @param call The call to be invoked, including method are arguments
     * @param opts The options (e.g. blocking, timeout etc)
     * @return A future from which the result can be fetched. If the callee threw an invocation, an ExecutionException
     *         will be thrown on calling Future.get(). If the invocation was asynchronous, null will be returned.
     * @throws Exception Thrown if the method invocation threw an exception
     */
    public <T> CompletableFuture<T> callRemoteMethodWithFuture(Address dest, MethodCall call, RequestOptions opts) throws Exception {
        if(log.isTraceEnabled())
            log.trace("dest=%s, method_call=%s, options=%s", dest, call, opts);
        Message msg=new ObjectMessage(dest, call);
        return super.sendMessageWithFuture(msg, opts);
    }




    /**
     * Message contains MethodCall. Execute it against *this* object and return result.
     * Use MethodCall.invoke() to do this. Return result.
     */
    public Object handle(Message req) throws Exception {
        if(server_obj == null) {
            log.error(Util.getMessage("NoMethodHandlerIsRegisteredDiscardingRequest"));
            return null;
        }

        if(req == null || req.getLength() == 0) {
            log.error(Util.getMessage("MessageOrMessageBufferIsNull"));
            return null;
        }

        MethodCall method_call=req.getObject();   // methodCallFromBuffer(req.getArray(), req.getOffset(), req.getLength(), marshaller);
        if(log.isTraceEnabled())
            log.trace("[sender=%s], method_call: %s", req.getSrc(), method_call);

        if(method_call.useIds()) {
            if(method_invoker != null) // this trumps a method lookup
                return method_invoker.invoke(server_obj, method_call.getMethodId(), method_call.getArgs());
            if(method_lookup == null)
                throw new Exception(String.format("MethodCall uses ID=%d, but method_lookup has not been set", method_call.getMethodId()));
            Method m=method_lookup.findMethod(method_call.getMethodId());
            if(m == null)
                throw new Exception("no method found for " + method_call.getMethodId());
            method_call.setMethod(m);
        }
        return method_call.invoke(server_obj);
    }

}
