
package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
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
    /** Marshaller to marshall requests at the caller, unmarshal requests at the receiver(s), marshall responses at the
     * receivers and unmarshall responses at the caller */
    protected Marshaller    marshaller;
    protected MethodLookup  method_lookup;
    protected MethodInvoker method_invoker;


    public RpcDispatcher() {
    }


    public RpcDispatcher(JChannel channel, Object server_obj) {
        super(channel);
        setRequestHandler(this);
        this.server_obj=server_obj;
    }


    public Marshaller    getMarshaller()                             {return marshaller;}
    public RpcDispatcher setMarshaller(Marshaller m)                 {marshaller=m; if(corr != null)
                                                                                    corr.setMarshaller(m); return this;}
    public Object        getServerObject()                           {return server_obj;}
    public RpcDispatcher setServerObject(Object server_obj)          {this.server_obj=server_obj; return this;}
    public RpcDispatcher setMembershipListener(MembershipListener l) {return (RpcDispatcher)super.setMembershipListener(l);}
    public MethodLookup  getMethodLookup()                           {return method_lookup;}
    public RpcDispatcher setMethodLookup(MethodLookup method_lookup) {this.method_lookup=method_lookup; return this;}
    public MethodInvoker getMethodInvoker()                          {return method_invoker;}
    public RpcDispatcher setMethodInvoker(MethodInvoker mi)          {this.method_invoker=mi; return this;}


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
                                            Class[] types, RequestOptions options) throws Exception {
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
            log.trace("destination list of %s() is empty: no need to send message", method_call.methodName());
            return empty_rsplist;
        }

        Buffer buf=methodCallToBuffer(method_call, marshaller);
        RspList<T> retval=super.castMessage(dests, buf, opts);
        if(log.isTraceEnabled())
            log.trace("dests=%s, method_call=%s, options=%s, responses: %s", dests, method_call, opts, retval);
        return retval;
    }


    /**
     * Invokes a method in all members and expects responses from members contained in dests (or all members if dests is null).
     * @param dests A list of addresses. If null, we'll wait for responses from all cluster members
     * @param method_call The method (plus args) to be invoked
     * @param options A collection of call options, e.g. sync versus async, timeout etc
     * @return CompletableFuture A future from which the results can be fetched, or null if the RPC is asynchronous
     * @throws Exception If the sending of the message threw an exception. Note that <em>no</em> exception will be
     *                   thrown if any of the target members threw an exception; such an exception will be in the Rsp
     *                   element for the particular member in the RspList
     */
    public <T> CompletableFuture<RspList<T>> callRemoteMethodsWithFuture(Collection<Address> dests, MethodCall method_call,
                                                                         RequestOptions options) throws Exception {
        if(dests != null && dests.isEmpty()) { // don't send if dest list is empty
            log.trace("destination list of %s() is empty: no need to send message", method_call.methodName());
            return CompletableFuture.completedFuture(empty_rsplist);
        }
        Buffer buf=methodCallToBuffer(method_call, marshaller);
        CompletableFuture<RspList<T>> retval=super.castMessageWithFuture(dests, buf, options);
        if(log.isTraceEnabled())
            log.trace("dests=%s, method_call=%s, options=%s", dests, method_call, options);
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
    public <T> T callRemoteMethod(Address dest, String meth, Object[] args, Class[] types, RequestOptions opts) throws Exception {
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
        Buffer buf=methodCallToBuffer(call, marshaller);
        T retval=super.sendMessage(dest, buf, options);
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
        Buffer buf=methodCallToBuffer(call, marshaller);
        return super.sendMessageWithFuture(dest, buf, opts);
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

        MethodCall method_call=methodCallFromBuffer(req.getRawBuffer(), req.getOffset(), req.getLength(), marshaller);
        if(log.isTraceEnabled())
            log.trace("[sender=%s], method_call: %s", req.getSrc(), method_call);

        if(method_call.mode() == MethodCall.ID) {
            if(method_invoker != null) // this trumps a method lookup
                return method_invoker.invoke(server_obj, method_call.methodId(), method_call.args());
            if(method_lookup == null)
                throw new Exception(String.format("MethodCall uses ID=%d, but method_lookup has not been set", method_call.methodId()));
            Method m=method_lookup.findMethod(method_call.methodId());
            if(m == null)
                throw new Exception("no method found for " + method_call.methodId());
            method_call.method(m);
        }
        return method_call.invoke(server_obj);
    }

    protected static Buffer methodCallToBuffer(final MethodCall call, Marshaller marshaller) throws Exception {
        Object[] args=call.args();

        int estimated_size=64;
        if(args != null)
            for(Object arg: args)
                estimated_size+=marshaller != null? marshaller.estimatedSize(arg) : (arg == null? 2 : 50);

        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(estimated_size, true);
        call.writeTo(out, marshaller);
        return out.getBuffer();
    }

    protected static MethodCall methodCallFromBuffer(final byte[] buf, int offset, int length, Marshaller marshaller) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, offset, length);
        MethodCall call=new MethodCall();
        call.readFrom(in, marshaller);
        return call;
    }

    protected void correlatorStarted() {
        if(corr != null)
            corr.setMarshaller(marshaller);
    }

}
