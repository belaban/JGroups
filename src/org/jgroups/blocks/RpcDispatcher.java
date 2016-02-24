
package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.util.Buffer;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

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


    public RpcDispatcher() {
    }


    public RpcDispatcher(JChannel channel, Object server_obj) {
        super(channel);
        setRequestHandler(this);
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
         * @return a Buffer
         * @throws Exception
         */
        Buffer objectToBuffer(Object obj) throws Exception;
        Object objectFromBuffer(byte[] buf, int offset, int length) throws Exception;
    }




    public static String getName()                                   {return RpcDispatcher.class.getSimpleName();}
    public Marshaller    getMarshaller()                             {return marshaller;}
    public RpcDispatcher setMarshaller(Marshaller m)                 {marshaller=m; return this;}
    public Object        getServerObject()                           {return server_obj;}
    public RpcDispatcher setServerObject(Object server_obj)          {this.server_obj=server_obj; return this;}
    public RpcDispatcher setMembershipListener(MembershipListener l) {return (RpcDispatcher)super.setMembershipListener(l);}
    public MethodLookup  getMethodLookup()                           {return method_lookup;}
    public RpcDispatcher setMethodLookup(MethodLookup method_lookup) {this.method_lookup=method_lookup; return this;}


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
            log.trace("destination list of %s() is empty: no need to send message", method_call.getName());
            return empty_rsplist;
        }

        if(log.isTraceEnabled())
            log.trace("dests=%s, method_call=%s, options=%s", dests, method_call, opts);

        Buffer buf=marshaller != null? marshaller.objectToBuffer(method_call) : Util.objectToBuffer(method_call);
        RspList<T> retval=super.castMessage(dests, buf, opts);
        if(log.isTraceEnabled()) log.trace("responses: %s", retval);
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
            log.trace("destination list of %s() is empty: no need to send message", method_call.getName());
            return CompletableFuture.completedFuture(empty_rsplist);
        }

        if(log.isTraceEnabled())
            log.trace("dests=%s, method_call=%s, options=%s", dests, method_call, options);

        Buffer buf=marshaller != null? marshaller.objectToBuffer(method_call) : Util.objectToBuffer(method_call);
        return super.castMessageWithFuture(dests, buf, options);
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
        if(log.isTraceEnabled())
            log.trace("dest=%s, method_call=%s, options=%s", dest, call, options);

        Buffer buf=marshaller != null? marshaller.objectToBuffer(call) : Util.objectToBuffer(call);
        T retval=super.sendMessage(dest, buf, options);
        if(log.isTraceEnabled()) log.trace("retval: %s", retval);
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
        Buffer buf=marshaller != null? marshaller.objectToBuffer(call) : Util.objectToBuffer(call);
        return super.sendMessageWithFuture(dest, buf, opts);
    }


    protected void correlatorStarted() {
        if(corr != null)
            corr.setMarshaller(marshaller);
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

        Object body=marshaller != null?
          marshaller.objectFromBuffer(req.getRawBuffer(), req.getOffset(), req.getLength()) : req.getObject();

        if(!(body instanceof MethodCall))
            throw new IllegalArgumentException("message does not contain a MethodCall object") ;

        MethodCall method_call=(MethodCall)body;

        if(log.isTraceEnabled())
            log.trace("[sender=%s], method_call: %s", req.getSrc(), method_call);

        if(method_call.getMode() == MethodCall.ID) {
            if(method_lookup == null)
                throw new Exception(String.format("MethodCall uses ID=%d, but method_lookup has not been set", method_call.getId()));
            Method m=method_lookup.findMethod(method_call.getId());
            if(m == null)
                throw new Exception("no method found for " + method_call.getId());
            method_call.setMethod(m);
        }
            
        return method_call.invoke(server_obj);
    }


}
