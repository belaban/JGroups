// $Id: RpcProtocol.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.stack;


import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.blocks.MethodLookupClos;
import org.jgroups.log.Trace;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.Vector;




/**
 * Base class for group RMC peer protocols.
 * @author Bela Ban
 */
public class RpcProtocol extends MessageProtocol {
    MethodLookup method_lookup=new MethodLookupClos();


    public String getName() {
        return "RpcProtocol";
    }


    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethods(Vector,MethodCall,int,long)
     */

    public RspList callRemoteMethods(Vector dests, String method_name, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }

    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethods(Vector,MethodCall,int,long)
     */

    public RspList callRemoteMethods(Vector dests, String method_name, Object arg1,
                                     int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, arg1);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }

    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethods(Vector,MethodCall,int,long)
     */

    public RspList callRemoteMethods(Vector dests, String method_name, Object arg1, Object arg2,
                                     int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }

    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethods(Vector,MethodCall,int,long)
     */

    public RspList callRemoteMethods(Vector dests, String method_name, Object arg1, Object arg2,
                                     Object arg3, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2, arg3);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }

    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethods(Vector,MethodCall,int,long)
     */

    public RspList callRemoteMethods(Vector dests, String method_name, Object arg1, Object arg2,
                                     Object arg3, Object arg4, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2, arg3, arg4);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }

    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethods(Vector,MethodCall,int,long)
     */

    public RspList callRemoteMethods(Vector dests, String method_name, Object arg1, Object arg2,
                                     Object arg3, Object arg4, Object arg5, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2, arg3, arg4, arg5);
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
        byte[] buf=null;
        Message msg=null;

        try {
            buf=Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            Trace.error("RpcProtocol.callRemoteMethods()", "exception=" + e);
            return null;
        }

        msg=new Message(null, null, buf);
        return castMessage(dests, msg, mode, timeout);
    }


    public Object callRemoteMethod(Address dest, String method_name, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }


    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethod(Address,MethodCall,int,long)
     */
    public Object callRemoteMethod(Address dest, String method_name, Object arg1, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, arg1);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }


    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethod(Address,MethodCall,int,long)
     */
    public Object callRemoteMethod(Address dest, String method_name, Object arg1, Object arg2,
                                   int mode, long timeout) throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }


    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethod(Address,MethodCall,int,long)
     */
    public Object callRemoteMethod(Address dest, String method_name, Object arg1, Object arg2,
                                   Object arg3, int mode, long timeout) throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2, arg3);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethod(Address,MethodCall,int,long)
     */
    public Object callRemoteMethod(Address dest, String method_name, Object arg1, Object arg2, Object arg3,
                                   Object arg4, int mode, long timeout) throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2, arg3, arg4);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    /**
     * @deprecated this method results in an invalid method call if the argument is null
     * @see #callRemoteMethod(Address,MethodCall,int,long)
     */
    public Object callRemoteMethod(Address dest, String method_name, Object arg1, Object arg2, Object arg3,
                                   Object arg4, Object arg5, int mode, long timeout) throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, arg1, arg2, arg3, arg4, arg5);
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
        byte[] buf=null;
        Message msg=null;

        try {
            buf=Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            Trace.error("RpcProtocol.callRemoteMethod()", "exception=" + e);
            return null;
        }

        msg=new Message(dest, null, buf);
        return sendMessage(msg, mode, timeout);
    }


    /**
     Message contains MethodCall. Execute it against *this* object and return result.
     Use MethodCall.invoke() to do this. Return result.
     */
    public Object handle(Message req) {
        Object     body=null;
        MethodCall method_call;

        if(req == null || req.getBuffer() == null) {
            Trace.error("RpcProtocol.handle()", "message or message buffer is null");
            return null;
        }

        try {
            body=Util.objectFromByteBuffer(req.getBuffer());
        }
        catch(Exception e) {
            Trace.error("RpcProtocol.handle()", "exception=" + e);
            return e;
        }

        if(body == null || !(body instanceof MethodCall)) {
            Trace.error("RpcProtocol.handle()", "message does not contain a MethodCall object");
            return null;
        }

        method_call=(MethodCall)body;
        try {
            return method_call.invoke(this, method_lookup);
        }
        catch(Throwable x) {
            Trace.error("RpcProtocol.handle()", Trace.getStackTrace(x));
            return x;
        }
    }


    /**
     Handle up event. Return false if it should not be passed up the stack.
     */
    public boolean handleUpEvent(Event evt) {
        return true;
    }


    /**
     Handle down event. Return false if it should not be passed down the stack.
     */
    public boolean handleDownEvent(Event evt) {
        return true;
    }


}
