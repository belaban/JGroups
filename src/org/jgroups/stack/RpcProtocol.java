// $Id: RpcProtocol.java,v 1.4 2004/05/13 05:44:32 belaban Exp $

package org.jgroups.stack;


import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.blocks.MethodLookupJava;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.Vector;




/**
 * Base class for group RMC peer protocols.
 * @author Bela Ban
 */
public class RpcProtocol extends MessageProtocol {
    MethodLookup method_lookup=new MethodLookupJava();


    public String getName() {
        return "RpcProtocol";
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
            if(log.isErrorEnabled()) log.error("exception=" + e);
            return null;
        }

        msg=new Message(null, null, buf);
        return castMessage(dests, msg, mode, timeout);
    }


    public Object callRemoteMethod(Address dest, String method_name, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        return callRemoteMethod(dest, method_name, new Object[]{}, new Class[]{}, mode, timeout);
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
            if(log.isErrorEnabled()) log.error("exception=" + e);
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

        if(req == null || req.getLength() == 0) {
            if(log.isErrorEnabled()) log.error("message or message buffer is null");
            return null;
        }

        try {
            body=req.getObject();
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception=" + e);
            return e;
        }

        if(body == null || !(body instanceof MethodCall)) {
            if(log.isErrorEnabled()) log.error("message does not contain a MethodCall object");
            return null;
        }

        method_call=(MethodCall)body;
        try {
            return method_call.invoke(this, method_lookup);
        }
        catch(Throwable x) {
            if(log.isErrorEnabled()) log.error(Util.getStackTrace(x));
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
