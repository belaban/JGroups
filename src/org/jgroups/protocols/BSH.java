// $Id: BSH.java,v 1.20 2008/04/08 14:51:21 belaban Exp $

package org.jgroups.protocols;


import bsh.EvalError;
import bsh.Interpreter;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;




/**
 * Beanshell (<a href=http://www.beanshell.org>www.beanshell.org</a>) interpreter class.
 * The <tt>eval()</tt> method receives Java code, executes it and returns the
 * result of the evaluation (or an exception).<p/>
 * This protocol is experimental
 * User: Bela
 * Date: Mar 8, 2003
 * Time: 1:57:07 PM
 * @author Bela Ban
 */
@Experimental
public class BSH extends Protocol {
    static final String name="BSH";
    Interpreter interpreter=null;

    public BSH() {
    }

    public String getName() {
        return name;
    }



    public void init() throws Exception {
    }

    public void start() throws Exception {
    }

    public void stop() {
        if(interpreter != null)
            destroyInterpreter();
    }

    public void destroy() {
    }


    public Object up(Event evt) {
        Header  h;
        Message msg;
        int     type;

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            h=msg.getHeader(name);
            if(h instanceof BshHeader) {
                type=((BshHeader)h).type;
                switch(type) {
                    case BshHeader.REQ:
                        handleRequest(msg.getSrc(), msg.getBuffer());
                        return null;
                    case BshHeader.RSP:
                        msg.putHeader(name, h);
                        up_prot.up(evt);
                        return null;
                    case BshHeader.DESTROY_INTERPRETER:
                        destroyInterpreter();
                        return null;
                    default:
                        if(log.isErrorEnabled()) log.error("header type was not REQ as expected (was " + type + ')');
                        return null;
                }
            }
        }
        return up_prot.up(evt);
    }


    void handleRequest(Address sender, byte[] buf) {
        Object retval;
        String code;


        if(buf == null) {
            if(log.isErrorEnabled()) log.error("buffer was null");
            return;
        }

        code=new String(buf);

        // create interpreter just-in-time
        if(interpreter == null) {
            interpreter=new Interpreter();

                if(log.isInfoEnabled()) log.info("beanshell interpreter was created");
            try {
                interpreter.set("bsh_prot", this);

                    if(log.isInfoEnabled()) log.info("set \"bsh_prot\" to " + this);
            }
            catch(EvalError err) {
                if(log.isErrorEnabled()) log.error("failed setting \"bsh_prot\": " + err);
            }

        }

        try {
            retval=interpreter.eval(code);

                if(log.isInfoEnabled()) log.info("eval: \"" + code +
                                                  "\", retval=" + retval);
        }
        catch(EvalError ex) {
            if(log.isErrorEnabled()) log.error("error is " + Util.getStackTrace(ex));
            retval=ex;
        }

        if(sender != null) {
            Message rsp=new Message(sender, null, null);

            // serialize the object if serializable, otherwise just send string
            // representation
            if(retval != null) {
                if(retval instanceof Serializable)
                    rsp.setObject((Serializable)retval);
                else
                    rsp.setObject(retval.toString());
            }


                if(log.isInfoEnabled()) log.info("sending back response " +
                                                  retval + " to " + rsp.getDest());
            rsp.putHeader(name, new BshHeader(BshHeader.RSP));
            down_prot.down(new Event(Event.MSG, rsp));
        }
    }


/* --------------------------- Callbacks ---------------------------- */
//    public Object eval(String code) throws Exception {
//        Object retval=null;
//        try {
//            retval=interpreter.eval(code);
//
//                if(log.isInfoEnabled()) log.info("BSH.eval()", "eval: \"" + code +
//                           "\", retval=" + retval);
//            if(retval != null && !(retval instanceof Serializable)) {
//                if(log.isErrorEnabled()) log.error("BSH.eval", "return value " + retval +
//                                        " is not serializable, cannot be sent back " +
//                                        "(returning null)");
//                return null;
//            }
//            return retval;
//        }
//        catch(EvalError ex) {
//            if(log.isErrorEnabled()) log.error("BSH.eval()", "error is " + Util.getStackTrace(ex));
//            return ex;
//        }
//    }
//
    public void destroyInterpreter() {
        interpreter=null; // allow it to be garbage collected

            if(log.isInfoEnabled()) log.info("beanshell interpreter was destroyed");
    }
    /* ------------------------ End of Callbacks ------------------------ */


    public static class BshHeader extends Header {
        public static final int REQ=1;
        public static final int RSP=2;
        public static final int DESTROY_INTERPRETER=3;
        int type=REQ;


        public BshHeader() {
        }

        public BshHeader(int type) {
            this.type=type;
        }

        public int size() {
            return 10;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(type == REQ)
                sb.append("REQ");
            else
                if(type == RSP)
                    sb.append("RSP");
                else
                    if(type == DESTROY_INTERPRETER)
                        sb.append("DESTROY_INTERPRETER");
                    else
                        sb.append("<unknown type>");
            return sb.toString();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
        }

    }

}
