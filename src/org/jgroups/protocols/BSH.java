// $Id: BSH.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.protocols;


import bsh.EvalError;
import bsh.Interpreter;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.log.Trace;
import org.jgroups.stack.Protocol;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Properties;




/**
 * Beanshell (<a href=http://www.beanshell.org>"www.beanshell.org"</a>) interpreter class.
 * The <tt>eval()</tt> method receives Java code, executes it and returns the
 * result of the evaluation (or an exception).
 * User: Bela
 * Date: Mar 8, 2003
 * Time: 1:57:07 PM
 * @author Bela Ban
 */
public class BSH extends Protocol {
    final String name="BSH";
    Interpreter interpreter=null;

    public BSH() {
        ;
    }

    public String getName() {
        return name;
    }

    public boolean setProperties(Properties props) {
        return super.setProperties(props);
    }

    public void init() throws Exception {
        ;
    }

    public void start() throws Exception {
        ;
    }

    public void stop() {
        if(interpreter != null)
            destroyInterpreter();
    }

    public void destroy() {
        ;
    }

    /** We have no up handler thread */
    public void startUpHandler() {
        ;
    }

    /** We have no down handler thread */
    public void startDownHandler() {
        ;
    }

    public void up(Event evt) {
        Header  h;
        Message msg;
        int     type;

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            h=msg.removeHeader(name);
            if(h != null && h instanceof BshHeader) {
                type=((BshHeader)h).type;
                switch(type) {
                    case BshHeader.REQ:
                        handleRequest(msg.getSrc(), msg.getBuffer());
                        return;
                    case BshHeader.RSP:
                        msg.putHeader(name, h);
                        passUp(evt);
                        return;
                    case BshHeader.DESTROY_INTERPRETER:
                        destroyInterpreter();
                        return;
                    default:
                        Trace.error("BSH.handleRequest()",
                                    "header type was not REQ as expected" +
                                    " (was " + type + ")");
                        return;
                }
            }
        }
        passUp(evt);
    }


    void handleRequest(Address sender, byte[] buf) {
        Object retval;
        String code;


        if(buf == null) {
            Trace.error("BSH.handleRequest()", "buffer was null");
            return;
        }

        code=new String(buf);

        // create interpreter just-in-time
        if(interpreter == null) {
            interpreter=new Interpreter();
            if(Trace.trace)
                Trace.info("BSH.handleRequest()", "beanshell interpreter was created");
            try {
                interpreter.set("bsh_prot", this);
                if(Trace.trace)
                    Trace.info("BSH.handleRequest()", "set \"bsh_prot\" to " + this);
            }
            catch(EvalError err) {
                Trace.error("BSH.handleRequest()", "failed setting \"bsh_prot\": " + err);
            }

        }

        try {
            retval=interpreter.eval(code);
            if(Trace.trace)
                Trace.info("BSH.handleRequest()", "eval: \"" + code +
                                                  "\", retval=" + retval);
        }
        catch(EvalError ex) {
            Trace.error("BSH.handleRequest()", "error is " + Trace.getStackTrace(ex));
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

            if(Trace.trace)
                Trace.info("BSH.handleRequest()", "sending back response " +
                                                  retval + " to " + rsp.getDest());
            rsp.putHeader(name, new BshHeader(BshHeader.RSP));
            passDown(new Event(Event.MSG, rsp));
        }
    }


/* --------------------------- Callbacks ---------------------------- */
//    public Object eval(String code) throws Exception {
//        Object retval=null;
//        try {
//            retval=interpreter.eval(code);
//            if(Trace.trace)
//                Trace.info("BSH.eval()", "eval: \"" + code +
//                           "\", retval=" + retval);
//            if(retval != null && !(retval instanceof Serializable)) {
//                Trace.error("BSH.eval", "return value " + retval +
//                                        " is not serializable, cannot be sent back " +
//                                        "(returning null)");
//                return null;
//            }
//            return retval;
//        }
//        catch(EvalError ex) {
//            Trace.error("BSH.eval()", "error is " + Trace.getStackTrace(ex));
//            return ex;
//        }
//    }
//
    public void destroyInterpreter() {
        interpreter=null; // allow it to be garbage collected
        if(Trace.trace)
            Trace.info("BSH.destroyInterpreter()", "beanshell interpreter was destroyed");
    }
    /* ------------------------ End of Callbacks ------------------------ */


    public static class BshHeader extends Header {
        public static final int REQ=1;
        public static final int RSP=2;
        public static final int DESTROY_INTERPRETER=3;
        int type=REQ;


        public BshHeader() {
            ; // used by externalization
        }

        public BshHeader(int type) {
            this.type=type;
        }

        public long size() {
            return 10;
        }

        public String toString() {
            StringBuffer sb=new StringBuffer();
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
