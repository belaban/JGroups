// $Id: SIZE.java,v 1.3 2003/12/27 00:08:35 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.log.Trace;
import org.jgroups.stack.Protocol;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;
import java.util.Vector;


/**
 * Protocol which prints out the real size of a message. To do this, the message
 * is serialized into a byte buffer and its size read. Don't use this layer in
 * a production stack since the costs are high (just for debugging).
 * 
 * @author Bela Ban June 13 2001
 */
public class SIZE extends Protocol {
    Vector members=new Vector();
    boolean print_msg=false;

    /** Min size in bytes above which msgs should be printed */
    long min_size=0;

    ByteArrayOutputStream out_stream=new ByteArrayOutputStream(65535);


    /**
     * All protocol names have to be unique !
     */
    public String getName() {
        return "SIZE";
    }


    public void init() {
    }


    /**
     * Setup the Protocol instance acording to the configuration string
     */
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("print_msg");
        if(str != null) {
            print_msg=new Boolean(str).booleanValue();
            props.remove("print_msg");
        }

        str=props.getProperty("min_size");
        if(str != null) {
            min_size=Integer.parseInt(str);
            props.remove("min_size");
        }

        if(props.size() > 0) {
            System.err.println("SIZE.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }


    public void up(Event evt) {
        Message msg;
        int payload_size=0;
        byte[] buf;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                if(Trace.trace) {
                    if((buf=msg.getBuffer()) != null)
                        payload_size=buf.length;
                    if(payload_size > min_size) {
                        Trace.info("SIZE.up()", "size of message is " + sizeOf(msg) +
                                ", " + msg.getHeaders().size() + " headers");
                        if(print_msg)
                            Trace.info("SIZE.up()", "headers are " + msg.getHeaders() +
                                    ", payload size=" + payload_size);
                    }
                }
                break;
        }

        passUp(evt);            // Pass up to the layer above us
    }


    public void down(Event evt) {
        Message msg;
        int payload_size=0;
        byte[] buf;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                if(Trace.trace) {
                    if((buf=msg.getBuffer()) != null)
                        payload_size=buf.length;
                    if(payload_size > min_size) {
                        Trace.info("SIZE.down()", "size of message is " + sizeOf(msg) +
                                ", " + msg.getHeaders().size() + " headers");
                        if(print_msg)
                            Trace.info("SIZE.up()", "headers are " + msg.getHeaders() +
                                    ", payload size=" + payload_size);
                    }
                }
                break;
        }

        passDown(evt);          // Pass on to the layer below us
    }


    int sizeOf(Message msg) {
        ObjectOutputStream out;

        synchronized(out_stream) {
            try {
                out_stream.reset();
                out=new ObjectOutputStream(out_stream);
                msg.writeExternal(out);
                out.flush();
                return out_stream.size();
            }
            catch(Exception e) {
                return 0;
            }
        }
    }


}
