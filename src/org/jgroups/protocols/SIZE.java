// $Id: SIZE.java,v 1.7 2004/04/23 19:36:13 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
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
    public boolean setProperties(Properties props) {super.setProperties(props);
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
        int payload_size=0, serialized_size;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                 {
                    if((payload_size=msg.getLength()) > 0) {
                        serialized_size=sizeOf(msg);
                        if(serialized_size > min_size) {
                            if(log.isInfoEnabled()) log.info("size of message is " + serialized_size +
                                    ", " + msg.getHeaders().size() + " headers");
                            if(print_msg)
                                if(log.isInfoEnabled()) log.info("headers are " + msg.getHeaders() +
                                        ", payload size=" + payload_size);
                        }
                    }
                }
                break;
        }

        passUp(evt);            // Pass up to the layer above us
    }


    public void down(Event evt) {
        Message msg;
        int payload_size=0, serialized_size;
        byte[] buf;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                 {
                    if((payload_size=msg.getLength()) > 0) {
                        serialized_size=sizeOf(msg);
                        if(serialized_size > min_size) {
                            if(log.isInfoEnabled()) log.info("size of message is " + serialized_size +
                                    ", " + msg.getHeaders().size() + " headers");
                            if(print_msg)
                                if(log.isInfoEnabled()) log.info("headers are " + msg.getHeaders() +
                                        ", payload size=" + payload_size);
                        }
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
