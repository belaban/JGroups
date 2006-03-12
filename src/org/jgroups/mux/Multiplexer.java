package org.jgroups.mux;

import org.jgroups.*;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Used for dispatching incoming messages. The Multiplexer implements UpHandler and registers with the associated
 * JChannel (there can only be 1 Multiplexer per JChannel). When up() is called with a message, the header of the
 * message is removed and the MuxChannel corresponding to the header's application ID is retrieved from the map,
 * and MuxChannel.up() is called with the message.
 * @author Bela Ban
 * @version $Id: Multiplexer.java,v 1.1 2006/03/12 11:49:27 belaban Exp $
 */
public class Multiplexer implements UpHandler {
    /** Map<String,MuxChannel>. Maintains the mapping between application IDs and their associated MuxChannels */
    private final Map apps=new HashMap();
    private final JChannel channel;
    static final Log log=LogFactory.getLog(Multiplexer.class);


    public Multiplexer() {
        this.channel=null;
    }

    public Multiplexer(JChannel channel) {
        this.channel=channel;
        this.channel.setUpHandler(this);
    }

    public void up(Event evt) {
        // remove header and dispatch to correct MuxChannel
        MuxHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                hdr=(MuxHeader)msg.getHeader("MUX");
                if(hdr == null) {
                    log.error("MuxHeader not present - discarding message " + msg);
                    return;
                }
                MuxChannel mux_ch=(MuxChannel)apps.get(hdr.id);
                if(mux_ch == null) {
                    log.error("didn't find an application for id=" + hdr.id + " discarding messgage " + msg);
                    return;
                }
                if(log.isTraceEnabled())
                    log.trace("dispatching message to " + hdr.id);
                mux_ch.up(evt);
                break;
            case Event.VIEW_CHANGE:
                // throw new UnsupportedOperationException("VIEW_CHANGE event");
                for(Iterator it=apps.values().iterator(); it.hasNext();) {
                    MuxChannel ch=(MuxChannel)it.next();
                    ch.up(evt);
                }
                break;
            case Event.SUSPECT:
                throw new UnsupportedOperationException("SUSPECT event");
                //break;
            default:
                log.warn("don't know what to do with event " + evt);
                break;
        }
    }


    public Channel createMuxChannel(JChannelFactory f, String id, String stack_name) throws Exception {
        synchronized(apps) {
            if(apps.containsKey(id))
                throw new Exception("application ID \"" + id + "\" is already registered, cannot register duplicate ID");
            MuxChannel ch=new MuxChannel(f, channel, id, stack_name);
            apps.put(id, ch);
            return ch;
        }
    }
}
