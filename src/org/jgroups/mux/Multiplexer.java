package org.jgroups.mux;

import org.jgroups.*;
import org.jgroups.stack.StateTransferInfo;
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
 * @version $Id: Multiplexer.java,v 1.5 2006/03/17 11:10:15 belaban Exp $
 */
public class Multiplexer implements UpHandler {
    /** Map<String,MuxChannel>. Maintains the mapping between application IDs and their associated MuxChannels */
    private final Map apps=new HashMap();
    private final JChannel channel;
    static final Log log=LogFactory.getLog(Multiplexer.class);
    static final String SEPARATOR="::";


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
                passToAllMuxChannels(evt);
                break;

            case Event.SUSPECT:
                passToAllMuxChannels(evt);
                break;

            case Event.GET_APPLSTATE:
                handleStateRequest(evt);
                break;

            case Event.GET_STATE_OK:
                handleStateResponse(evt);
                break;

            default:
                passToAllMuxChannels(evt);
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


    private void passToAllMuxChannels(Event evt) {
        for(Iterator it=apps.values().iterator(); it.hasNext();) {
            MuxChannel ch=(MuxChannel)it.next();
            ch.up(evt);
        }
    }

    public MuxChannel remove(String id) {
        synchronized(apps) {
            return (MuxChannel)apps.remove(id);
        }
    }

    

    /** Closes the underlying JChannel is all MuxChannels have been disconnected */
    public void disconnect() {
        MuxChannel mux_ch;
        boolean all_disconnected=true;
        synchronized(apps) {
            for(Iterator it=apps.values().iterator(); it.hasNext();) {
                mux_ch=(MuxChannel)it.next();
                if(mux_ch.isConnected()) {
                    all_disconnected=false;
                    break;
                }
            }
            if(all_disconnected) {
                if(log.isTraceEnabled()) {
                    log.trace("disconnecting underlying JChannel as all MuxChannels are disconnected");
                }
                channel.disconnect();
            }
        }
    }

    public void close() {
        MuxChannel mux_ch;
        boolean all_closed=true;
        synchronized(apps) {
            for(Iterator it=apps.values().iterator(); it.hasNext();) {
                mux_ch=(MuxChannel)it.next();
                if(mux_ch.isOpen()) {
                    all_closed=false;
                    break;
                }
            }
            if(all_closed) {
                if(log.isTraceEnabled()) {
                    log.trace("closing underlying JChannel as all MuxChannels are closed");
                }
                channel.close();
                apps.clear();
            }
        }
    }

    public void closeAll() {
        synchronized(apps) {
            MuxChannel mux_ch;
            for(Iterator it=apps.values().iterator(); it.hasNext();) {
                mux_ch=(MuxChannel)it.next();
                mux_ch.setConnected(false);
                mux_ch.setClosed(true);
                mux_ch.closeMessageQueue(true);
            }
        }
    }

    public void shutdown() {
        MuxChannel mux_ch;
        boolean all_closed=true;
        synchronized(apps) {
            for(Iterator it=apps.values().iterator(); it.hasNext();) {
                mux_ch=(MuxChannel)it.next();
                if(mux_ch.isOpen()) {
                    all_closed=false;
                    break;
                }
            }
            if(all_closed) {
                if(log.isTraceEnabled()) {
                    log.trace("shutting down underlying JChannel as all MuxChannels are closed");
                }
                channel.shutdown();
                apps.clear();
            }
        }
    }



    private void handleStateRequest(Event evt) {
        StateTransferInfo info=(StateTransferInfo)evt.getArg();
        String state_id=info.state_id;
        MuxChannel mux_ch;

        // state_id might be "myID" or "myID::mySubID". if it is null, get all substates

        info.state_id=null;

        int index=state_id.indexOf(SEPARATOR);
        if(index > -1) {
            state_id=state_id.substring(0, index);
            info.state_id=state_id.substring(index+1, state_id.length()-1);
        }

        mux_ch=(MuxChannel)apps.get(state_id);
        if(mux_ch == null) {
            log.error("didn't find application with ID=" + state_id + " to fetch state from");
        }
        else {
            mux_ch.up(evt); // state_id will be null, get regular state from tha application named state_id
        }
    }


    private void handleStateResponse(Event evt) {
        StateTransferInfo info=(StateTransferInfo)evt.getArg();
        String state_id=info.state_id;
        MuxChannel mux_ch;

        // state_id might be "myID" or "myID::mySubID". if it is null, get all substates

        info.state_id=null;

        int index=state_id.indexOf(SEPARATOR);
        if(index > -1) {
            state_id=state_id.substring(0, index);
            info.state_id=state_id.substring(index+1, state_id.length()-1);
        }

        mux_ch=(MuxChannel)apps.get(state_id);
        if(mux_ch == null) {
            log.error("didn't find application with ID=" + state_id + " to fetch state from");
        }
        else {
            mux_ch.up(evt); // state_id will be null, get regular state from tha application named state_id
        }
    }


}
