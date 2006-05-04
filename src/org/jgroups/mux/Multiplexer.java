package org.jgroups.mux;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.jgroups.stack.StateTransferInfo;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.util.*;

/**
 * Used for dispatching incoming messages. The Multiplexer implements UpHandler and registers with the associated
 * JChannel (there can only be 1 Multiplexer per JChannel). When up() is called with a message, the header of the
 * message is removed and the MuxChannel corresponding to the header's application ID is retrieved from the map,
 * and MuxChannel.up() is called with the message.
 * @author Bela Ban
 * @version $Id: Multiplexer.java,v 1.8 2006/05/04 06:51:16 belaban Exp $
 */
public class Multiplexer implements UpHandler {
    /** Map<String,MuxChannel>. Maintains the mapping between application IDs and their associated MuxChannels */
    private final Map apps=new HashMap();
    private final JChannel channel;
    static final Log log=LogFactory.getLog(Multiplexer.class);
    static final String SEPARATOR="::";
    static final short SEPARATOR_LEN=(short)SEPARATOR.length();
    static final String LIST_SEPARATOR=";";

    /** Map<String,Boolean>. Map of application IDs and booleans that determine whether getState() has already been called */
    private final Map state_transfer_listeners=new HashMap();

    /** Set<String>. Set (no duplicates) of application IDs */
    // private final Set state_transfer_rsps=new HashSet();

    // private String current_state_id=null;


    public Multiplexer() {
        this.channel=null;
    }

    public Multiplexer(JChannel channel) {
        this.channel=channel;
        this.channel.setUpHandler(this);
    }

    public Set getApplicationIds() {
        return apps != null? apps.keySet() : null;
    }

    public boolean stateTransferListenersPresent() {
        return state_transfer_listeners != null && state_transfer_listeners.size() > 0;
    }

    public synchronized void registerForStateTransfer(String appl_id, String substate_id) {
        String key=appl_id;
        if(substate_id != null && substate_id.length() > 0)
            key+=SEPARATOR + substate_id;
        state_transfer_listeners.put(key, Boolean.FALSE);
    }

    public synchronized boolean getState(Address target, String id, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        if(state_transfer_listeners == null)
            return false;
        Map.Entry entry;
        String key;
        for(Iterator it=state_transfer_listeners.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(String)entry.getKey();
            int index=key.indexOf(SEPARATOR);
            boolean match;
            if(index > -1) {
                String tmp=key.substring(0, index);
                match=id.equals(tmp);
            }
            else {
                match=id.equals(key);
            }
            if(match) {
                entry.setValue(Boolean.TRUE);
                break;
            }
        }

        Collection values=state_transfer_listeners.values();
        boolean all_true=Util.all(values, Boolean.TRUE);
        if(!all_true)
            return true; // pseudo

        boolean rc=false;
        try {
            startFlush();
            Set keys=new HashSet(state_transfer_listeners.keySet());
            rc=fetchApplicationStates(target, keys, timeout);
            state_transfer_listeners.clear();
        }
        finally {
            stopFlush();
        }
        return rc;
    }

    /** Fetches the app states for all application IDs in keys.
     * The keys are a duplicate list, so it cannot be modified by the caller of this method
     * @param keys
     */
    private boolean fetchApplicationStates(Address target, Set keys, long timeout) throws ChannelClosedException, ChannelNotConnectedException {
        // String combined_id=Util.generateList(keys, ";");
        // return channel.getState(target, combined_id, timeout);

        boolean rc, all_rcs=true;
        String appl_id;
        for(Iterator it=keys.iterator(); it.hasNext();) {
            appl_id=(String)it.next();
            rc=channel.getState(target, appl_id, timeout);
            if(rc == false)
                all_rcs=false;
        }
        return all_rcs;
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
            MuxChannel ch=new MuxChannel(f, channel, id, stack_name, this);
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



/*    private void handleStateRequest(Event evt) {
        StateTransferInfo info=(StateTransferInfo)evt.getArg();
        String state_id=info.state_id;
        MuxChannel mux_ch;

        // state_id might be "myID" or "myID::mySubID". if it is null, get all substates
        // could be a list, e.g. "myID::subID;mySecondID::subID-2;myThirdID::subID-3 etc

        // current_state_id=state_id;

        // List ids=parseStateIds(state_id);
        synchronized(state_transfer_rsps) {
            for(Iterator it=ids.iterator(); it.hasNext();) {
                String id=(String)it.next();
                String appl_id=id;  // e.g. "myID::myStateID"
                int index=id.indexOf(SEPARATOR);
                if(index > -1) {
                    appl_id=id.substring(0, index);  // similar reuse as above...
                }
                state_transfer_rsps.add(appl_id);
            }
        }

        for(Iterator it=ids.iterator(); it.hasNext();) {
            String id=(String)it.next();
            _handleStateRequest(id, evt, info);
        }
    }*/


    private void handleStateRequest(Event evt) {
        StateTransferInfo info=(StateTransferInfo)evt.getArg();
        String id=info.state_id;
        MuxChannel mux_ch=null;

        try {

            int index=id.indexOf(SEPARATOR);
            if(index > -1) {
                info.state_id=id.substring(index + SEPARATOR_LEN);
                id=id.substring(0, index);  // similar reuse as above...
            }
            else {
                info.state_id=null;
            }

            mux_ch=(MuxChannel)apps.get(id);
            if(mux_ch == null)
                throw new IllegalArgumentException("didn't find application with ID=" + id + " to fetch state from");

            // evt.setArg(info);
            mux_ch.up(evt); // state_id will be null, get regular state from tha application named state_id
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            mux_ch.returnState(null, id); // todo: check whether id is correct
        }
    }


//    /** state_id: "myID;myID:mySubID;myID3" */
//    private List parseStateIds(String state_id) {
//        return Util.parseStringList(state_id, LIST_SEPARATOR);
//    }


    private void handleStateResponse(Event evt) {
        StateTransferInfo info=(StateTransferInfo)evt.getArg();
        MuxChannel mux_ch;

        // state_id might be "myID" or "myID::mySubID". if it is null, get all substates
        // could be a list, e.g. "myID::subID;mySecondID::subID-2;myThirdID::subID-3 etc

//        if(info.last_chunk) {
//            System.out.println("last chunk - is ignored");
//            return;
//        }

        // List states=Util.parseStringList(info.state_id, ";");
        String appl_id, substate_id, tmp;
        //for(Iterator it=states.iterator(); it.hasNext();) {
        tmp=info.state_id;

        int index=tmp.indexOf(SEPARATOR);
        if(index > -1) {
            appl_id=tmp.substring(0, index);
            substate_id=tmp.substring(index+SEPARATOR_LEN);
        }
        else {
            appl_id=tmp;
            substate_id=null;
        }

        mux_ch=(MuxChannel)apps.get(appl_id);
        if(mux_ch == null) {
            log.error("didn't find application with ID=" + appl_id + " to fetch state from");
        }
        else {
            StateTransferInfo tmp_info=info.copy();
            tmp_info.state_id=substate_id;
            evt.setArg(tmp_info);
            mux_ch.up(evt); // state_id will be null, get regular state from the application named state_id
        }
        //}
    }


//    public static void main(String[] args) {
//        Multiplexer mux=new Multiplexer();
//
//        String tmp="c::substate-1;c2;c3::substate-2";
//        Event evt=new Event(Event.START, new StateTransferInfo(null, tmp, 0, null, true));
//
//        mux.handleStateResponse(evt);
//    }


/*    public void returnState(byte[] state, String id) { // id might be "myID" or "myID::mySubstateID"
        //synchronized(state_transfer_rsps) {
            //boolean was_present=state_transfer_rsps.remove(id);
            if(!was_present) {
                channel.returnState(state, id);
                return;
            }

            channel.returnState(state, id, false); // this is not the last chunk of the state response
            if(state_transfer_rsps.size() == 0) {
                channel.returnState(state, current_state_id, true); // this *is* the last chunk of the state response
                current_state_id=null;
            }
       // }
    }*/

    /** Tell the underlying channel to start the flush protocol, this will be handled by FLUSH */
    void startFlush() {

    }

    /** Tell the underlying channel to stop the flush, and resume message sending. This will be handled by FLUSH */
    void stopFlush() {

    }



    /** Push all IDs and all byte[] buffer into a common byte[] buffer */
//    private byte[] generateCombinedState() {
//        byte[] metadata;
//        synchronized(state_transfer_rsps) {
//            int num=state_transfer_rsps.size();
//
//        }
//
//
//        return null; // todo: implement
//    }

//
//    private static class StateResponse {
//        boolean received=false;
//        byte[] state=null;
//    }


}
