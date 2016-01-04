
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Buffer;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.NotSerializableException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;


/**
 * Framework to send requests and receive matching responses (matching on
 * request ID).
 * Multiple requests can be sent at a time. Whenever a response is received,
 * the correct <code>RspCollector</code> is looked up (key = id) and its
 * method <code>receiveResponse()</code> invoked. A caller may use
 * <code>done()</code> to signal that no more responses are expected, and that
 * the corresponding entry may be removed.
 * <p>
 * <code>RequestCorrelator</code> can be installed at both client and server
 * sides, it can also switch roles dynamically; i.e., send a request and at
 * the same time process an incoming request (when local delivery is enabled,
 * this is actually the default).
 * <p>
 *
 * @author Bela Ban
 */
public class RequestCorrelator {

    /** The protocol layer to use to pass up/down messages. Can be either a Protocol or a Transport */
    protected Protocol                               transport;

    /** The table of pending requests (keys=Long (request IDs), values=<tt>RequestEntry</tt>) */
    protected final ConcurrentMap<Long,RspCollector> requests=Util.createConcurrentMap();


    /** The handler for the incoming requests. It is called from inside the dispatcher thread */
    protected RequestHandler                         request_handler;

    /** Possibility for an external marshaller to marshal/unmarshal responses */
    protected RpcDispatcher.Marshaller               marshaller;

    /** makes the instance unique (together with IDs) */
    protected short                                  id=ClassConfigurator.getProtocolId(this.getClass());

    /** The address of this group member */
    protected Address                                local_addr;

    protected volatile View                          view;

    protected boolean                                started=false;

    /** Whether or not to use async dispatcher */
    protected boolean                                async_dispatching=false;

    // send exceptions back wrapped in an {@link InvocationTargetException}, or not
    protected boolean                                wrap_exceptions=true;

    private final MyProbeHandler                     probe_handler=new MyProbeHandler(requests);

    protected static final Log                       log=LogFactory.getLog(RequestCorrelator.class);


    /**
     * Constructor. Uses transport to send messages. If <code>handler</code>
     * is not null, all incoming requests will be dispatched to it (via
     * <code>handle(Message)</code>).
     *
     * @param id Used to differentiate between different RequestCorrelators
     * (e.g. in different protocol layers). Has to be unique if multiple
     * request correlators are used.
     *
     * @param transport Used to send/pass up requests. Is a Protocol (up_prot.up()/down_prot.down() will be used)
     *
     * @param handler Request handler. Method <code>handle(Message)</code>
     * will be called when a request is received.
     */
    public RequestCorrelator(short id, Protocol transport, RequestHandler handler, Address local_addr) {
        this.id         = id;
        this.transport  = transport;
        this.local_addr = local_addr;
        request_handler = handler;
        start();
    }

    public RequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr) {
        this.transport  = transport;
        this.local_addr = local_addr;
        request_handler = handler;
        start();
    }



    public void setRequestHandler(RequestHandler handler) {
        request_handler=handler;
        start();
    }



    public RpcDispatcher.Marshaller getMarshaller()                {return marshaller;}
    public void                     setMarshaller(RpcDispatcher.Marshaller marshaller) {this.marshaller=marshaller;}
    public boolean                  asyncDispatching()             {return async_dispatching;}
    public RequestCorrelator        asyncDispatching(boolean flag) {async_dispatching=flag; return this;}
    public boolean                  wrapExceptions()               {return wrap_exceptions;}
    public RequestCorrelator        wrapExceptions(boolean flag)   {wrap_exceptions=flag; return this;}

    public void sendRequest(long id, List<Address> dest_mbrs, Message msg, RspCollector coll) throws Exception {
        sendRequest(id, dest_mbrs, msg, coll, new RequestOptions().setAnycasting(false));
    }

    /**
     * Sends a request to a group. If no response collector is given, no responses are expected (making the call asynchronous)
     *
     * @param id The request ID. Must be unique for this JVM (e.g. current time in millisecs)
     * @param dest_mbrs The list of members who should receive the call. Usually a group RPC
     *                  is sent via multicast, but a receiver drops the request if its own address
     *                  is not in this list. Will not be used if it is null.
     * @param msg The request to be sent. The body of the message carries
     * the request data
     *
     * @param coll A response collector (usually the object that invokes this method). Its methods
     * <code>receiveResponse()</code> and <code>suspect()</code> will be invoked when a message has been received
     * or a member is suspected, respectively.
     */
    public void sendRequest(long id, Collection<Address> dest_mbrs, Message msg, RspCollector coll, RequestOptions options) throws Exception {
        if(transport == null) {
            if(log.isWarnEnabled()) log.warn("transport is not available !");
            return;
        }

        // i.   Create the request correlator header and add it to the msg
        // ii.  If a reply is expected (coll != null), add a coresponding entry in the pending requests table
        // iii. If deadlock detection is enabled, set/update the call stack
        // iv.  Pass the msg down to the protocol layer below
        Header hdr=options.hasExclusionList()?
                new MultiDestinationHeader(Header.REQ, id, (coll != null), this.id, options.exclusionList())
                : new Header(Header.REQ, id, (coll != null), this.id);

        msg.putHeader(this.id, hdr);

        if(coll != null) {
            addEntry(hdr.id, coll);
            // make sure no view is received before we add ourself as a view handler (https://issues.jboss.org/browse/JGRP-1428)
            coll.viewChange(view);
        }

        if(options.getAnycasting()) {
            if(options.useAnycastAddresses()) {
                Message copy=msg.copy(true);
                AnycastAddress dest=new AnycastAddress(dest_mbrs);
                copy.setDest(dest);
                transport.down(new Event(Event.MSG, copy));
            }
            else {
                for(Address mbr: dest_mbrs) {
                    Message copy=msg.copy(true);
                    copy.setDest(mbr);
                    if(!mbr.equals(local_addr) && copy.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK))
                        copy.clearTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
                    transport.down(new Event(Event.MSG, copy));
                }
            }
        }
        else
            transport.down(new Event(Event.MSG, msg));
    }

    /**
     * Sends a request to a single destination
     * @param id
     * @param target
     * @param msg
     * @param coll
     * @throws Exception
     */
    public void sendUnicastRequest(long id, Address target, Message msg, RspCollector coll) throws Exception {
        if(transport == null) {
            if(log.isWarnEnabled()) log.warn("transport is not available !");
            return;
        }

        // i.   Create the request correlator header and add it to the msg
        // ii.  If a reply is expected (coll != null), add a coresponding entry in the pending requests table
        // iii. If deadlock detection is enabled, set/update the call stack
        // iv.  Pass the msg down to the protocol layer below
        Header hdr=new Header(Header.REQ, id, (coll != null), this.id);
        msg.putHeader(this.id, hdr);

        if(coll != null) {
            addEntry(hdr.id, coll);
            // make sure no view is received before we add ourself as a view handler (https://issues.jboss.org/browse/JGRP-1428)
            coll.viewChange(view);
        }

        transport.down(new Event(Event.MSG, msg));
    }





    /**
     * Used to signal that a certain request may be garbage collected as all responses have been received.
     */
    public void done(long id) {
        removeEntry(id);
    }


    /**
     * <b>Callback</b>.
     * <p>
     * Called by the protocol below when a message has been received. The
     * algorithm should test whether the message is destined for us and,
     * if not, pass it up to the next layer. Otherwise, it should remove
     * the header and check whether the message is a request or response.
     * In the first case, the message will be delivered to the request
     * handler registered (calling its <code>handle()</code> method), in the
     * second case, the corresponding response collector is looked up and
     * the message delivered.
     * @param evt The event to be received
     * @return Whether or not the event was consumed. If true, don't pass message up, else pass it up
     */
    public boolean receive(Event evt) {
        switch(evt.getType()) {

            case Event.SUSPECT:     // don't wait for responses from faulty members
                receiveSuspect((Address)evt.getArg());
                break;

            case Event.VIEW_CHANGE: // adjust number of responses to wait for
                receiveView((View)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                setLocalAddress((Address)evt.getArg());
                break;

            case Event.MSG:
                if(receiveMessage((Message)evt.getArg()))
                    return true; // message was consumed, don't pass it up
                break;
            case Event.SITE_UNREACHABLE:
                SiteMaster site_master=(SiteMaster)evt.getArg();
                String site=site_master.getSite();
                setSiteUnreachable(site);
                break; // let others have a stab at this event, too
        }
        return false;
    }


    public final void start() {
        started=true;
    }

    public void stop() {
        started=false;
        for(RspCollector coll: requests.values())
            coll.transportClosed();
        requests.clear();
    }


    public void registerProbeHandler(TP transport) {
        if(transport != null)
            transport.registerProbeHandler(probe_handler);
    }

    public void unregisterProbeHandler(TP transport) {
        if(transport != null)
            transport.unregisterProbeHandler(probe_handler);
    }
    // .......................................................................



    /**
     * <tt>Event.SUSPECT</tt> event received from a layer below.
     * <p>
     * All response collectors currently registered will
     * be notified that <code>mbr</code> may have crashed, so they won't
     * wait for its response.
     */
    public void receiveSuspect(Address mbr) {
        if(mbr == null) return;
        if(log.isDebugEnabled()) log.debug("suspect=" + mbr);

        // copy so we don't run into bug #761804 - Bela June 27 2003
        // copy=new ArrayList(requests.values()); // removed because ConcurrentReaderHashMap can tolerate concurrent mods (bela May 8 2006)
        for(RspCollector coll: requests.values()) {
            if(coll != null)
                coll.suspect(mbr);
        }
    }


    /** An entire site is down; mark all requests that point to that site as unreachable (used by RELAY2) */
    public void setSiteUnreachable(String site) {
        for(RspCollector coll: requests.values()) {
            if(coll != null)
                coll.siteUnreachable(site);
        }
    }


    /**
     * <tt>Event.VIEW_CHANGE</tt> event received from a layer below.
     * <p>
     * Mark all responses from members that are not in new_view as
     * NOT_RECEIVED.
     *
     */
    public void receiveView(View new_view) {
        // ArrayList    copy;
        // copy so we don't run into bug #761804 - Bela June 27 2003
        // copy=new ArrayList(requests.values());  // removed because ConcurrentHashMap can tolerate concurrent mods (bela May 8 2006)
        view=new_view; // move this before the iteration (JGRP-1428)
        for(RspCollector coll: requests.values()) {
            if(coll != null)
                coll.viewChange(new_view);
        }
    }


    /**
     * Handles a message coming from a layer below
     *
     * @return true if the message was consumed, don't pass it further up, else false
     */
    public boolean receiveMessage(Message msg) {
        Header hdr=(Header)msg.getHeader(this.id);
        if(hdr == null)
            return false;

        // Check if the message was sent by a request correlator with the same name;
        // there may be multiple request correlators in the same protocol stack
        if(hdr.corrId != this.id) {
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("id of request correlator header (").append(hdr.corrId).
                          append(") is different from ours (").append(this.id).append("). Msg not accepted, passed up"));
            return false;
        }

        if(hdr instanceof MultiDestinationHeader) {
            // if we are part of the exclusion list, then we discard the request (addressed to different members)
            Address[] exclusion_list=((MultiDestinationHeader)hdr).exclusion_list;
            if(exclusion_list != null && local_addr != null && Util.contains(local_addr, exclusion_list)) {
                if(log.isTraceEnabled())
                    log.trace("%s: discarded request from %s as we are in the exclusion list, hdr=",
                              local_addr, msg.getSrc(), hdr);
                return true; // don't pass this message further up
            }
        }

        // [Header.REQ]:
        // i. If there is no request handler, discard
        // ii. Check whether priority: if synchronous and call stack contains
        // address that equals local address -> add priority request. Else
        // add normal request.
        //
        // [Header.RSP]:
        // Remove the msg request correlator header and notify the associated
        // <tt>RspCollector</tt> that a reply has been received
        switch(hdr.type) {
            case Header.REQ:
                handleRequest(msg, hdr);
                break;

            case Header.RSP:
            case Header.EXC_RSP:
                RspCollector coll=requests.get(hdr.id);
                if(coll != null) {
                    boolean is_exception=hdr.type == Header.EXC_RSP;
                    Address sender=msg.getSrc();
                    Object retval;
                    byte[] buf=msg.getRawBuffer();
                    int offset=msg.getOffset(), length=msg.getLength();
                    try {
                        retval=marshaller != null? marshaller.objectFromBuffer(buf, offset, length) :
                                Util.objectFromByteBuffer(buf, offset, length);
                    }
                    catch(Exception e) {
                        log.error(Util.getMessage("FailedUnmarshallingBufferIntoReturnValue"), e);
                        retval=e;
                        is_exception=true;
                    }
                    coll.receiveResponse(retval, sender, is_exception);
                }
                break;

            default:
                msg.getHeader(this.id);
                if(log.isErrorEnabled()) log.error(Util.getMessage("HeaderSTypeIsNeitherREQNorRSP"));
                break;
        }

        return true; // message was consumed
    }

    public Address getLocalAddress() {
        return local_addr;
    }

    public void setLocalAddress(Address local_addr) {
        this.local_addr=local_addr;
    }


    // .......................................................................

    /**
     * Add an association of:<br>
     * ID -> <tt>RspCollector</tt>
     */
    private void addEntry(long id, RspCollector coll) {
        requests.putIfAbsent(id, coll);
    }


    /**
     * Remove the request entry associated with the given ID
     *
     * @param id the id of the <tt>RequestEntry</tt> to remove
     */
    private void removeEntry(long id) {
        // changed by bela Feb 28 2003 (bug fix for 690606)
        // changed back to use synchronization by bela June 27 2003 (bug fix for #761804),
        // we can do this because we now copy for iteration (viewChange() and suspect())
        requests.remove(id);
    }



    /**
     * Handle a request msg for this correlator
     * @param req the request msg
     */
    protected void handleRequest(Message req, Header hdr) {
        Object        retval;
        boolean       threw_exception=false;

        if(log.isTraceEnabled()) {
            log.trace(new StringBuilder("calling (").append((request_handler != null? request_handler.getClass().getName() : "null")).
                      append(") with request ").append(hdr.id));
        }
        if(async_dispatching && request_handler instanceof AsyncRequestHandler) {
            Response rsp=hdr.rsp_expected? new ResponseImpl(req, hdr.id) : null;
            try {
                ((AsyncRequestHandler)request_handler).handle(req, rsp);
            }
            catch(Throwable t) {
                if(rsp != null)
                    rsp.send(wrap_exceptions ? new InvocationTargetException(t) : t, true);
                else
                    log.error(local_addr + ": failed dispatching request asynchronously: " + t);
            }
            return;
        }

        try {
            retval=request_handler.handle(req);
        }
        catch(Throwable t) {
            threw_exception=true;
            retval=wrap_exceptions ? new InvocationTargetException(t) : t;
        }
        if(hdr.rsp_expected)
            sendReply(req, hdr.id, retval, threw_exception);
    }


    protected void sendReply(final Message req, final long req_id, Object reply, boolean is_exception) {
        Buffer rsp_buf;
        try {  // retval could be an exception, or a real value
            rsp_buf=marshaller != null? marshaller.objectToBuffer(reply) : Util.objectToBuffer(reply);
        }
        catch(Throwable t) {
            try {  // this call should succeed (all exceptions are serializable)
                rsp_buf=marshaller != null? marshaller.objectToBuffer(t) : Util.objectToBuffer(t);
                is_exception=true;
            }
            catch(NotSerializableException not_serializable) {
                if(log.isErrorEnabled()) log.error(Util.getMessage("FailedMarshallingRsp") + reply + "): not serializable");
                return;
            }
            catch(Throwable tt) {
                if(log.isErrorEnabled()) log.error(Util.getMessage("FailedMarshallingRsp") + reply + "): " + tt);
                return;
            }
        }

        Message rsp=req.makeReply().setFlag(req.getFlags()).setBuffer(rsp_buf)
          .clearFlag(Message.Flag.RSVP, Message.Flag.SCOPED, Message.Flag.INTERNAL); // JGRP-1940

       sendResponse(rsp, req_id, is_exception);
    }

    protected void sendResponse(Message rsp, long req_id, boolean is_exception) {
        prepareResponse(rsp);
        Header rsp_hdr=new Header(is_exception? Header.EXC_RSP : Header.RSP, req_id, false, id);
        rsp.putHeader(id, rsp_hdr);
        if(log.isTraceEnabled())
            log.trace(new StringBuilder("sending rsp for ").append(rsp_hdr.id).append(" to ").append(rsp.getDest()));

        transport.down(new Event(Event.MSG, rsp));
    }


    protected void prepareResponse(Message rsp) {
        ;
    }

    // .......................................................................


    protected class ResponseImpl implements Response {
        protected final Message req;
        protected final long    req_id;

        public ResponseImpl(Message req, long req_id) {
            this.req=req;
            this.req_id=req_id;
        }

        public void send(Object reply, boolean is_exception) {
            sendReply(req, req_id, reply, is_exception);
        }

        public void send(Message reply, boolean is_exception) {
            sendResponse(reply, req_id, is_exception);
        }
    }


    /**
     * The header for <tt>RequestCorrelator</tt> messages
     */
    public static class Header extends org.jgroups.Header {
        public static final byte REQ     = 0;
        public static final byte RSP     = 1;
        public static final byte EXC_RSP = 2; // exception

        /** Type of header: request or reply */
        public byte    type;
        /**
         * The id of this request to distinguish among other requests from the same <tt>RequestCorrelator</tt> */
        public long    id;

        /** msg is synchronous if true */
        public boolean rsp_expected;

        /** The unique ID of the associated <tt>RequestCorrelator</tt> */
        public short   corrId;



        /**
         * Used for externalization
         */
        public Header() {}

        /**
         * @param type type of header (<tt>REQ</tt>/<tt>RSP</tt>)
         * @param id id of this header relative to ids of other requests
         * originating from the same correlator
         * @param rsp_expected whether it's a sync or async request
         * @param corr_id The ID of the <tt>RequestCorrelator</tt> from which
         */
        public Header(byte type, long id, boolean rsp_expected, short corr_id) {
            this.type         = type;
            this.id           = id;
            this.rsp_expected = rsp_expected;
            this.corrId       = corr_id;
        }

        public String toString() {
            StringBuilder ret=new StringBuilder();
            ret.append("id=" + corrId + ", type=");
            switch(type) {
                case REQ: ret.append("REQ");
                    break;
                case RSP: ret.append("RSP");
                    break;
                case EXC_RSP: ret.append("EXC_RSP");
                    break;
                default: ret.append("<unknown>");
            }
            ret.append(", id=" + id);
            ret.append(", rsp_expected=" + rsp_expected);
            return ret.toString();
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(id,out);
            out.writeBoolean(rsp_expected);
            out.writeShort(corrId);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            id=Bits.readLong(in);
            rsp_expected=in.readBoolean();
            corrId=in.readShort();
        }

        public int size() {
            return Global.BYTE_SIZE  // type
              + Bits.size(id)        // id
              + Global.BYTE_SIZE     // rsp_expected
              + Global.SHORT_SIZE;   // corrId
        }
    }



    public static final class MultiDestinationHeader extends Header {
        /** Contains a list of members who should not receive the request (others will drop). Ignored if null */
        public Address[] exclusion_list;

        public MultiDestinationHeader() {
        }

        public MultiDestinationHeader(byte type, long id, boolean rsp_expected, short corr_id, Address[] exclusion_list) {
            super(type, id, rsp_expected, corr_id);
            this.exclusion_list=exclusion_list;
        }


        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            Util.writeAddresses(exclusion_list, out);
        }

        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            exclusion_list=Util.readAddresses(in);
        }

        public int size() {
            return (int)(super.size() + Util.size(exclusion_list));
        }

        public String toString() {
            String str=super.toString();
            if(exclusion_list != null)
                str=str+ ", exclusion_list=" + Arrays.toString(exclusion_list);
            return str;
        }
    }



    private static class MyProbeHandler implements DiagnosticsHandler.ProbeHandler {
        private final ConcurrentMap<Long,RspCollector> requests;

        private MyProbeHandler(ConcurrentMap<Long,RspCollector> requests) {
            this.requests=requests;
        }

        public Map<String, String> handleProbe(String... keys) {
            if(requests == null)
                return null;
            Map<String,String> retval=new HashMap<>();
            for(String key: keys) {
                if(key.equals("requests")) {
                    StringBuilder sb=new StringBuilder();
                    for(Map.Entry<Long,RspCollector> entry: requests.entrySet()) {
                        sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                    }
                    retval.put("requests", sb.toString());
                    break;
                }
            }
            return retval;
        }

        public String[] supportedKeys() {
            return new String[]{"requests"};
        }
    }




}
