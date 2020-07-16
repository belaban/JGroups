
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;


/**
 * Framework to send requests and receive matching responses (on request ID).
 * Multiple requests can be sent at a time. Whenever a response is received, the correct {@code Request} is looked up
 * (key = id) and its method {@code receiveResponse()} invoked.
 * @author Bela Ban
 */
public class RequestCorrelator {

    /** The protocol layer to use to pass up/down messages. Can be either a Protocol or a Transport */
    protected Protocol                               transport;

    /** The table of pending requests (keys=Long (request IDs), values=<tt>RequestEntry</tt>) */
    protected final ConcurrentMap<Long,Request<?>>   requests=Util.createConcurrentMap();

    /** To generate unique request IDs */
    protected static final AtomicLong                REQUEST_ID=new AtomicLong(1);

    /** The handler for the incoming requests. It is called from inside the dispatcher thread */
    protected RequestHandler                         request_handler;

    /** makes the instance unique (together with IDs) */
    protected short                                  corr_id=ClassConfigurator.getProtocolId(this.getClass());

    /** The address of this group member */
    protected Address                                local_addr;

    protected volatile View                          view;

    protected boolean                                started;

    /** Whether or not to use async dispatcher */
    protected boolean                                async_dispatching;

    // send exceptions back wrapped in an {@link InvocationTargetException}, or not
    protected boolean                                wrap_exceptions=false;

    protected final MyProbeHandler                   probe_handler=new MyProbeHandler();

    protected final RpcStats                         rpc_stats=new RpcStats(false);

    protected static final Log                       log=LogFactory.getLog(RequestCorrelator.class);


    /**
     * Constructor. Uses transport to send messages. If {@code handler} is not null, all incoming requests will be
     * dispatched to it (via {@code handle(Message)}).
     *
     * @param corr_id Used to differentiate between different RequestCorrelators (e.g. in different protocol layers).
     *                Has to be unique if multiple request correlators are used.
     * @param transport Used to send/pass up requests.
     * @param handler Request handler. Method {@code handle(Message)} will be called when a request is received.
     */
    public RequestCorrelator(short corr_id, Protocol transport, RequestHandler handler, Address local_addr) {
        this.corr_id=corr_id;
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

    public Address                getLocalAddress()              {return local_addr;}
    public RequestCorrelator      setLocalAddress(Address a)     {this.local_addr=a; return this;}
    public boolean                asyncDispatching()             {return async_dispatching;}
    public RequestCorrelator      asyncDispatching(boolean flag) {async_dispatching=flag; return this;}
    public boolean                wrapExceptions()               {return wrap_exceptions;}
    public RequestCorrelator      wrapExceptions(boolean flag)   {wrap_exceptions=flag; return this;}


    /**
     * Sends a request to a group. If no response collector is given, no responses are expected (making the call asynchronous)
     * @param dest_mbrs The list of members who should receive the call. Usually a group RPC is sent via multicast,
     *                  but a receiver drops the request if its own address is not in this list. Will not be used if it is null.
     * @param msg The message to be sent.
     * @param req A request (usually the object that invokes this method). Its methods {@code receiveResponse()} and
     *            {@code suspect()} will be invoked when a message has been received or a member is suspected.
     */
    public <T> void sendRequest(Collection<Address> dest_mbrs, Message msg, Request<T> req, RequestOptions opts) throws Exception {
        // i.   Create the request correlator header and add it to the msg
        // ii.  If a reply is expected (coll != null), add a coresponding entry in the pending requests table
        Header hdr=opts.hasExclusionList()? new MultiDestinationHeader(Header.REQ, 0, this.corr_id, opts.exclusionList())
          : new Header(Header.REQ, 0, this.corr_id);

        msg.putHeader(this.corr_id, hdr)
          .setFlag(opts.flags(), false).setFlag(opts.transientFlags(), true);

        if(req != null) { // sync
            long req_id=REQUEST_ID.getAndIncrement();
            req.requestId(req_id);
            hdr.requestId(req_id); // set the request-id only for *synchronous RPCs*
            if(log.isTraceEnabled())
                log.trace("%s: invoking multicast RPC [req-id=%d]", local_addr, req_id);
            requests.putIfAbsent(req_id, req);
            // make sure no view is received before we add ourself as a view handler (https://issues.jboss.org/browse/JGRP-1428)
            req.viewChange(view);
            if(rpc_stats.extendedStats())
                req.start_time=System.nanoTime();
        }
        else {  // async
            if(opts != null && opts.anycasting())
                rpc_stats.addAnycast(false, 0, dest_mbrs);
            else
                rpc_stats.add(RpcStats.Type.MULTICAST, null, false, 0);
        }

        if(opts.anycasting()) {
            boolean first=true;
            for(Address mbr: dest_mbrs) {
                Message copy=(first? msg : msg.copy(true, true)).setDest(mbr);
                first=false;
                if(!mbr.equals(local_addr) && copy.isFlagSet(Message.TransientFlag.DONT_LOOPBACK))
                    copy.clearFlag(Message.TransientFlag.DONT_LOOPBACK);
                transport.down(copy);
            }
        }
        else
            transport.down(msg);
    }


    /** Sends a request to a single destination */
    public <T> void sendUnicastRequest(Message msg, Request<T> req, RequestOptions opts) throws Exception {
        Address dest=msg.getDest();
        Header hdr=new Header(Header.REQ, 0, this.corr_id);
        msg.putHeader(this.corr_id, hdr).setFlag(opts.flags(), false)
          .setFlag(opts.transientFlags(), true);

        if(req != null) { // sync RPC
            long req_id=REQUEST_ID.getAndIncrement();
            req.requestId(req_id);
            hdr.requestId(req_id); // set the request-id only for *synchronous RPCs*
            if(log.isTraceEnabled())
                log.trace("%s: invoking unicast RPC [req-id=%d] on %s", local_addr, req_id, dest);
            requests.putIfAbsent(req_id, req);
            // make sure no view is received before we add ourself as a view handler (https://issues.jboss.org/browse/JGRP-1428)
            req.viewChange(view);
            if(rpc_stats.extendedStats())
                req.start_time=System.nanoTime();
        }
        else // async RPC
            rpc_stats.add(RpcStats.Type.UNICAST, dest, false, 0);
        transport.down(msg);
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
     * Called by the protocol below when a message has been received. The algorithm should test whether the message
     * is destined for us and, if not, pass it up to the next layer. Otherwise, it should remove the header and check
     * whether the message is a request or response.
     * In the first case, the message will be delivered to the request handler registered
     * (calling its {@code handle()} method), in the second case, the corresponding response collector is looked up and
     * the message delivered.
     * @param evt The event to be received
     * @return Whether or not the event was consumed. If true, don't pass message up, else pass it up
     */
    public boolean receive(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE: // adjust number of responses to wait for
                receiveView(evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                setLocalAddress(evt.getArg());
                break;

            case Event.SITE_UNREACHABLE:
                SiteMaster site_master=evt.getArg();
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
        requests.values().forEach(Request::transportClosed);
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




    /** An entire site is down; mark all requests that point to that site as unreachable (used by RELAY2) */
    public void setSiteUnreachable(String site) {
        requests.values().stream().filter(Objects::nonNull).forEach(req -> req.siteUnreachable(site));
    }


    /**
     * View received: mark all responses from members that are not in new_view as suspected
     */
    public void receiveView(View new_view) {
        view=new_view; // move this before the iteration (JGRP-1428)
        requests.values().stream().filter(Objects::nonNull).forEach(req -> req.viewChange(new_view));
    }


    /**
     * Handles a message coming from a layer below
     * @return true if the message was consumed, don't pass it further up, else false
     */
    public boolean receiveMessage(Message msg) {
        Header hdr=msg.getHeader(this.corr_id);

        // Check if the message was sent by a request correlator with the same name;
        // there may be multiple request correlators in the same protocol stack
        if(hdr == null || hdr.corrId != this.corr_id) {
            log.trace("ID of request correlator header (%s) is different from ours (%d). Msg not accepted, passed up",
                      hdr != null? String.valueOf(hdr.corrId) : "null", this.corr_id);
            return false;
        }

        if(hdr instanceof MultiDestinationHeader) {
            // if we are part of the exclusion list, then we discard the request (addressed to different members)
            Address[] exclusion_list=((MultiDestinationHeader)hdr).exclusion_list;
            if(local_addr != null && Util.contains(local_addr, exclusion_list)) {
                log.trace("%s: dropped req from %s as we are in the exclusion list, hdr=%s", local_addr, msg.src(), hdr);
                return true; // don't pass this message further up
            }
        }
        dispatch(msg, hdr);
        return true; // message was consumed
    }

    public void receiveMessageBatch(MessageBatch batch) {
        for(Message msg : batch) {
            Header hdr=msg.getHeader(this.corr_id);
            if(hdr == null || hdr.corrId != this.corr_id) // msg was sent by a different request corr in the same stack
                continue;

            if(hdr instanceof MultiDestinationHeader) {
                // if we are part of the exclusion list, then we discard the request (addressed to different members)
                Address[] exclusion_list=((MultiDestinationHeader)hdr).exclusion_list;
                if(local_addr != null && Util.contains(local_addr, exclusion_list)) {
                    log.trace("%s: dropped req from %s as we are in the exclusion list, hdr=%s", local_addr, msg.src(), hdr);
                    batch.remove(msg);
                    continue; // don't pass this message further up
                }
            }
            dispatch(msg, hdr);
        }
    }



    // .......................................................................
    protected RequestCorrelator removeEntry(long id) {
        Request<?> req=requests.remove(id);
        if(req != null) {
            long time_ns=req.start_time > 0? System.nanoTime() - req.start_time : 0;
            if(req instanceof UnicastRequest)
                rpc_stats.add(RpcStats.Type.UNICAST, ((UnicastRequest<?>)req).target, true, time_ns);
            else if(req instanceof GroupRequest) {
                if(req.options != null && req.options.anycasting())
                    rpc_stats.addAnycast(true, time_ns, ((GroupRequest<?>)req).rsps.keySet());
                else
                    rpc_stats.add(RpcStats.Type.MULTICAST, null, true, time_ns);
            }
            else
                log.error("request type %s not known", req != null? req.getClass().getSimpleName() : req);
        }
        return this;
    }



    protected void dispatch(final Message msg, final Header hdr) {
        switch(hdr.type) {
            case Header.REQ:
                handleRequest(msg, hdr);
                break;

            case Header.RSP:
            case Header.EXC_RSP:
                Request<?> req=requests.get(hdr.req_id);
                if(req != null) {
                    Object retval=msg.getPayload();
                    req.receiveResponse(retval, msg.getSrc(), hdr.type == Header.EXC_RSP);
                }
                break;

            default:
                log.error(Util.getMessage("HeaderSTypeIsNeitherREQNorRSP"));
                break;
        }
    }

    /** Handle a request msg for this correlator */
    protected void handleRequest(Message req, Header hdr) {
        Object        retval;
        boolean       threw_exception=false;

        if(log.isTraceEnabled())
            log.trace("calling (%s) with request %d",
                      request_handler != null? request_handler.getClass().getName() : "null", hdr.req_id);
        if(async_dispatching && request_handler != null) {
            Response rsp=hdr.rspExpected()? new ResponseImpl(req, hdr.req_id) : null;
            try {
                request_handler.handle(req, rsp);
            }
            catch(Throwable t) {
                if(rsp != null)
                    rsp.send(wrap_exceptions ? new InvocationTargetException(t) : t, true);
                else
                    log.error("%s: failed dispatching request asynchronously: %s", local_addr, t);
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
        if(hdr.rspExpected())
            sendReply(req, hdr.req_id, retval, threw_exception);
    }


    protected void sendReply(final Message req, final long req_id, Object reply, boolean is_exception) {
        Message rsp=makeReply(req).setFlag(req.getFlags(false), false)
          .setPayload(reply)
          .clearFlag(Message.Flag.RSVP, Message.Flag.INTERNAL); // JGRP-1940
        sendResponse(rsp, req_id, is_exception);
    }

    protected static Message makeReply(Message msg) {
        Message reply=msg.create().get().setDest(msg.getSrc());
        if(msg.getDest() != null)
            reply.setSrc(msg.getDest());
        return reply;
    }

    protected void sendResponse(Message rsp, long req_id, boolean is_exception) {
        Header rsp_hdr=new Header(is_exception? Header.EXC_RSP : Header.RSP, req_id, corr_id);
        rsp.putHeader(corr_id, rsp_hdr);
        if(log.isTraceEnabled())
            log.trace("sending rsp for %d to %s", req_id, rsp.getDest());
        transport.down(rsp);
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

        /** The request id (unique for each blocking request), 0 means no response is expected */
        public long    req_id;

        /** The unique ID of the associated RequestCorrelator */
        public short   corrId;



        public Header() {}

        /**
         * @param type type of header (<tt>REQ</tt>/<tt>RSP</tt>)
         * @param req_id id of this header relative to ids of other requests originating from the same correlator
         * @param corr_id The ID of the <tt>RequestCorrelator</tt> from which
         */
        public Header(byte type, long req_id, short corr_id) {
            this.type=type;
            this.req_id=req_id;
            this.corrId=corr_id;
        }

        public Header  requestId(long req_id) {
            if(this.req_id > 0)
                throw new IllegalStateException(String.format("request-id (%d) is already set: trying to set it again (%d)", this.req_id, req_id));
            this.req_id=req_id;
            return this;
        }
        public short getMagicId() {return 67;}
        public Supplier<? extends org.jgroups.Header> create() {return Header::new;}

        public long    requestId()   {return req_id;}
        public boolean rspExpected() {return req_id > 0;}
        public short   corrId()      {return corrId;}

        public String toString() {
            StringBuilder ret=new StringBuilder();
            ret.append("corr_id=" + corrId + ", type=");
            switch(type) {
                case REQ: ret.append("REQ");
                    break;
                case RSP: ret.append("RSP");
                    break;
                case EXC_RSP: ret.append("EXC_RSP");
                    break;
                default: ret.append("<unknown>");
            }
            ret.append(", req_id=" + req_id).append(", rsp_expected=" + rspExpected());
            return ret.toString();
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
            Bits.writeLong(req_id, out);
            out.writeShort(corrId);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            req_id=Bits.readLong(in);
            corrId=in.readShort();
        }

        @Override
        public int serializedSize() {
            return Global.BYTE_SIZE  // type
              + Bits.size(req_id)    // req_id
              + Global.SHORT_SIZE;   // corrId
        }
    }



    public static final class MultiDestinationHeader extends Header {
        /** Contains a list of members who should not receive the request (others will drop). Ignored if null */
        public Address[] exclusion_list;

        public MultiDestinationHeader() {
        }

        public MultiDestinationHeader(byte type, long id, short corr_id, Address[] exclusion_list) {
            super(type, id, corr_id);
            this.exclusion_list=exclusion_list;
        }
        public short getMagicId() {return 68;}
        public Supplier<? extends org.jgroups.Header> create() {
            return MultiDestinationHeader::new;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Util.writeAddresses(exclusion_list, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            exclusion_list=Util.readAddresses(in);
        }

        @Override
        public int serializedSize() {
            return (int)(super.serializedSize() + Util.size(exclusion_list));
        }

        public String toString() {
            String str=super.toString();
            if(exclusion_list != null)
                str=str+ ", exclusion_list=" + Arrays.toString(exclusion_list);
            return str;
        }
    }



    protected class MyProbeHandler implements DiagnosticsHandler.ProbeHandler {

        public Map<String, String> handleProbe(String... keys) {
            if(requests == null)
                return null;
            Map<String,String> retval=new HashMap<>();
            for(String key: keys) {
                switch(key) {
                    case "requests":
                        StringBuilder sb=new StringBuilder();
                        for(Map.Entry<Long,Request<?>> entry: requests.entrySet())
                            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                        retval.put(key, sb.toString());
                        break;
                    case "reqtable-info":
                        retval.put(key, String.format("size=%d, next-id=%d", requests.size(), REQUEST_ID.get()));
                        break;
                    case "rpcs":
                        retval.put("sync  unicast   RPCs", String.valueOf(rpc_stats.unicasts(true)));
                        retval.put("sync  multicast RPCs", String.valueOf(rpc_stats.multicasts(true)));
                        retval.put("async unicast   RPCs", String.valueOf(rpc_stats.unicasts(false)));
                        retval.put("async multicast RPCs", String.valueOf(rpc_stats.multicasts(false)));
                        retval.put("sync  anycast   RPCs", String.valueOf(rpc_stats.anycasts(true)));
                        retval.put("async anycast   RPCs", String.valueOf(rpc_stats.anycasts(false)));
                        break;
                    case "rpcs-reset":
                        rpc_stats.reset();
                        break;
                    case "rpcs-enable-details":
                        rpc_stats.extendedStats(true);
                        break;
                    case "rpcs-disable-details":
                        rpc_stats.extendedStats(false);
                        break;
                    case "rpcs-details":
                        if(!rpc_stats.extendedStats())
                            retval.put(key, "<details not enabled: use rpcs-enable-details to enable>");
                        else
                            retval.put(key, rpc_stats.printOrderByDest());
                        break;
                }
            }
            return retval;
        }

        public String[] supportedKeys() {
            return new String[]{"requests", "reqtable-info", "rpcs", "rpcs-reset", "rpcs-enable-details", "rpcs-disable-details", "rpcs-details"};
        }
    }




}
