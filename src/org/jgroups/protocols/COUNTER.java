package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;


/**
 * Protocol which is used by {@link org.jgroups.blocks.atomic.CounterService} to provide a distributed atomic counter
 * @author Bela Ban
 * @since 3.0.0
 */
@MBean(description="Protocol to maintain distributed atomic counters")
public class COUNTER extends Protocol {

    @Property(description="bypasses message bundling if set")
    protected boolean bypass_bundling=true;

    @Property(description="Request timeouts")
    protected long timeout=60000;


    protected Address local_addr;

    protected View    view;

    /** The address of the cluster coordinator. Updated on view changes */
    protected Address coord;

    // server side counters
    protected final ConcurrentMap<String,VersionedValue> counters=Util.createConcurrentMap(20);

    // client side counters
    protected final Map<Owner,Tuple<Request,Promise>> pending_requests=Util.createConcurrentMap(20);

    protected static final byte REQUEST  = 1;
    protected static final byte RESPONSE = 2;
    

    protected static enum RequestType {
        GET_OR_CREATE,
        DELETE,
        SET,
        COMPARE_AND_SET,
        ADD_AND_GET
    }

    protected static enum ResponseType {
        VOID,
        GET_OR_CREATE,
        BOOLEAN,
        VALUE,
        EXCEPTION
    }

    protected static RequestType requestToRequestType(Request req) {
        if(req instanceof GetOrCreateRequest)   return RequestType.GET_OR_CREATE;
        if(req instanceof DeleteRequest)        return RequestType.DELETE;
        if(req instanceof AddAndGetRequest)     return RequestType.ADD_AND_GET;
        if(req instanceof SetRequest)           return RequestType.SET;
        if(req instanceof CompareAndSetRequest) return RequestType.COMPARE_AND_SET;
        return null;
    }

    protected static ResponseType responseToResponseType(Response rsp) {
        if(rsp instanceof GetOrCreateResponse) return ResponseType.GET_OR_CREATE;
        if(rsp instanceof BooleanResponse) return ResponseType.BOOLEAN;
        if(rsp instanceof ValueResponse) return ResponseType.VALUE;
        if(rsp instanceof ExceptionResponse) return ResponseType.EXCEPTION;
        if(rsp != null) return ResponseType.VOID;
        return null;
    }


    public boolean getBypassBundling() {
        return bypass_bundling;
    }

    public void setBypassBundling(boolean bypass_bundling) {
        this.bypass_bundling=bypass_bundling;
    }


    @ManagedAttribute
    public String getAddress() {
        return local_addr != null? local_addr.toString() : null;
    }

    @ManagedAttribute
    public String getView() {
        return view != null? view.toString() : null;
    }


    public Counter getOrCreateCounter(String name, long initial_value) {
        Owner owner=getOwner();
        GetOrCreateRequest req=new GetOrCreateRequest(owner, name, initial_value);
        Promise<long[]> promise=new Promise<long[]>();
        pending_requests.put(owner, new Tuple<Request,Promise>(req, promise));
        sendRequest(coord, req);
        long[] result=promise.getResultWithTimeout(timeout);
        long value=result[0], version=result[1];
        if(!coord.equals(local_addr))
            counters.put(name, new VersionedValue(value, version));
        return new CounterImpl(name);
    }

    /** Sentr asynchronously - we don't wait for an ack */
    public void deleteCounter(String name) {
        Owner owner=getOwner();
        Request req=new DeleteRequest(owner, name);
        sendRequest(coord, req);
        if(!local_addr.equals(coord))
            counters.remove(name);
    }



    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                CounterHeader hdr=(CounterHeader)msg.getHeader(id);
                if(hdr == null)
                    break;

                try {
                    Object obj=streamableFromBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    if(log.isTraceEnabled())
                        log.trace("[" + local_addr + "] <-- [" + msg.getSrc() + "] " + obj);

                    if(obj instanceof Request) {
                        handleRequest((Request)obj, msg.getSrc());
                    }
                    else if(obj instanceof Response) {
                        handleResponse((Response)obj);
                    }
                    else {
                        log.error("received object is neither a Request nor a Response: " + obj);
                    }
                }
                catch(Exception ex) {
                    log.error("failed unmarshalling message", ex);
                }
                return null;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    
    protected void handleRequest(Request req, Address sender) {
        RequestType type=requestToRequestType(req);
        switch(type) {
            case GET_OR_CREATE:
                GetOrCreateRequest tmp=(GetOrCreateRequest)req;
                VersionedValue new_val=new VersionedValue(tmp.initial_value);
                VersionedValue val=counters.putIfAbsent(tmp.name, new_val);
                if(val == null)
                    val=new_val;
                Response rsp=new GetOrCreateResponse(tmp.owner, val.version, val.value);
                sendResponse(sender, rsp);
                break;
            case DELETE:
                counters.remove(req.name);
                break;
            case SET:
                val=counters.get(req.name);
                if(val == null) {
                    sendCounterNotFoundExceptionResponse(sender, req.owner, req.name);
                    return;
                }
                long[] result=val.set(((SetRequest)req).value);
                rsp=new ValueResponse(req.owner, result[0], result[1]);
                sendResponse(sender, rsp);
                break;
            case COMPARE_AND_SET:
                break;
            case ADD_AND_GET:
                val=counters.get(req.name);
                if(val == null) {
                    sendCounterNotFoundExceptionResponse(sender, req.owner, req.name);
                    return;
                }
                result=val.addAndGet(((AddAndGetRequest)req).value);
                rsp=new ValueResponse(req.owner, result[0], result[1]);
                sendResponse(sender, rsp);
                break;
            default:
                break;
        }
    }


    protected long[] add(String name, long delta) {
        return getCounter(name).addAndGet(delta);
    }

    protected void _set(String name, long value) {
        getCounter(name).set(value);
    }

    protected VersionedValue getCounter(String name) {
        VersionedValue val=counters.get(name);
        if(val == null)
            throw new IllegalStateException("counter \"" + name + "\" not found");
        return val;
    }

    protected void handleResponse(Response rsp) {
        Tuple<Request,Promise> tuple=pending_requests.get(rsp.owner);
        if(tuple == null) {
            log.warn("response for " + rsp.owner + " didn't have an entry");
            return;
        }
        Promise promise=tuple.getVal2();
        if(rsp instanceof ValueResponse) {
            ValueResponse tmp=(ValueResponse)rsp;
            long[] result={tmp.result,tmp.version};
            promise.setResult(result);
        }
        else if(rsp instanceof BooleanResponse)
            promise.setResult(((BooleanResponse)rsp).result);
        else if(rsp instanceof ExceptionResponse) {
            promise.setResult(new Throwable(((ExceptionResponse)rsp).error_message));
        }
        else
            promise.setResult(null);
    }


    



    @ManagedOperation(description="Dumps all counters")
    public String printCounters() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,VersionedValue> entry: counters.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        return sb.toString();
    }

    protected void handleView(View view) {
        this.view=view;
        if(log.isDebugEnabled())
            log.debug("view=" + view);
        List<Address> members=view.getMembers();
        Address old_coord=coord;
        if(!members.isEmpty())
            coord=members.get(0);

        if(old_coord != null && coord != null && !old_coord.equals(coord)) {

            // todo: handle case when the coordinator changed
            
        }


        // todo: if the coordinator failed, we need to get counter information from a backup-coord, or by
        // contacting all members and get the last values (locally cached)
    }


    protected Owner getOwner() {
        return new Owner(local_addr, Thread.currentThread().getId());
    }


    protected void sendRequest(Address dest, Request req) {
        try {
            Buffer buffer=requestToBuffer(req);
            Message msg=new Message(dest, null, buffer);
            msg.putHeader(id, new CounterHeader());
            if(bypass_bundling)
                msg.setFlag(Message.DONT_BUNDLE);
            if(log.isTraceEnabled())
                log.trace("[" + local_addr + "] --> [" + (dest == null? "ALL" : dest) + "] " + req);

            down_prot.down(new Event(Event.MSG, msg));
        }
        catch(Exception ex) {
            log.error("failed sending " + req + " request: " + ex);
        }
    }


    protected void sendResponse(Address dest, Response rsp) {
        try {
            Buffer buffer=responseToBuffer(rsp);
            Message rsp_msg=new Message(dest, null, buffer);
            rsp_msg.putHeader(id, new CounterHeader());
            if(bypass_bundling)
                rsp_msg.setFlag(Message.DONT_BUNDLE);

            if(log.isTraceEnabled())
                log.trace("[" + local_addr + "] --> [" + dest + "] " + rsp);

            down_prot.down(new Event(Event.MSG, rsp_msg));
        }
        catch(Exception ex) {
            log.error("failed sending " + rsp + " message to " + dest + ": " + ex);
        }
    }

    protected void sendCounterNotFoundExceptionResponse(Address dest, Owner owner, String counter_name) {
        Response rsp=new ExceptionResponse(owner, "counter \"" + counter_name + "\" not found");
        sendResponse(dest, rsp);
    }


    protected static Buffer requestToBuffer(Request req) throws IOException {
        return streamableToBuffer(REQUEST,(byte)requestToRequestType(req).ordinal(), req);
    }

    protected static Buffer responseToBuffer(Response rsp) throws IOException {
        return streamableToBuffer(RESPONSE,(byte)responseToResponseType(rsp).ordinal(), rsp);
    }

    protected static Buffer streamableToBuffer(byte req_or_rsp, byte type, Streamable obj) throws IOException {
        ExposedByteArrayOutputStream output=new ExposedByteArrayOutputStream(100);
        DataOutputStream out=new DataOutputStream(output);
        out.writeByte(req_or_rsp);
        out.writeByte(type);
        obj.writeTo(out);
        out.flush();
        return output.getBuffer();
    }

    protected static Streamable streamableFromBuffer(byte[] buf, int offset, int length) throws IOException, IllegalAccessException, InstantiationException {
        switch(buf[offset]) {
            case REQUEST:
                return requestFromBuffer(buf, offset+1, length-1);
            case RESPONSE:
                return responseFromBuffer(buf, offset+1, length-1);
            default:
                throw new IllegalArgumentException("type " + buf[offset] + " is invalid (expected Request (1) or RESPONSE (2)");
        }
    }

    protected static final Request requestFromBuffer(byte[] buf, int offset, int length) throws IOException, InstantiationException, IllegalAccessException {
        ByteArrayInputStream input=new ByteArrayInputStream(buf, offset, length);
        DataInputStream in=new DataInputStream(input);
        RequestType type=RequestType.values()[in.readByte()];
        Request retval=createRequest(type);
        retval.readFrom(in);
        return retval;
    }

    protected static Request createRequest(RequestType type) {
        switch(type) {
            case COMPARE_AND_SET: return new CompareAndSetRequest();
            case ADD_AND_GET:     return new AddAndGetRequest();
            case GET_OR_CREATE:   return new GetOrCreateRequest();
            case DELETE:          return new DeleteRequest();
            case SET:             return new SetRequest();
            default:              return null;
        }
    }

    protected static final Response responseFromBuffer(byte[] buf, int offset, int length) throws IOException, InstantiationException, IllegalAccessException {
        ByteArrayInputStream input=new ByteArrayInputStream(buf, offset, length);
        DataInputStream in=new DataInputStream(input);
        ResponseType type=ResponseType.values()[in.readByte()];
        Response retval=createResponse(type);
        retval.readFrom(in);
        return retval;
    }

    protected static Response createResponse(ResponseType type) {
        switch(type) {
            case VOID:          return new Response();
            case GET_OR_CREATE: return new GetOrCreateResponse();
            case BOOLEAN:       return new BooleanResponse();
            case VALUE:         return new ValueResponse();
            case EXCEPTION:     return new ExceptionResponse();
            default: return null;
        }
    }


    protected class CounterImpl implements Counter {
        protected final String  name;

        protected CounterImpl(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public long get() {
            return addAndGet(0);
        }

        @Override
        public void set(long new_value) {
            if(local_addr.equals(coord)) {
                _set(name,new_value);
                return;
            }
            Owner owner=getOwner();
            Request req=new SetRequest(owner, name, new_value);
            Promise<long[]> promise=new Promise<long[]>();
            pending_requests.put(owner, new Tuple<Request,Promise>(req, promise));
            sendRequest(coord, req);
            Object obj=promise.getResultWithTimeout(timeout);
            if(obj instanceof Throwable)
                throw new IllegalStateException((Throwable)obj);
            long[] result=(long[])obj;
            long value=result[0], version=result[1];
            if(!coord.equals(local_addr))
                counters.put(name, new VersionedValue(value, version));
        }

        @Override
        public boolean compareAndSet(long expect, long update) {
            return false;
        }

        @Override
        public long incrementAndGet() {
            return addAndGet(1);
        }

        @Override
        public long decrementAndGet() {
            return addAndGet(-1);
        }

        @Override
        public long addAndGet(long delta) {
            if(local_addr.equals(coord))
                return add(name,delta)[0];
            Owner owner=getOwner();
            Request req=new AddAndGetRequest(owner, name, delta);
            Promise<long[]> promise=new Promise<long[]>();
            pending_requests.put(owner, new Tuple<Request,Promise>(req, promise));
            sendRequest(coord, req);
            Object obj=promise.getResultWithTimeout(timeout);
            if(obj instanceof Throwable)
                throw new IllegalStateException((Throwable)obj);
            long[] result=(long[])obj;
            long value=result[0], version=result[1];
            if(!coord.equals(local_addr))
                counters.put(name, new VersionedValue(value, version));
            return value;
        }

        @Override
        public String toString() {
            VersionedValue val=counters.get(name);
            return val != null? val.toString() : "n/a";
        }
    }







    protected abstract static class Request implements Streamable {
        protected Owner   owner;
        protected String  name;


        protected Request() {
        }

        protected Request(Owner owner, String name) {
            this.owner=owner;
            this.name=name;
        }

        public void writeTo(DataOutput out) throws IOException {
            owner.writeTo(out);
            Util.writeString(name, out);
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            owner=new Owner();
            owner.readFrom(in);
            name=Util.readString(in);
        }

        public String toString() {
            return owner + " [" + name + "]";
        }
    }

    protected static class GetOrCreateRequest extends Request {
        protected long initial_value;

        protected GetOrCreateRequest() {
        }

        GetOrCreateRequest(Owner owner, String name, long initial_value) {
            super(owner,name);
            this.initial_value=initial_value;
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            super.readFrom(in);
            initial_value=Util.readLong(in);
        }

        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Util.writeLong(initial_value, out);
        }
    }


    protected static class DeleteRequest extends Request {

        protected DeleteRequest() {
        }

        protected DeleteRequest(Owner owner, String name) {
            super(owner,name);
        }

        public String toString() {
            return "DeleteRequest: " + super.toString();
        }
    }


    protected static class AddAndGetRequest extends SetRequest {
        protected AddAndGetRequest() {
        }

        protected AddAndGetRequest(Owner owner, String name, long value) {
            super(owner,name,value);
        }

        public String toString() {
            return "AddAndGetRequest: " + super.toString();
        }
    }



    protected static class SetRequest extends Request {
        protected long value;

        protected SetRequest() {
        }

        protected SetRequest(Owner owner, String name, long value) {
            super(owner, name);
            this.value=value;
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            super.readFrom(in);
            value=Util.readLong(in);
        }

        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Util.writeLong(value, out);
        }

        public String toString() {
            return super.toString() + ": " + value;
        }
    }


    protected static class CompareAndSetRequest extends Request {
        protected long expected, update;

        protected CompareAndSetRequest() {
        }

        protected CompareAndSetRequest(Owner owner, String name, long expected, long update) {
            super(owner, name);
            this.expected=expected;
            this.update=update;
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            super.readFrom(in);
            expected=Util.readLong(in);
            update=Util.readLong(in);
        }

        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Util.writeLong(expected, out);
            Util.writeLong(update, out);
        }

        public String toString() {
            return super.toString() + ", expected=" + expected + ", update=" + update;
        }
    }


    
    /** Response without data */
    protected static class Response implements Streamable {
        protected Owner owner;
        protected long  version;

        protected Response() {
        }

        protected Response(Owner owner, long version) {
            this.owner=owner;
            this.version=version;
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            owner=new Owner();
            owner.readFrom(in);
            version=Util.readLong(in);
        }

        public void writeTo(DataOutput out) throws IOException {
            owner.writeTo(out);
            Util.writeLong(version, out);
        }

        public String toString() {
            return "Response";
        }
    }


    protected static class BooleanResponse extends Response {
        protected boolean result;

        protected BooleanResponse() {
        }

        protected BooleanResponse(Owner owner, long version, boolean result) {
            super(owner, version);
            this.result=result;
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            super.readFrom(in);
            result=in.readBoolean();
        }

        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(result);
        }

        public String toString() {
            return "BooleanResponse(" + result + ")";
        }
    }

    protected static class ValueResponse extends Response {
        protected long result;

        protected ValueResponse() {
        }

        protected ValueResponse(Owner owner, long result, long version) {
            super(owner, version);
            this.result=result;
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            super.readFrom(in);
            result=Util.readLong(in);
        }

        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Util.writeLong(result, out);
        }

        public String toString() {
            return "ValueResponse(" + result + ")";
        }
    }


    protected static class GetOrCreateResponse extends ValueResponse {

        protected GetOrCreateResponse() {
        }

        protected GetOrCreateResponse(Owner owner, long result, long version) {
            super(owner,result, version);
        }

        public String toString() {
            return "GetOrCreateResponse(" + result + ")";
        }
    }

    protected static class ExceptionResponse extends Response {
        protected String error_message;

        protected ExceptionResponse() {
        }

        protected ExceptionResponse(Owner owner, String error_message) {
            super(owner, 0);
            this.error_message=error_message;
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            super.readFrom(in);
            error_message=Util.readString(in);
        }

        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Util.writeString(error_message, out);
        }

        public String toString() {
            return "ExceptionResponse: " + super.toString();
        }
    }
    


    public static class CounterHeader extends Header {
        public int size() {return 0;}
        public void writeTo(DataOutput out) throws IOException {}
        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {}
    }
    

    protected static class VersionedValue {
        protected long value;
        protected long version=1;

        protected VersionedValue(long value) {
            this.value=value;
        }

        protected VersionedValue(long value, long version) {
            this.value=value;
            this.version=version;
        }

        /** num == 0 --> GET */
        protected synchronized long[] addAndGet(long num) {
            return num == 0? new long[]{value, version} : new long[]{value+=num, ++version};
        }

        protected synchronized long[] set(long value) {
            return new long[]{this.value=value,++version};
        }

        protected synchronized boolean compareAndSet(long expected, long update) {
            if(value == expected) {
                value=update;
                ++version;
                return true;
            }
            return false;
        }

        public String toString() {
            return value + " (version=" + version + ")";
        }
    }

}
