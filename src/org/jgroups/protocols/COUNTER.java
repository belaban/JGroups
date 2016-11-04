package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;


/**
 * Protocol which is used by {@link org.jgroups.blocks.atomic.CounterService} to provide a distributed atomic counter
 * @author Bela Ban
 * @since 3.0.0
 */
@MBean(description="Protocol to maintain distributed atomic counters")
public class COUNTER extends Protocol {

    @Property(description="Bypasses message bundling if true")
    protected boolean bypass_bundling=true;

    @Property(description="Request timeouts (in ms). If the timeout elapses, a Timeout (runtime) exception will be thrown")
    protected long timeout=60000;

    @Property(description="Number of milliseconds to wait for reconciliation responses from all current members")
    protected long reconciliation_timeout=10000;

    @Property(description="Number of backup coordinators. Modifications are asynchronously sent to all backup coordinators")
    protected int num_backups=1;

    protected Address local_addr;

    /** Set to true during reconciliation process, will cause all requests to be discarded */
    protected boolean discard_requests=false;

    protected View    view;

    /** The address of the cluster coordinator. Updated on view changes */
    protected Address coord;

    /** Backup coordinators. Only created if num_backups > 0 and coord=true */
    protected List<Address> backup_coords=null;

    protected Future<?> reconciliation_task_future;

    protected ReconciliationTask reconciliation_task;

    // server side counters
    protected final ConcurrentMap<String,VersionedValue> counters=Util.createConcurrentMap(20);

    // (client side) pending requests
    protected final Map<Owner,Tuple<Request,Promise>> pending_requests=Util.createConcurrentMap(20);

    protected static final byte REQUEST  = 1;
    protected static final byte RESPONSE = 2;
    

    protected enum RequestType {
        GET_OR_CREATE,
        DELETE,
        SET,
        COMPARE_AND_SET,
        ADD_AND_GET,
        UPDATE,
        RECONCILE,
        RESEND_PENDING_REQUESTS
    }

    protected enum ResponseType {
        VOID,
        GET_OR_CREATE,
        BOOLEAN,
        VALUE,
        EXCEPTION,
        RECONCILE
    }

    protected static RequestType requestToRequestType(Request req) {
        if(req instanceof GetOrCreateRequest)    return RequestType.GET_OR_CREATE;
        if(req instanceof DeleteRequest)         return RequestType.DELETE;
        if(req instanceof AddAndGetRequest)      return RequestType.ADD_AND_GET;
        if(req instanceof UpdateRequest)         return RequestType.UPDATE;
        if(req instanceof SetRequest)            return RequestType.SET;
        if(req instanceof CompareAndSetRequest)  return RequestType.COMPARE_AND_SET;
        if(req instanceof ReconcileRequest)      return RequestType.RECONCILE;
        if(req instanceof ResendPendingRequests) return RequestType.RESEND_PENDING_REQUESTS;
        throw new IllegalStateException("request " + req + " cannot be mapped to request type");
    }

    protected static ResponseType responseToResponseType(Response rsp) {
        if(rsp instanceof GetOrCreateResponse) return ResponseType.GET_OR_CREATE;
        if(rsp instanceof BooleanResponse) return ResponseType.BOOLEAN;
        if(rsp instanceof ValueResponse) return ResponseType.VALUE;
        if(rsp instanceof ExceptionResponse) return ResponseType.EXCEPTION;
        if(rsp instanceof ReconcileResponse) return ResponseType.RECONCILE;
        if(rsp != null) return ResponseType.VOID;
        throw new IllegalStateException("response " + rsp + " cannot be mapped to response type");
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

    @ManagedAttribute(description="List of the backup coordinator (null if num_backups <= 0")
    public String getBackupCoords() {
        return backup_coords != null? backup_coords.toString() : "null";
    }


    public Counter getOrCreateCounter(String name, long initial_value) {
        if(local_addr == null)
            throw new IllegalArgumentException("the channel needs to be connected before creating or getting a counter");
        Owner owner=getOwner();
        GetOrCreateRequest req=new GetOrCreateRequest(owner, name, initial_value);
        Promise<long[]> promise=new Promise<>();
        pending_requests.put(owner, new Tuple<>(req, promise));
        sendRequest(coord, req);
        long[] result=new long[0];
        try {
            result=promise.getResultWithTimeout(timeout);
            long value=result[0], version=result[1];
            if(!coord.equals(local_addr))
                counters.put(name, new VersionedValue(value, version));
            return new CounterImpl(name);
        }
        catch(TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /** Sent asynchronously - we don't wait for an ack */
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
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        CounterHeader hdr=msg.getHeader(id);
        if(hdr == null)
            return up_prot.up(msg);

        try {
            Object obj=streamableFromBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            if(log.isTraceEnabled())
                log.trace("[" + local_addr + "] <-- [" + msg.getSrc() + "] " + obj);

            if(obj instanceof Request) {
                handleRequest((Request)obj, msg.getSrc());
            }
            else if(obj instanceof Response) {
                handleResponse((Response)obj, msg.getSrc());
            }
            else {
                log.error(Util.getMessage("ReceivedObjectIsNeitherARequestNorAResponse") + obj);
            }
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedHandlingMessage"), ex);
        }
        return null;
    }

    
    protected void handleRequest(Request req, Address sender) {
        RequestType type=requestToRequestType(req);
        switch(type) {
            case GET_OR_CREATE:
                if(!local_addr.equals(coord) || discard_requests)
                    return;
                GetOrCreateRequest tmp=(GetOrCreateRequest)req;
                VersionedValue new_val=new VersionedValue(tmp.initial_value);
                VersionedValue val=counters.putIfAbsent(tmp.name, new_val);
                if(val == null)
                    val=new_val;
                Response rsp=new GetOrCreateResponse(tmp.owner, val.value, val.version);
                sendResponse(sender,rsp);
                if(backup_coords != null)
                    updateBackups(tmp.name, val.value, val.version);
                break;
            case DELETE:
                if(!local_addr.equals(coord) || discard_requests)
                    return;
                counters.remove(((SimpleRequest)req).name);
                break;
            case SET:
                if(!local_addr.equals(coord) || discard_requests)
                    return;
                val=counters.get(((SimpleRequest)req).name);
                if(val == null) {
                    sendCounterNotFoundExceptionResponse(sender, ((SimpleRequest)req).owner, ((SimpleRequest)req).name);
                    return;
                }
                long[] result=val.set(((SetRequest)req).value);
                rsp=new ValueResponse(((SimpleRequest)req).owner, result[0], result[1]);
                sendResponse(sender, rsp);
                if(backup_coords != null)
                    updateBackups(((SimpleRequest)req).name, result[0], result[1]);
                break;
            case COMPARE_AND_SET:
                if(!local_addr.equals(coord) || discard_requests)
                    return;
                val=counters.get(((SimpleRequest)req).name);
                if(val == null) {
                    sendCounterNotFoundExceptionResponse(sender, ((SimpleRequest)req).owner, ((SimpleRequest)req).name);
                    return;
                }
                result=val.compareAndSet(((CompareAndSetRequest)req).expected,((CompareAndSetRequest)req).update);
                rsp=new ValueResponse(((SimpleRequest)req).owner, result == null? -1 : result[0], result == null? -1 : result[1]);
                sendResponse(sender, rsp);
                if(backup_coords != null) {
                    VersionedValue value=counters.get(((SimpleRequest)req).name);
                    updateBackups(((SimpleRequest)req).name, value.value, value.version);
                }
                break;
            case ADD_AND_GET:
                if(!local_addr.equals(coord) || discard_requests)
                    return;
                val=counters.get(((SimpleRequest)req).name);
                if(val == null) {
                    sendCounterNotFoundExceptionResponse(sender, ((SimpleRequest)req).owner, ((SimpleRequest)req).name);
                    return;
                }
                result=val.addAndGet(((AddAndGetRequest)req).value);
                rsp=new ValueResponse(((SimpleRequest)req).owner, result[0], result[1]);
                sendResponse(sender, rsp);
                if(backup_coords != null)
                    updateBackups(((SimpleRequest)req).name, result[0], result[1]);
                break;
            case UPDATE:
                String counter_name=((UpdateRequest)req).name;
                long new_value=((UpdateRequest)req).value, new_version=((UpdateRequest)req).version;
                VersionedValue current=counters.get(counter_name);
                if(current == null)
                    counters.put(counter_name, new VersionedValue(new_value, new_version));
                else {
                    current.updateIfBigger(new_value, new_version);
                }
                break;
            case RECONCILE:
                if(sender.equals(local_addr)) // we don't need to reply to our own reconciliation request
                    break;

                // return all values except those with lower or same versions than the ones in the ReconcileRequest
                ReconcileRequest reconcile_req=(ReconcileRequest)req;
                Map<String,VersionedValue> map=new HashMap<>(counters);
                if(reconcile_req.names !=  null) {
                    for(int i=0; i < reconcile_req.names.length; i++) {
                        counter_name=reconcile_req.names[i];
                        long version=reconcile_req.versions[i];
                        VersionedValue my_value=map.get(counter_name);
                        if(my_value != null && my_value.version <= version)
                            map.remove(counter_name);
                    }
                }

                int len=map.size();
                String[] names=new String[len];
                long[] values=new long[len];
                long[] versions=new long[len];
                int index=0;
                for(Map.Entry<String,VersionedValue> entry: map.entrySet()) {
                    names[index]=entry.getKey();
                    values[index]=entry.getValue().value;
                    versions[index]=entry.getValue().version;
                    index++;
                }

                rsp=new ReconcileResponse(names, values, versions);
                sendResponse(sender, rsp);
                break;
            case RESEND_PENDING_REQUESTS:
                for(Tuple<Request,Promise> tuple: pending_requests.values()) {
                    Request request=tuple.getVal1();
                    if(log.isTraceEnabled())
                        log.trace("[" + local_addr + "] --> [" + coord + "] resending " + request);
                    sendRequest(coord, request);
                }
                break;

            default:
                break;
        }
    }


    protected VersionedValue getCounter(String name) {
        VersionedValue val=counters.get(name);
        if(val == null)
            throw new IllegalStateException("counter \"" + name + "\" not found");
        return val;
    }

    @SuppressWarnings("unchecked")
    protected void handleResponse(Response rsp, Address sender) {
        if(rsp instanceof ReconcileResponse) {
            if(log.isTraceEnabled() && ((ReconcileResponse)rsp).names != null && ((ReconcileResponse)rsp).names.length > 0)
                log.trace("[" + local_addr + "] <-- [" + sender + "] RECONCILE-RSP: " +
                            dump(((ReconcileResponse)rsp).names, ((ReconcileResponse)rsp).values, ((ReconcileResponse)rsp).versions));
            if(reconciliation_task != null)
                reconciliation_task.add((ReconcileResponse)rsp, sender);
            return;
        }

        Tuple<Request,Promise> tuple=pending_requests.remove(((SimpleResponse)rsp).owner);
        if(tuple == null) {
            log.warn("response for " + ((SimpleResponse)rsp).owner + " didn't have an entry");
            return;
        }
        Promise promise=tuple.getVal2();
        if(rsp instanceof ValueResponse) {
            ValueResponse tmp=(ValueResponse)rsp;
            if(tmp.result == -1 && tmp.version == -1)
                promise.setResult(null);
            else {
                long[] result={tmp.result,tmp.version};
                promise.setResult(result);
            }
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

    @ManagedOperation(description="Dumps all pending requests")
    public String dumpPendingRequests() {
        StringBuilder sb=new StringBuilder();
        for(Tuple<Request,Promise> tuple: pending_requests.values()) {
            Request tmp=tuple.getVal1();
            sb.append(tmp + " (" + tmp.getClass().getCanonicalName() + ") ");
        }
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

        if(Objects.equals(coord, local_addr)) {
            List<Address> old_backups=backup_coords != null? new ArrayList<>(backup_coords) : null;
            backup_coords=new CopyOnWriteArrayList<>(Util.pickNext(members, local_addr, num_backups));

            // send the current values to all *new* backups
            List<Address> new_backups=Util.newElements(old_backups,backup_coords);
            for(Address new_backup: new_backups) {
                for(Map.Entry<String,VersionedValue> entry: counters.entrySet()) {
                    UpdateRequest update=new UpdateRequest(entry.getKey(), entry.getValue().value, entry.getValue().version);
                    sendRequest(new_backup, update);
                }
            }
        }
        else
            backup_coords=null;

        if(old_coord != null && coord != null && !old_coord.equals(coord) && local_addr.equals(coord)) {
            discard_requests=true; // set to false when the task is done
            startReconciliationTask();
        }
    }


    protected Owner getOwner() {
        return new Owner(local_addr, Thread.currentThread().getId());
    }


    protected void sendRequest(Address dest, Request req) {
        try {
            Buffer buffer=requestToBuffer(req);
            Message msg=new Message(dest, buffer).putHeader(id, new CounterHeader());
            if(bypass_bundling)
                msg.setFlag(Message.Flag.DONT_BUNDLE);
            if(log.isTraceEnabled())
                log.trace("[" + local_addr + "] --> [" + (dest == null? "ALL" : dest) + "] " + req);

            down_prot.down(msg);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + req + " request: " + ex);
        }
    }


    protected void sendResponse(Address dest, Response rsp) {
        try {
            Buffer buffer=responseToBuffer(rsp);
            Message rsp_msg=new Message(dest, buffer).putHeader(id, new CounterHeader());
            if(bypass_bundling)
                rsp_msg.setFlag(Message.Flag.DONT_BUNDLE);

            if(log.isTraceEnabled())
                log.trace("[" + local_addr + "] --> [" + dest + "] " + rsp);

            down_prot.down(rsp_msg);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + rsp + " message to " + dest + ": " + ex);
        }
    }

    protected void updateBackups(String name, long value, long version) {
        Request req=new UpdateRequest(name, value, version);
        try {
            Buffer buffer=requestToBuffer(req);
            if(backup_coords != null && !backup_coords.isEmpty()) {
                for(Address backup_coord: backup_coords)
                    send(backup_coord, buffer);
            }
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + req + " to backup coordinator(s):" + ex);
        }
    }

    protected void send(Address dest, Buffer buffer) {
        try {
            Message rsp_msg=new Message(dest, buffer).putHeader(id, new CounterHeader());
            if(bypass_bundling)
                rsp_msg.setFlag(Message.Flag.DONT_BUNDLE);
            down_prot.down(rsp_msg);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSendingMessageTo") + dest + ": " + ex);
        }
    }

    protected void sendCounterNotFoundExceptionResponse(Address dest, Owner owner, String counter_name) {
        Response rsp=new ExceptionResponse(owner, "counter \"" + counter_name + "\" not found");
        sendResponse(dest, rsp);
    }


    protected static Buffer requestToBuffer(Request req) throws Exception {
        return streamableToBuffer(REQUEST,(byte)requestToRequestType(req).ordinal(), req);
    }

    protected static Buffer responseToBuffer(Response rsp) throws Exception {
        return streamableToBuffer(RESPONSE,(byte)responseToResponseType(rsp).ordinal(), rsp);
    }

    protected static Buffer streamableToBuffer(byte req_or_rsp, byte type, Streamable obj) throws Exception {
        int expected_size=obj instanceof SizeStreamable? ((SizeStreamable)obj).serializedSize() : 100;
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size);
        out.writeByte(req_or_rsp);
        out.writeByte(type);
        obj.writeTo(out);
        return new Buffer(out.buffer(), 0, out.position());
    }

    protected static Streamable streamableFromBuffer(byte[] buf, int offset, int length) throws Exception {
        switch(buf[offset]) {
            case REQUEST:
                return requestFromBuffer(buf, offset+1, length-1);
            case RESPONSE:
                return responseFromBuffer(buf, offset+1, length-1);
            default:
                throw new IllegalArgumentException("type " + buf[offset] + " is invalid (expected Request (1) or RESPONSE (2)");
        }
    }

    protected static final Request requestFromBuffer(byte[] buf, int offset, int length) throws Exception {
        ByteArrayInputStream input=new ByteArrayInputStream(buf, offset, length);
        DataInputStream in=new DataInputStream(input);
        RequestType type=RequestType.values()[in.readByte()];
        Request retval=createRequest(type);
        retval.readFrom(in);
        return retval;
    }

    protected static Request createRequest(RequestType type) {
        switch(type) {
            case COMPARE_AND_SET:         return new CompareAndSetRequest();
            case ADD_AND_GET:             return new AddAndGetRequest();
            case UPDATE:                  return new UpdateRequest();
            case GET_OR_CREATE:           return new GetOrCreateRequest();
            case DELETE:                  return new DeleteRequest();
            case SET:                     return new SetRequest();
            case RECONCILE:               return new ReconcileRequest();
            case RESEND_PENDING_REQUESTS: return new ResendPendingRequests();
            default:                      throw new IllegalArgumentException("failed creating a request from " + type);
        }
    }

    protected static final Response responseFromBuffer(byte[] buf, int offset, int length) throws Exception {
        ByteArrayInputStream input=new ByteArrayInputStream(buf, offset, length);
        DataInputStream in=new DataInputStream(input);
        ResponseType type=ResponseType.values()[in.readByte()];
        Response retval=createResponse(type);
        retval.readFrom(in);
        return retval;
    }

    protected static Response createResponse(ResponseType type) {
        switch(type) {
            case VOID:          return new SimpleResponse();
            case GET_OR_CREATE: return new GetOrCreateResponse();
            case BOOLEAN:       return new BooleanResponse();
            case VALUE:         return new ValueResponse();
            case EXCEPTION:     return new ExceptionResponse();
            case RECONCILE:     return new ReconcileResponse();
            default:            throw new IllegalArgumentException("failed creating a response from " + type);
        }
    }


    protected synchronized void startReconciliationTask() {
        if(reconciliation_task_future == null || reconciliation_task_future.isDone()) {
            reconciliation_task=new ReconciliationTask();
            reconciliation_task_future=getTransport().getTimer().schedule(reconciliation_task, 0, TimeUnit.MILLISECONDS);
        }
    }

    protected synchronized void stopReconciliationTask() {
        if(reconciliation_task_future != null) {
            reconciliation_task_future.cancel(true);
            if(reconciliation_task != null)
                reconciliation_task.cancel();
            reconciliation_task_future=null;
        }
    }


    protected static void writeReconciliation(DataOutput out, String[] names, long[] values, long[] versions) throws Exception {
        if(names == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(names.length);
        for(String name: names)
            Bits.writeString(name,out);
        for(long value: values)
            Bits.writeLong(value, out);
        for(long version: versions)
            Bits.writeLong(version, out);
    }

    protected static String[] readReconciliationNames(DataInput in, int len) throws Exception {
        String[] retval=new String[len];
        for(int i=0; i < len; i++)
            retval[i]=Bits.readString(in);
        return retval;
    }

    protected static long[] readReconciliationLongs(DataInput in, int len) throws Exception {
        long[] retval=new long[len];
        for(int i=0; i < len; i++)
            retval[i]=Bits.readLong(in);
        return retval;
    }

    protected static String dump(String[] names, long[] values, long[] versions) {
        StringBuilder sb=new StringBuilder();
        if(names != null) {
            for(int i=0; i < names.length; i++) {
                sb.append(names[i]).append(": ").append(values[i]).append(" (").append(versions[i]).append(")\n");
            }
        }
        return sb.toString();
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
                VersionedValue val=getCounter(name);
                val.set(new_value);
                if(backup_coords != null)
                    updateBackups(name, val.value, val.version);
                return;
            }
            Owner owner=getOwner();
            Request req=new SetRequest(owner, name, new_value);
            Promise<long[]> promise=new Promise<>();
            pending_requests.put(owner, new Tuple<>(req, promise));
            sendRequest(coord, req);
            Object obj=null;
            try {
                obj=promise.getResultWithTimeout(timeout);
                if(obj instanceof Throwable)
                    throw new IllegalStateException((Throwable)obj);
                long[] result=(long[])obj;
                long value=result[0], version=result[1];
                if(!coord.equals(local_addr))
                    counters.put(name, new VersionedValue(value, version));
            }
            catch(TimeoutException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean compareAndSet(long expect, long update) {
            if(local_addr.equals(coord)) {
                VersionedValue val=getCounter(name);
                boolean retval=val.compareAndSet(expect, update) != null;
                if(backup_coords != null)
                    updateBackups(name, val.value, val.version);
                return retval;
            }
            Owner owner=getOwner();
            Request req=new CompareAndSetRequest(owner, name, expect, update);
            Promise<long[]> promise=new Promise<>();
            pending_requests.put(owner, new Tuple<>(req, promise));
            sendRequest(coord, req);
            Object obj=null;
            try {
                obj=promise.getResultWithTimeout(timeout);
                if(obj instanceof Throwable)
                    throw new IllegalStateException((Throwable)obj);
                if(obj == null)
                    return false;
                long[] result=(long[])obj;
                long value=result[0], version=result[1];
                if(!coord.equals(local_addr))
                    counters.put(name, new VersionedValue(value, version));
                return true;
            }
            catch(TimeoutException e) {
                throw new RuntimeException(e);
            }
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
            if(local_addr.equals(coord)) {
                VersionedValue val=getCounter(name);
                long retval=val.addAndGet(delta)[0];
                if(backup_coords != null)
                    updateBackups(name, val.value, val.version);
                return retval;
            }
            Owner owner=getOwner();
            Request req=new AddAndGetRequest(owner, name, delta);
            Promise<long[]> promise=new Promise<>();
            pending_requests.put(owner, new Tuple<>(req, promise));
            sendRequest(coord, req);
            Object obj=null;
            try {
                obj=promise.getResultWithTimeout(timeout);
                if(obj instanceof Throwable)
                    throw new IllegalStateException((Throwable)obj);
                long[] result=(long[])obj;
                long value=result[0], version=result[1];
                if(!coord.equals(local_addr))
                    counters.put(name, new VersionedValue(value, version));
                return value;
            }
            catch(TimeoutException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String toString() {
            VersionedValue val=counters.get(name);
            return val != null? val.toString() : "n/a";
        }
    }




    protected interface Request extends Streamable {

    }


    protected static class SimpleRequest implements Request {
        protected Owner   owner;
        protected String  name;


        protected SimpleRequest() {
        }

        protected SimpleRequest(Owner owner, String name) {
            this.owner=owner;
            this.name=name;
        }

        public void writeTo(DataOutput out) throws Exception {
            owner.writeTo(out);
            Bits.writeString(name,out);
        }

        public void readFrom(DataInput in) throws Exception {
            owner=new Owner();
            owner.readFrom(in);
            name=Bits.readString(in);
        }

        public String toString() {
            return owner + " [" + name + "]";
        }
    }

    protected static class ResendPendingRequests implements Request {
        public void writeTo(DataOutput out) throws Exception {}
        public void readFrom(DataInput in) throws Exception {}
        public String toString() {return "ResendPendingRequests";}
    }

    protected static class GetOrCreateRequest extends SimpleRequest {
        protected long initial_value;

        protected GetOrCreateRequest() {}

        GetOrCreateRequest(Owner owner, String name, long initial_value) {
            super(owner,name);
            this.initial_value=initial_value;
        }

        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            initial_value=Bits.readLong(in);
        }

        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            Bits.writeLong(initial_value, out);
        }
    }


    protected static class DeleteRequest extends SimpleRequest {

        protected DeleteRequest() {}

        protected DeleteRequest(Owner owner, String name) {
            super(owner,name);
        }

        public String toString() {return "DeleteRequest: " + super.toString();}
    }


    protected static class AddAndGetRequest extends SetRequest {
        protected AddAndGetRequest() {}

        protected AddAndGetRequest(Owner owner, String name, long value) {
            super(owner,name,value);
        }

        public String toString() {return "AddAndGetRequest: " + super.toString();}
    }



    protected static class SetRequest extends SimpleRequest {
        protected long value;

        protected SetRequest() {}

        protected SetRequest(Owner owner, String name, long value) {
            super(owner, name);
            this.value=value;
        }

        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            value=Bits.readLong(in);
        }

        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            Bits.writeLong(value, out);
        }

        public String toString() {return super.toString() + ": " + value;}
    }


    protected static class CompareAndSetRequest extends SimpleRequest {
        protected long expected, update;

        protected CompareAndSetRequest() {}

        protected CompareAndSetRequest(Owner owner, String name, long expected, long update) {
            super(owner, name);
            this.expected=expected;
            this.update=update;
        }

        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            expected=Bits.readLong(in);
            update=Bits.readLong(in);
        }

        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            Bits.writeLong(expected, out);
            Bits.writeLong(update, out);
        }

        public String toString() {return super.toString() + ", expected=" + expected + ", update=" + update;}
    }


    protected static class ReconcileRequest implements Request {
        protected String[] names;
        protected long[]   values;
        protected long[]   versions;

        protected ReconcileRequest() {}

        protected ReconcileRequest(String[] names, long[] values, long[] versions) {
            this.names=names;
            this.values=values;
            this.versions=versions;
        }

        public void writeTo(DataOutput out) throws Exception {
            writeReconciliation(out, names, values, versions);
        }

        public void readFrom(DataInput in) throws Exception {
            int len=in.readInt();
            names=readReconciliationNames(in, len);
            values=readReconciliationLongs(in, len);
            versions=readReconciliationLongs(in,len);
        }

        public String toString() {return "ReconcileRequest (" + names.length + ") entries";}
    }


    protected static class UpdateRequest implements Request {
        protected String name;
        protected long   value;
        protected long   version;

        protected UpdateRequest() {}

        protected UpdateRequest(String name, long value, long version) {
            this.name=name;
            this.value=value;
            this.version=version;
        }

        public void writeTo(DataOutput out) throws Exception {
            Bits.writeString(name,out);
            Bits.writeLong(value, out);
            Bits.writeLong(version, out);
        }

        public void readFrom(DataInput in) throws Exception {
            name=Bits.readString(in);
            value=Bits.readLong(in);
            version=Bits.readLong(in);
        }

        public String toString() {return "UpdateRequest(" + name + ": "+ value + " (" + version + ")";}
    }



    protected interface Response extends Streamable {}

    
    /** Response without data */
    protected static class SimpleResponse implements Response {
        protected Owner owner;
        protected long  version;

        protected SimpleResponse() {}

        protected SimpleResponse(Owner owner, long version) {
            this.owner=owner;
            this.version=version;
        }

        public void readFrom(DataInput in) throws Exception {
            owner=new Owner();
            owner.readFrom(in);
            version=Bits.readLong(in);
        }

        public void writeTo(DataOutput out) throws Exception {
            owner.writeTo(out);
            Bits.writeLong(version, out);
        }

        public String toString() {return "Response";}
    }


    protected static class BooleanResponse extends SimpleResponse {
        protected boolean result;

        protected BooleanResponse() {}

        protected BooleanResponse(Owner owner, long version, boolean result) {
            super(owner, version);
            this.result=result;
        }

        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            result=in.readBoolean();
        }

        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            out.writeBoolean(result);
        }

        public String toString() {return "BooleanResponse(" + result + ")";}
    }

    protected static class ValueResponse extends SimpleResponse {
        protected long result;

        protected ValueResponse() {}

        protected ValueResponse(Owner owner, long result, long version) {
            super(owner, version);
            this.result=result;
        }

        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            result=Bits.readLong(in);
        }

        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            Bits.writeLong(result, out);
        }

        public String toString() {return "ValueResponse(" + result + ")";}
    }


    protected static class GetOrCreateResponse extends ValueResponse {

        protected GetOrCreateResponse() {}

        protected GetOrCreateResponse(Owner owner, long result, long version) {
            super(owner,result, version);
        }

        public String toString() {return "GetOrCreateResponse(" + result + ")";}
    }

    protected static class ExceptionResponse extends SimpleResponse {
        protected String error_message;

        protected ExceptionResponse() {}

        protected ExceptionResponse(Owner owner, String error_message) {
            super(owner, 0);
            this.error_message=error_message;
        }

        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            error_message=Bits.readString(in);
        }

        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            Bits.writeString(error_message,out);
        }

        public String toString() {return "ExceptionResponse: " + super.toString();}
    }


    
    protected static class ReconcileResponse implements Response {
        protected String[] names;
        protected long[]   values;
        protected long[]   versions;

        protected ReconcileResponse() {}

        protected ReconcileResponse(String[] names, long[] values, long[] versions) {
            this.names=names;
            this.values=values;
            this.versions=versions;
        }

        public void writeTo(DataOutput out) throws Exception {
            writeReconciliation(out,names,values,versions);
        }

        public void readFrom(DataInput in) throws Exception {
            int len=in.readInt();
            names=readReconciliationNames(in, len);
            values=readReconciliationLongs(in, len);
            versions=readReconciliationLongs(in,len);
        }

        public String toString() {
            int num=names != null? names.length : 0;
            return "ReconcileResponse (" + num + ") entries";
        }
    }
    


    public static class CounterHeader extends Header {
        public Supplier<? extends Header> create() {return CounterHeader::new;}
        public short getMagicId() {return 74;}
        public int serializedSize() {return 0;}
        public void writeTo(DataOutput out) throws Exception {}
        public void readFrom(DataInput in) throws Exception {}
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

        protected synchronized long[] compareAndSet(long expected, long update) {
            if(value == expected)
                return new long[]{value=update, ++version};
            return null;
        }

        /** Sets the value only if the version argument is greater than the own version */
        protected synchronized void updateIfBigger(long value, long version) {
            if(version > this.version) {
                this.version=version;
                this.value=value;
            }
        }

        public String toString() {return value + " (version=" + version + ")";}
    }


    protected class ReconciliationTask implements Runnable {
        protected ResponseCollector<ReconcileResponse> responses;


        public void run() {
            try {
                _run();
            }
            finally {
                discard_requests=false;
            }

            Request req=new ResendPendingRequests();
            sendRequest(null, req);
        }


        protected void _run() {
            Map<String,VersionedValue> copy=new HashMap<>(counters);
            int len=copy.size();
            String[] names=new String[len];
            long[] values=new long[len], versions=new long[len];
            int index=0;
            for(Map.Entry<String,VersionedValue> entry: copy.entrySet()) {
                names[index]=entry.getKey();
                values[index]=entry.getValue().value;
                versions[index]=entry.getValue().version;
                index++;
            }
            List<Address> targets=new ArrayList<>(view.getMembers());
            targets.remove(local_addr);
            responses=new ResponseCollector<>(targets); // send to everyone but us
            Request req=new ReconcileRequest(names, values, versions);
            sendRequest(null, req);

            responses.waitForAllResponses(reconciliation_timeout);
            Map<Address,ReconcileResponse> reconcile_results=responses.getResults();
            for(Map.Entry<Address,ReconcileResponse> entry: reconcile_results.entrySet()) {
                if(entry.getKey().equals(local_addr))
                    continue;
                ReconcileResponse rsp=entry.getValue();
                if(rsp != null && rsp.names != null) {
                    for(int i=0; i < rsp.names.length; i++) {
                        String counter_name=rsp.names[i];
                        long version=rsp.versions[i];
                        long value=rsp.values[i];
                        VersionedValue my_value=counters.get(counter_name);
                        if(my_value == null) {
                            counters.put(counter_name, new VersionedValue(value, version));
                            continue;
                        }
                            
                        if(my_value.version < version)
                            my_value.updateIfBigger(value, version);
                    }
                }
            }
        }


        public void add(ReconcileResponse rsp, Address sender) {
            if(responses != null)
                responses.add(sender, rsp);
        }

        protected void cancel() {
            if(responses != null)
                responses.reset();
        }

        public String toString() {
            return COUNTER.class.getSimpleName() + ": " + getClass().getSimpleName();
        }
    }

}
