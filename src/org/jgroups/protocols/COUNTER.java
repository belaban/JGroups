package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * Protocol which is used by {@link org.jgroups.blocks.atomic.CounterService} to provide a distributed atomic counter
 * @author Bela Ban
 * @since 3.0.0
 */
@MBean(description="Protocol to maintain distributed atomic counters")
public class COUNTER extends Protocol {

    private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong();
    //enum value() method is expensive since it creates a copy of the internal array every time it is invoked.
    //we can cache it since we don't change it.
    private static final RequestType[] REQUEST_TYPES_CACHED = RequestType.values();
    private static final ResponseType[] RESPONSE_TYPES_CACHED = ResponseType.values();

    @Property(description="Bypasses message bundling if true")
    protected boolean bypass_bundling=true;

    @Property(description="Request timeouts (in ms). If the timeout elapses, a TimeoutException will be thrown",
      type=AttributeType.TIME)
    protected long timeout=60000;

    @Property(description="Number of milliseconds to wait for reconciliation responses from all current members",
      type=AttributeType.TIME)
    protected long reconciliation_timeout=10000;

    @Property(description="Number of backup coordinators. Modifications are asynchronously sent to all backup coordinators")
    protected int num_backups=1;

    /** Set to true during reconciliation process, will cause all requests to be discarded */
    protected boolean discard_requests=false;

    protected View    view;

    /** The address of the cluster coordinator. Updated on view changes */
    protected Address coord;

    /** Backup coordinators. Only created if num_backups > 0 and coord=true */
    protected List<Address> backup_coords=null;

    @GuardedBy("this")
    protected Future<?> reconciliation_task_future;

    protected ReconciliationTask reconciliation_task;

    // server side counters
    protected final Map<String, VersionedValue> counters = Util.createConcurrentMap(20);

    // (client side) pending requests
    protected final Map<Owner, RequestCompletableFuture> pending_requests = Util.createConcurrentMap(20);

    protected static final byte REQUEST  = 1;
    protected static final byte RESPONSE = 2;

    private TP transport;

    protected enum RequestType {
        GET_OR_CREATE {
            @Override
            Request create() {
                return new GetOrCreateRequest();
            }
        },
        DELETE {
            @Override
            Request create() {
                return new DeleteRequest();
            }
        },
        SET {
            @Override
            Request create() {
                return new SetRequest();
            }
        },
        COMPARE_AND_SET {
            @Override
            Request create() {
                return new CompareAndSetRequest();
            }
        },
        ADD_AND_GET {
            @Override
            Request create() {
                return new AddAndGetRequest();
            }
        },
        UPDATE {
            @Override
            Request create() {
                return new UpdateRequest();
            }
        },
        RECONCILE {
            @Override
            Request create() {
                return new ReconcileRequest();
            }
        },
        RESEND_PENDING_REQUESTS {
            @Override
            Request create() {
                return new ResendPendingRequests();
            }
        };

        abstract Request create();
    }

    protected enum ResponseType {
        VALUE {
            @Override
            Response create() {
                return new ValueResponse();
            }
        },
        EXCEPTION{
            @Override
            Response create() {
                return new ExceptionResponse();
            }
        },
        RECONCILE {
            @Override
            Response create() {
                return new ReconcileResponse();
            }
        };

        abstract Response create();
    }

    public boolean getBypassBundling() {
        return bypass_bundling;
    }

    public COUNTER setBypassBundling(boolean bypass_bundling) {
        this.bypass_bundling=bypass_bundling;
        return this;
    }

    @ManagedAttribute
    public String getView() {
        return view != null? view.toString() : null;
    }

    @ManagedAttribute(description="List of the backup coordinator (null if num_backups <= 0")
    public String getBackupCoords() {
        return backup_coords != null? backup_coords.toString() : "null";
    }

    @Override
    public void init() throws Exception {
        super.init();
        transport = getTransport();
    }

    @Deprecated
    public Counter getOrCreateCounter(String name, long initial_value) {
        CounterImpl counter = CompletableFutures.join(doGetOrCreateCounter(name, initial_value));
        return counter.sync;
    }

    public CompletionStage<AsyncCounter> getOrCreateAsyncCounter(String name, long initial_value) {
        return doGetOrCreateCounter(name, initial_value).thenApply(Function.identity());
    }

    private CompletionStage<CounterImpl> doGetOrCreateCounter(String name, long initial_value) {
        Objects.requireNonNull(name);
        if(local_addr == null)
            throw new IllegalStateException("the channel needs to be connected before creating or getting a counter");
        // is it safe?
        if (counters.containsKey(name)) {
            // if the counter exists, we do not need to send a request to the coordinator, right?
            return CompletableFuture.completedFuture(new CounterImpl(name));
        }
        Owner owner=getOwner();
        GetOrCreateRequest req=new GetOrCreateRequest(owner, name, initial_value);
        CompletableFuture<Long> rsp = sendRequestToCoordinator(owner, req);
        return rsp.thenApply(aLong -> new CounterImpl(name));
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
            case Event.VIEW_CHANGE:
                handleView(evt.arg());
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        if (evt.getType() == Event.VIEW_CHANGE) {
            handleView(evt.getArg());
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        CounterHeader hdr = msg.getHeader(id);
        if (hdr == null)
            return up_prot.up(msg);

        try {
            assert msg.hasArray();
            DataInput in = new DataInputStream(new ByteArrayInputStream(msg.getArray(), msg.getOffset(), msg.getLength()));
            switch (in.readByte()) {
                case REQUEST:
                    Request req = requestFromDataInput(in);
                    if (log.isTraceEnabled())
                        log.trace("[" + local_addr + "] <-- [" + msg.getSrc() + "] " + req);
                    req.execute(this, msg.getSrc());
                    break;
                case RESPONSE:
                    Response rsp = responseFromDataInput(in);
                    if (log.isTraceEnabled())
                        log.trace("[" + local_addr + "] <-- [" + msg.getSrc() + "] " + rsp);
                    handleResponse(rsp, msg.getSrc());
                    break;
                default:
                    log.error(Util.getMessage("ReceivedObjectIsNeitherARequestNorAResponse"));
                    break;
            }
        } catch (Exception ex) {
            log.error(Util.getMessage("FailedHandlingMessage"), ex);
        }
        return null;
    }

    protected VersionedValue getCounter(String name) {
        VersionedValue val=counters.get(name);
        if(val == null)
            throw new IllegalStateException("counter \"" + name + "\" not found");
        return val;
    }

    protected void handleResponse(Response rsp, Address sender) {
        if(rsp instanceof ReconcileResponse) {
            handleReconcileResponse((ReconcileResponse) rsp, sender);
            return;
        }

        RequestCompletableFuture cf=pending_requests.remove(rsp.getOwner());
        if(cf == null) {
            log.warn("response for " + rsp.getOwner() + " didn't have an entry");
            return;
        }
        rsp.complete(cf);
    }

    private void handleReconcileResponse(ReconcileResponse rsp, Address sender) {
        if(log.isTraceEnabled() && rsp.names != null && rsp.names.length > 0)
            log.trace("[" + local_addr + "] <-- [" + sender + "] RECONCILE-RSP: " + dump(rsp.names, rsp.values, rsp.versions));
        if(reconciliation_task != null)
            reconciliation_task.add(rsp, sender);
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
        for(RequestCompletableFuture cf: pending_requests.values()) {
            Request tmp=cf.getRequest();
            sb.append(tmp).append('(').append(tmp.getClass().getCanonicalName()).append(") ");
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
        return new Owner(local_addr, REQUEST_ID_GENERATOR.incrementAndGet());
    }

    protected void updateBackups(String name, long[] versionedValue) {
        if (backup_coords == null || backup_coords.isEmpty()) {
            return;
        }
        Request req=new UpdateRequest(name, versionedValue[0], versionedValue[1]);
        try {
            ByteArray buffer=requestToBuffer(req);
            for(Address dst: backup_coords) {
                logSending(dst, req);
                send(dst, buffer);
            }
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + req + " to backup coordinator(s):" + ex);
        }
    }

    protected void sendRequest(Address dest, Request req) {
        try {
            ByteArray buffer=requestToBuffer(req);
            logSending(dest, req);
            send(dest, buffer);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + req + " request: " + ex);
        }
    }


    protected void sendResponse(Address dest, Response rsp) {
        try {
            ByteArray buffer=responseToBuffer(rsp);
            logSending(dest, rsp);
            send(dest, buffer);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + rsp + " message to " + dest + ": " + ex);
        }
    }

    protected void send(Address dest, ByteArray buffer) {
        try {
            Message rsp_msg=new BytesMessage(dest, buffer).putHeader(id, new CounterHeader());
            if(bypass_bundling)
                rsp_msg.setFlag(Message.Flag.DONT_BUNDLE);
            down_prot.down(rsp_msg);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSendingMessageTo") + dest + ": " + ex);
        }
    }

    private void logSending(Address dst, Object data) {
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + (dst == null? "ALL" : dst) + "]: " + data);
    }

    protected void sendCounterNotFoundExceptionResponse(Address dest, Owner owner, String counter_name) {
        Response rsp=new ExceptionResponse(owner, "counter \"" + counter_name + "\" not found");
        sendResponse(dest, rsp);
    }

    private long updateCounter(ResponseData responseData) {
        if(!coord.equals(local_addr)) {
            counters.compute(responseData.counterName, responseData);
        }
        return responseData.value;
    }

    protected static ByteArray requestToBuffer(Request req) throws Exception {
        return streamableToBuffer(REQUEST,(byte)req.getRequestType().ordinal(), req);
    }

    protected static ByteArray responseToBuffer(Response rsp) throws Exception {
        return streamableToBuffer(RESPONSE,(byte)rsp.getResponseType().ordinal(), rsp);
    }

    protected static ByteArray streamableToBuffer(byte req_or_rsp, byte type, Streamable obj) throws Exception {
        int expected_size=obj instanceof SizeStreamable? ((SizeStreamable)obj).serializedSize() : 100;
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size);
        out.writeByte(req_or_rsp);
        out.writeByte(type);
        obj.writeTo(out);
        return new ByteArray(out.buffer(), 0, out.position());
    }

    protected static Request requestFromDataInput(DataInput in) throws Exception {
        Request retval=REQUEST_TYPES_CACHED[in.readByte()].create();
        retval.readFrom(in);
        return retval;
    }

    protected static Response responseFromDataInput(DataInput in) throws Exception {
        Response retval=RESPONSE_TYPES_CACHED[in.readByte()].create();
        retval.readFrom(in);
        return retval;
    }

    protected synchronized void startReconciliationTask() {
        if(reconciliation_task_future == null || reconciliation_task_future.isDone()) {
            reconciliation_task=new ReconciliationTask();
            reconciliation_task_future=transport.getTimer().schedule(reconciliation_task, 0, TimeUnit.MILLISECONDS);
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


    protected static void writeReconciliation(DataOutput out, String[] names, long[] values, long[] versions) throws IOException {
        if(names == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(names.length);
        for(String name: names)
            Bits.writeString(name,out);
        for(long value: values)
            Bits.writeLongCompressed(value, out);
        for(long version: versions)
            Bits.writeLongCompressed(version, out);
    }

    protected static String[] readReconciliationNames(DataInput in, int len) throws IOException {
        String[] retval=new String[len];
        for(int i=0; i < len; i++)
            retval[i]=Bits.readString(in);
        return retval;
    }

    protected static long[] readReconciliationLongs(DataInput in, int len) throws IOException {
        long[] retval=new long[len];
        for(int i=0; i < len; i++)
            retval[i]=Bits.readLongCompressed(in);
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


    protected class CounterImpl implements AsyncCounter {
        protected final String  name;
        final SyncCounterImpl sync;

        protected CounterImpl(String name) {
            this.name = name;
            this.sync = new SyncCounterImpl(this);
        }

        public String getName() {
            return name;
        }

        @Override
        public CompletableFuture<Void> set(long new_value) {
            if(local_addr.equals(coord)) {
                VersionedValue val=getCounter(name);
                long[] result = val.set(new_value);
                updateBackups(name, result);
                return CompletableFutures.completedNull();
            }
            Owner owner=getOwner();
            Request req=new SetRequest(owner, name, new_value);
            return sendRequestToCoordinator(owner, req).thenAccept(CompletableFutures.voidConsumer());
        }

        @Override
        public CompletableFuture<Long> compareAndSwap(long expect, long update) {
            if(local_addr.equals(coord)) {
                VersionedValue val=getCounter(name);
                long retval=val.compareAndSwap(expect, update)[0];
                updateBackups(name, val.snapshot());
                return CompletableFuture.completedFuture(retval);
            }
            Owner owner=getOwner();
            Request req=new CompareAndSetRequest(owner, name, expect, update);
            return sendRequestToCoordinator(owner, req);
        }

        @Override
        public CompletableFuture<Long> addAndGet(long delta) {
            if(local_addr.equals(coord)) {
                VersionedValue val=getCounter(name);
                long[] result=val.addAndGet(delta);
                updateBackups(name, result);
                return CompletableFuture.completedFuture(result[0]);
            }
            Owner owner=getOwner();
            Request req=new AddAndGetRequest(owner, name, delta);
            return sendRequestToCoordinator(owner, req);
        }

        @Override
        public SyncCounter sync() {
            return sync;
        }

        @Override
        public String toString() {
            VersionedValue val=counters.get(name);
            return val != null? val.toString() : "n/a";
        }
    }

    private static class SyncCounterImpl implements Counter {

        private final AsyncCounter counter;

        private SyncCounterImpl(AsyncCounter counter) {
            this.counter = counter;
        }

        @Override
        public String getName() {
            return counter.getName();
        }

        @Override
        public long get() {
            return CompletableFutures.join(counter.get());
        }

        @Override
        public void set(long new_value) {
            CompletableFutures.join(counter.set(new_value));
        }

        @Override
        public long compareAndSwap(long expect, long update) {
            return CompletableFutures.join(counter.compareAndSwap(expect, update));
        }

        @Override
        public long addAndGet(long delta) {
            return CompletableFutures.join(counter.addAndGet(delta));
        }

        @Override
        public AsyncCounter async() {
            return counter;
        }

        public String toString() {
            return counter != null? counter.toString() : null;
        }
    }

    private CompletableFuture<Long> sendRequestToCoordinator(Owner owner, Request request) {
        RequestCompletableFuture cf = new RequestCompletableFuture(request);
        pending_requests.put(owner, cf);
        sendRequest(coord, request);
        return cf.orTimeout(timeout, TimeUnit.MILLISECONDS).thenApply(this::updateCounter);
    }

    private boolean skipRequest() {
        return !local_addr.equals(coord) || discard_requests;
    }

    protected interface Request extends Streamable {

        String getCounterName();

        RequestType getRequestType();

        void execute(COUNTER protocol, Address sender);
    }


    protected abstract static class SimpleRequest implements Request {
        protected Owner   owner;
        protected String  name;


        protected SimpleRequest() {
        }

        protected SimpleRequest(Owner owner, String name) {
            this.owner=owner;
            this.name=name;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            owner.writeTo(out);
            Bits.writeString(name,out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            owner=new Owner();
            owner.readFrom(in);
            name=Bits.readString(in);
        }

        public String toString() {
            return owner + " [" + name + "]";
        }

        @Override
        public String getCounterName() {
            return name;
        }
    }

    protected static class ResendPendingRequests implements Request {
        @Override
        public void writeTo(DataOutput out) throws IOException {}
        @Override
        public void readFrom(DataInput in) throws IOException {}
        public String toString() {return "ResendPendingRequests";}

        @Override
        public String getCounterName() {
            return null;
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.RESEND_PENDING_REQUESTS;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            for(RequestCompletableFuture cf : protocol.pending_requests.values()) {
                Request request=cf.getRequest();
                protocol.traceResending(request);
                protocol.sendRequest(protocol.coord, request);
            }
        }
    }

    private void traceResending(Request request) {
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + coord + "] resending " + request);
    }

    protected static class GetOrCreateRequest extends SimpleRequest {
        protected long initial_value;

        protected GetOrCreateRequest() {}

        GetOrCreateRequest(Owner owner, String name, long initial_value) {
            super(owner,name);
            this.initial_value=initial_value;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            initial_value=Bits.readLongCompressed(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLongCompressed(initial_value, out);
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.GET_OR_CREATE;
        }

        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            VersionedValue new_val=new VersionedValue(initial_value);
            VersionedValue val=protocol.counters.putIfAbsent(name, new_val);
            if(val == null)
                val=new_val;
            long[] result = val.snapshot();
            Response rsp=new ValueResponse(owner, result);
            protocol.sendResponse(sender,rsp);
            protocol.updateBackups(name, result);
        }
    }


    protected static class DeleteRequest extends SimpleRequest {

        protected DeleteRequest() {}

        protected DeleteRequest(Owner owner, String name) {
            super(owner,name);
        }

        public String toString() {return "DeleteRequest: " + super.toString();}

        @Override
        public RequestType getRequestType() {
            return RequestType.DELETE;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            protocol.counters.remove(name);
        }
    }


    protected static class AddAndGetRequest extends SetRequest {
        protected AddAndGetRequest() {}

        protected AddAndGetRequest(Owner owner, String name, long value) {
            super(owner,name,value);
        }

        public String toString() {return "AddAndGetRequest: " + super.toString();}

        @Override
        public RequestType getRequestType() {
            return RequestType.ADD_AND_GET;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            VersionedValue val=protocol.counters.get(name);
            if(val == null) {
                protocol.sendCounterNotFoundExceptionResponse(sender, owner, name);
                return;
            }
            long[] result=val.addAndGet(value);
            Response rsp=new ValueResponse(owner, result);
            protocol.sendResponse(sender, rsp);
            if (value != 0) {
                // value == 0 means it is a counter.get(); no backup update is required.
                protocol.updateBackups(name, result);
            }
        }
    }



    protected static class SetRequest extends SimpleRequest {
        protected long value;

        protected SetRequest() {}

        protected SetRequest(Owner owner, String name, long value) {
            super(owner, name);
            this.value=value;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            value=Bits.readLongCompressed(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLongCompressed(value, out);
        }

        public String toString() {return super.toString() + ": " + value;}

        @Override
        public RequestType getRequestType() {
            return RequestType.SET;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            VersionedValue val=protocol.counters.get(name);
            if(val == null) {
                protocol.sendCounterNotFoundExceptionResponse(sender, owner, name);
                return;
            }
            long[] result=val.set(value);
            Response rsp=new ValueResponse(owner, result);
            protocol.sendResponse(sender, rsp);
            protocol.updateBackups(name, result);
        }
    }


    protected static class CompareAndSetRequest extends SimpleRequest {
        protected long expected, update;

        protected CompareAndSetRequest() {}

        protected CompareAndSetRequest(Owner owner, String name, long expected, long update) {
            super(owner, name);
            this.expected=expected;
            this.update=update;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            expected=Bits.readLongCompressed(in);
            update=Bits.readLongCompressed(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLongCompressed(expected, out);
            Bits.writeLongCompressed(update, out);
        }

        public String toString() {return super.toString() + ", expected=" + expected + ", update=" + update;}

        @Override
        public RequestType getRequestType() {
            return RequestType.COMPARE_AND_SET;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            VersionedValue val=protocol.counters.get(name);
            if(val == null) {
                protocol.sendCounterNotFoundExceptionResponse(sender, owner, name);
                return;
            }
            long[] result=val.compareAndSwap(expected, update);
            Response rsp=new ValueResponse(owner, result);
            protocol.sendResponse(sender, rsp);
            protocol.updateBackups(name, val.snapshot());
        }
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

        @Override
        public void writeTo(DataOutput out) throws IOException {
            writeReconciliation(out, names, values, versions);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            int len=in.readInt();
            names=readReconciliationNames(in, len);
            values=readReconciliationLongs(in, len);
            versions=readReconciliationLongs(in,len);
        }

        public String toString() {return "ReconcileRequest (" + names.length + ") entries";}

        @Override
        public String getCounterName() {
            return null;
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.RECONCILE;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(sender.equals(protocol.local_addr)) // we don't need to reply to our own reconciliation request
                return;

            // return all values except those with lower or same versions than the ones in the ReconcileRequest
            Map<String,VersionedValue> map=new HashMap<>(protocol.counters);
            if(names !=  null) {
                for(int i=0; i < names.length; i++) {
                    String counter_name=names[i];
                    long version=versions[i];
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

            Response rsp=new ReconcileResponse(names, values, versions);
            protocol.sendResponse(sender, rsp);
        }
    }


    protected static class UpdateRequest implements Request, BiFunction<String, VersionedValue, VersionedValue> {
        protected String name;
        protected long   value;
        protected long   version;

        protected UpdateRequest() {}

        protected UpdateRequest(String name, long value, long version) {
            this.name=name;
            this.value=value;
            this.version=version;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeString(name,out);
            Bits.writeLongCompressed(value, out);
            Bits.writeLongCompressed(version, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            name=Bits.readString(in);
            value=Bits.readLongCompressed(in);
            version=Bits.readLongCompressed(in);
        }

        public String toString() {return "UpdateRequest(" + name + ": "+ value + " (" + version + ")";}

        @Override
        public String getCounterName() {
            return name;
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.UPDATE;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            protocol.counters.compute(name, this);
        }

        @Override
        public VersionedValue apply(String name, VersionedValue versionedValue) {
            if (versionedValue == null) {
                versionedValue = new VersionedValue(value, version);
            } else {
                versionedValue.updateIfBigger(value, version);
            }
            return versionedValue;
        }
    }

    private static abstract class Response implements Streamable {

        private Owner owner;

        Response() {}

        Response(Owner owner) {
            this.owner = owner;
        }

        abstract ResponseType getResponseType();

        abstract void complete(RequestCompletableFuture completableFuture);

        final Owner getOwner() {
            return owner;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            owner.writeTo(out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            owner = new Owner();
            owner.readFrom(in);
        }
    }


    protected static class ValueResponse extends Response {
        protected long result;
        protected long version;

        protected ValueResponse() {}

        ValueResponse(Owner owner, long[] versionedValue) {
            this(owner, versionedValue[0], versionedValue[1]);
        }

        protected ValueResponse(Owner owner, long result, long version) {
            super(owner);
            this.result=result;
            this.version=version;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            result=Bits.readLongCompressed(in);
            version=Bits.readLongCompressed(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLongCompressed(result, out);
            Bits.writeLongCompressed(version, out);
        }

        public String toString() {return "ValueResponse(" + result + ")";}

        @Override
        public ResponseType getResponseType() {
            return ResponseType.VALUE;
        }

        @Override
        void complete(RequestCompletableFuture cf) {
            cf.requestCompleted(result, version);
        }
    }


    protected static class ExceptionResponse extends Response {
        protected String error_message;

        protected ExceptionResponse() {}

        protected ExceptionResponse(Owner owner, String error_message) {
            super(owner);
            this.error_message=error_message;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            error_message=Bits.readString(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeString(error_message,out);
        }

        public String toString() {return "ExceptionResponse: " + super.toString();}

        @Override
        public ResponseType getResponseType() {
            return ResponseType.EXCEPTION;
        }

        @Override
        void complete(RequestCompletableFuture cf) {
            cf.requestFailed(error_message);
        }
    }

    protected static class ReconcileResponse extends Response {
        protected String[] names;
        protected long[]   values;
        protected long[]   versions;

        protected ReconcileResponse() {}

        protected ReconcileResponse(String[] names, long[] values, long[] versions) {
            this.names=names;
            this.values=values;
            this.versions=versions;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            writeReconciliation(out,names,values,versions);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            int len=in.readInt();
            names=readReconciliationNames(in, len);
            values=readReconciliationLongs(in, len);
            versions=readReconciliationLongs(in,len);
        }

        public String toString() {
            int num=names != null? names.length : 0;
            return "ReconcileResponse (" + num + ") entries";
        }

        @Override
        public ResponseType getResponseType() {
            return ResponseType.RECONCILE;
        }

        @Override
        void complete(RequestCompletableFuture cf) {
            //no-op
        }
    }



    public static class CounterHeader extends Header {
        public Supplier<? extends Header> create() {return CounterHeader::new;}
        public short getMagicId() {return 74;}
        @Override
        public int serializedSize() {return 0;}
        @Override
        public void writeTo(DataOutput out) {}
        @Override
        public void readFrom(DataInput in) {}
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

        protected synchronized long[] compareAndSwap(long expected, long update) {
            long oldValue = value;
            if (oldValue == expected) {
                value = update;
                ++version;
            }
            return new long[]{oldValue, version};
        }

        /** Sets the value only if the version argument is greater than the own version */
        protected synchronized void updateIfBigger(long value, long version) {
            if(version > this.version) {
                this.version=version;
                this.value=value;
            }
        }

        synchronized long[] snapshot() {
            return new long[] {value, version};
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
                if (rsp == null || rsp.names == null) {
                    continue;
                }
                for(int i=0; i < rsp.names.length; i++) {
                    String counter_name=rsp.names[i];
                    long version=rsp.versions[i];
                    long value=rsp.values[i];
                    VersionedValue my_value=counters.get(counter_name);
                    if(my_value == null) {
                        counters.put(counter_name, new VersionedValue(value, version));
                        continue;
                    }
                    my_value.updateIfBigger(value, version);
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

    private static class RequestCompletableFuture extends CompletableFuture<ResponseData> {

        private final Request request;

        private RequestCompletableFuture(Request request) {
            this.request = request;
        }

        Request getRequest() {
            return request;
        }

        void requestCompleted(long value, long version) {
            this.complete(new ResponseData(request.getCounterName(), value, version));
        }

        void requestFailed(String errorMessage) {
            this.completeExceptionally(new Throwable(errorMessage));
        }
    }

    private static class ResponseData implements BiFunction<String, VersionedValue, VersionedValue> {
        private final String counterName;
        private final long value;
        private final long version;

        private ResponseData(String counterName, long value, long version) {
            this.counterName = counterName;
            this.value = value;
            this.version = version;
        }

        /**
         * Updates the VersionedValue if the version is bigger.
         */
        @Override
        public VersionedValue apply(String s, VersionedValue versionedValue) {
            if (versionedValue == null) {
                versionedValue = new VersionedValue(value, version);
            } else {
                versionedValue.updateIfBigger(value, version);
            }
            return versionedValue;
        }
    }
}
