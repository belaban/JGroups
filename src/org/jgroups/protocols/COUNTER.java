package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Owner;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Protocol which is used by {@link org.jgroups.blocks.atomic.CounterService} to provide a distributed atomic counter
 * @author Bela Ban
 * @since 3.0.0
 */
@MBean(description="Protocol to maintain distributed atomic counters")
public class COUNTER extends Protocol {

    @Property(description="bypasses message bundling if set")
    protected boolean bypass_bundling=true;


    protected Address local_addr;

    protected View view;

    // server side counters
    protected final ConcurrentMap<String,AtomicInteger> counters=Util.createConcurrentMap(20);

    // client side counters
    protected final Map<String,Map<Owner,CounterImpl>> client_locks=new HashMap<String,Map<Owner,CounterImpl>>();



    protected static enum Type {
        CREATE_OR_GET,
        GET,
        SET,
        COMPARE_AND_SET,
        INCR_AND_GET,
        DECR_AND_GET
    }



    public COUNTER() {
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

   
    @Override
    public Object down(Event evt) {
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                CounterHeader hdr=(CounterHeader)msg.getHeader(id);
                if(hdr == null)
                    break;

                Request req=(Request)msg.getObject();
                if(log.isTraceEnabled())
                    log.trace("[" + local_addr + "] <-- [" + msg.getSrc() + "] " + req);
                switch(req.type) {
                   
                    default:
                        log.error("Request of type " + req.type + " not known");
                        break;
                }
                return null;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }



    @ManagedOperation(description="Dumps all counters")
    public String printCounters() {
        StringBuilder sb=new StringBuilder();
        sb.append("counters:\n");
        for(Map.Entry<String,AtomicInteger> entry: counters.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    protected void handleView(View view) {
        this.view=view;
        if(log.isDebugEnabled())
            log.debug("view=" + view);
        List<Address> members=view.getMembers();


        // todo: if the coordinator failed, we need to get counter information from a backup-coord, or by
        // contacting all members and get the last values (locally cached)
    }





    protected void sendRequest(Address dest, Type type, String name) {
        Request req=new Request(type, name);
        Message msg=new Message(dest, null, req);
        msg.putHeader(id, new CounterHeader());
        if(bypass_bundling)
            msg.setFlag(Message.DONT_BUNDLE);
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + (dest == null? "ALL" : dest) + "] " + req);
        try {
            down_prot.down(new Event(Event.MSG, msg));
        }
        catch(Exception ex) {
            log.error("failed sending " + type + " request: " + ex);
        }
    }


    protected void sendLockResponse(Address dest, Type type, String name) {
        Request rsp=new Request(type, name);
        Message lock_granted_rsp=new Message(dest, null, rsp);
        lock_granted_rsp.putHeader(id, new CounterHeader());
        if(bypass_bundling)
            lock_granted_rsp.setFlag(Message.DONT_BUNDLE);

        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + dest + "] " + rsp);

        try {
            down_prot.down(new Event(Event.MSG, lock_granted_rsp));
        }
        catch(Exception ex) {
            log.error("failed sending " + type + " message to " + dest + ": " + ex);
        }
    }




    protected static class CounterImpl implements Counter {
        protected final String name;
        protected final COUNTER counter_prot;

        public CounterImpl(String name, COUNTER counter_prot) {
            this.name = name;
            this.counter_prot = counter_prot;
        }

        @Override
        public int get() {
            return 0;
        }

        @Override
        public void set(int new_value) {
        }

        @Override
        public boolean compareAndSet(int expect, int update) {
            return false;
        }

        @Override
        public int incrementAndGet() {
            return 0;
        }

        @Override
        public int decrementAndGet() {
            return 0;
        }

        @Override
        public String toString() {
            return String.valueOf(get());
        }
    }







    protected static class Request implements Streamable {
        protected Type    type;
        protected String  name;
        protected int     value;


        public Request() {
        }

        public Request(Type type, String name) {
            this.type=type;
            this.name=name;
        }


        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type.ordinal());
            Util.writeString(name, out);
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
            type= Type.values()[in.readByte()];
            name=Util.readString(in);
        }

        public String toString() {
            return type.name() + " [" + name;
        }

    }


    public static class CounterHeader extends Header {

        public CounterHeader() {
        }

        public int size() {
            return 0;
        }

        public void writeTo(DataOutput out) throws IOException {
        }

        public void readFrom(DataInput in) throws IOException, IllegalAccessException, InstantiationException {
        }
    }

}
