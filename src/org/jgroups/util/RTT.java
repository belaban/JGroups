package org.jgroups.util;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.TpHeader;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Measures round-trip times (RTT) between nodes
 * @author Bela Ban
 * @since  5.4, 5.3.8
 */
@MBean(description="Component measuring round-trip times to all other nodes")
public class RTT {
    protected TP                transport;
    protected short             tp_id;

    @Property(description="Enables or disables RTT functionality")
    protected boolean           enabled;

    @Property(description="The number of RPCs to send")
    protected int               num_reqs=10;

    @Property(description="Timeout (ms) for an RPC")
    protected long              timeout=1000;

    @Property(description="Size (in bytes) of an RPC")
    protected int               size;

    @Property(description="Send requests and responses as OOB messages")
    protected boolean           oob;

    protected final Map<Address,AverageMinMax> rtts=Util.createConcurrentMap();
    protected final Map<Address,long[]>        times=Util.createConcurrentMap(); // list of start times (us)

    public boolean enabled()              {return enabled;}
    public RTT     enabled(boolean f)     {enabled=f; return this;}
    public int     numReqs()              {return num_reqs;}
    public RTT     numReqs(int n)         {this.num_reqs=n; return this;}
    public long    timeout()              {return timeout;}
    public RTT     timeout(long t)        {this.timeout=t; return this;}
    public int     size()                 {return size;}
    public RTT     size(int size)         {this.size=size; return this;}
    public boolean oob()                  {return oob;}
    public RTT     oob(boolean b)         {oob=b; return this;}

    public void init(TP tp) {
        this.transport=Objects.requireNonNull(tp);
        this.tp_id=transport.getId();
    }

    @ManagedOperation(description="Sends N RPCs to all other nodes and computes min/avg/max RTT")
    public String rtt() {
        return rtt(num_reqs, size, true, false);
    }

    /**
     * Sends N requests to all members and computes RTTs
     * @param num_reqs The number of requests to be sent to all members
     * @param details Whether to print details (e.g. min/max/percentiles)
     */
    @ManagedOperation(description="Sends N RPCs to all other nodes and computes min/avg/max RTT")
    public String rtt(int num_reqs, boolean details) {
        return rtt(num_reqs, this.size, details, false);
    }

    /**
     * Sends N requests to all members and computes RTTs
     * @param num_reqs The number of requests to be sent to all members
     * @param size The number of bytes a request should have
     * @param details Whether to print details (e.g. min/max/percentiles)
     * @param exclude_self Whether to exclude the local node
     */
    @ManagedOperation(description="Sends N RPCs to all other nodes and computes min/avg/max RTT")
    public String rtt(int num_reqs, int size, boolean details, boolean exclude_self) {
        if(!enabled)
            return "RTT functionality is disabled";
        Map<Address,AverageMinMax> m=_rtt(num_reqs, size, exclude_self);
        return m.entrySet().stream()
          .map(e -> String.format("%s: %s", e.getKey(), print(e.getValue(), details, TimeUnit.MICROSECONDS, num_reqs)))
          .collect(Collectors.joining("\n"));
    }

    public Map<Address,AverageMinMax> _rtt(int num_reqs, int size, boolean exclude_self) {
        rtts.clear();
        times.clear();
        View view=transport.view();
        byte[] payload=new byte[size];
        final List<Address> targets=new ArrayList<>(view.getMembers());
        if(exclude_self)
            targets.remove(transport.addr());
        for(Address addr: targets) {
            rtts.put(addr, new AverageMinMax().usePercentiles(128).unit(TimeUnit.MICROSECONDS));
            times.put(addr, new long[num_reqs]);
        }
        AsciiString cluster=transport.getClusterNameAscii();
        for(int i=0; i < num_reqs; i++) {
            TpHeader hdr=new TpHeader(cluster, TpHeader.REQ, i);
            for(Address addr: targets) {
                Message msg=new BytesMessage(addr, payload).putHeader(tp_id, hdr);
                if(oob)
                    msg.setFlag(Message.Flag.OOB);
                long[] t=times.get(addr);
                t[i]=Util.micros();
                transport.down(msg);
            }
        }
        Util.waitUntilTrue(timeout, timeout/10, () -> rtts.values().stream().allMatch(a -> a.count() >= num_reqs));
        return new HashMap<>(rtts);
    }

    /** Called when a message (request or response) is received */
    public void handleMessage(Message msg, TpHeader hdr) {
        if(hdr != null) {
            switch(hdr.flag()) {
                case TpHeader.REQ:
                    handleRequest(msg.src(), hdr);
                    break;
                case TpHeader.RSP:
                    handleResponse(msg.src(), hdr.index());
            }
        }
    }

    protected void handleRequest(Address sender, TpHeader hdr) {
        Message rsp=new EmptyMessage(sender)
          .putHeader(tp_id, new TpHeader(transport.getClusterNameAscii(), TpHeader.RSP, hdr.index()));
        if(oob)
            rsp.setFlag(Message.Flag.OOB);
        transport.down(rsp);
    }

    protected void handleResponse(Address sender, int index) {
        long[] start_times=times.get(sender);
        if(start_times != null) {
            long time=Util.micros() - start_times[index];
            AverageMinMax avg=rtts.get(sender);
            if(avg != null)
                avg.add(time);
        }
    }

    protected static String print(AverageMinMax avg, boolean details, TimeUnit unit, int num_reqs) {
        return details? avg.toString(unit) + String.format(" (%s)", percentiles(avg, num_reqs))
          : Util.printTime(avg.average(), unit);
    }

    protected static String percentiles(AverageMinMax avg, int num_reqs) {
        List<Double> values=avg.values();
        int received=values.size(), non_received=num_reqs - received;
        double failure_rate=non_received == 0? 0.0 : (double)non_received / received;
        String failures=non_received == 0? "" : String.format(" (failure rate: %.2f)", failure_rate);
        return String.format("p90=%s p99=%s p99.9=%s%s", Util.printTime(avg.p(90), avg.unit()),
                             Util.printTime(avg.p(99), avg.unit()),
                             Util.printTime(avg.p(99.9), avg.unit()), failures);
    }

}
