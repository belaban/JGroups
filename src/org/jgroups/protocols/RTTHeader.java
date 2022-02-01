package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Header to measure round-trip times (in nanoseconds) for sync RPCs (https://issues.redhat.com/browse/JGRP-2604)
 * @author Bela Ban
 * @since  5.2.1
 */
public class RTTHeader extends Header {
    public static final short RTT_ID=1500; // dummy protocol ID to get the RTTHeader from a message

    protected long send_req;        // when the request is sent (e.g. in RequestCorrelator)
    protected long serialize_req;   // when the request is serialized at the caller

    protected long deserialize_req; // the request is de-serialized at the receiver
    protected long receive_req;     // the RequestCorrelator receives the request
    protected long send_rsp;        // the RequestCorrelator is about to send the request
    protected long serialize_rsp;   // just before the response is serialized at the receiver

    protected long deserialize_rsp; // just after the response has been deserialized at the caller
    protected long rsp_dispatched;  // the RequestCorrelator has handed over the response to the caller


    public RTTHeader() {
    }

    public Supplier<? extends Header> create()   {return RTTHeader::new;}
    public short     getMagicId()                {return 97;}

    public RTTHeader sendReq(long nanos)         {send_req=nanos; return this;}

    /**
     * Since we don't know whether we're serializing a request or a response, the following happens: since the request
     * is serialized _before_ the response, we check if {@link #serialize_req} is 0: if so we set it, otherwise we
     * set {@link #serialize_rsp} instead.
     */
    public RTTHeader serialize(long nanos) {
        if(serialize_req == 0)
            serialize_req=nanos;
        else
            serialize_rsp=nanos;
        return this;
    }


    /** Sets either {@link #deserialize_req} or {@link #deserialize_rsp}; same logic as {@link #serialize(long)}.
     */
    public RTTHeader deserialize(long nanos) {
        if(deserialize_req == 0)
            deserialize_req=nanos;
        else
            deserialize_rsp=nanos;
        return this;
    }

    public RTTHeader receiveReq(long nanos) {receive_req=nanos; return this;}
    public RTTHeader sendRsp(long nanos)    {send_rsp=nanos; return this;}

    public RTTHeader rspDispatched(long nanos)   {rsp_dispatched=nanos; return this;}

    /** The total time for a round-trip */
    public long totalTime() {
        return send_req > 0 && rsp_dispatched > 0? rsp_dispatched-send_req : 0;
    }

    /** Time to send a request down, from sending until just before serialization */
    public long downRequest() {
        return send_req > 0 && serialize_req > 0? serialize_req-send_req : 0;
    }

    /** The time the request has spent on the network, between serializing it at the caller and de-serializing it
        at the receiver */
    public long networkRequest() {
        return deserialize_req > 0 && serialize_req > 0? deserialize_req-serialize_req : 0;
    }

    /** The time after deserializing a request until before it is dispatched to the application */
    public long upReq() {
        return receive_req > 0 && deserialize_req > 0? receive_req-deserialize_req : 0;
    }

    /** The time between deserialization of a response and after dispatching to the application */
    public long upRsp() {
        return deserialize_rsp > 0 && rsp_dispatched > 0? rsp_dispatched-deserialize_rsp : 0;
    }

    /** Time between reception of a message and sending of a response (= time spent in application code) */
    public long processingTime() {
        return receive_req > 0 && send_rsp > 0? send_rsp-receive_req : 0;
    }

    /** The time a response has spent on the network, between serializing the response and de-serializing it */
    public long networkResponse() {
        return deserialize_rsp > 0 && serialize_rsp > 0? deserialize_rsp-serialize_rsp : 0;
    }

    public String toString() {
        return String.format("total=%s down req=%s network req=%s network rsp=%s",
                             print(totalTime()), print(downRequest()), print(networkRequest()), print(networkResponse()));
    }

    public int serializedSize() {
        return 8 * Long.BYTES;
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(send_req);
        out.writeLong(serialize_req);
        out.writeLong(deserialize_req);
        out.writeLong(receive_req);
        out.writeLong(send_rsp);
        out.writeLong(serialize_rsp);
        out.writeLong(deserialize_rsp);
        out.writeLong(rsp_dispatched);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        send_req=in.readLong();
        serialize_req=in.readLong();
        deserialize_req=in.readLong();
        receive_req=in.readLong();
        send_rsp=in.readLong();
        serialize_rsp=in.readLong();
        deserialize_rsp=in.readLong();
        rsp_dispatched=in.readLong();
    }

    protected static String print(long r) {
        return r <= 0? "n/a" : Util.printTime(r, TimeUnit.NANOSECONDS);
    }
}
