package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;
import org.jgroups.Message;
import org.jgroups.protocols.RTTHeader;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.RpcStats;

import java.util.Iterator;
import java.util.List;

/**
 * @author Bela Ban
 * @since  5.2.1
 */
public class RttTestHelper extends Helper {
    protected RttTestHelper(Rule rule) {
        super(rule);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void attachHeader(Message msg) {
        msg.putHeader(RTTHeader.RTT_ID, new RTTHeader());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setSendRequestTime(Message msg) {
        RTTHeader hdr=getHeader(msg);
        if(hdr != null)
            hdr.sendReq(System.nanoTime());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setSerializeTime(Message msg) {
        RTTHeader hdr=getHeader(msg);
        if(hdr != null)
            hdr.serialize(System.nanoTime());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setDeserializeTime(Message msg) {
        RTTHeader hdr=getHeader(msg);
        if(hdr != null)
            hdr.deserialize(System.nanoTime());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setSerializeTime(List<Message> list) {
        long time=System.nanoTime();
        for(Message msg: list) {
            if(msg != null) {
                RTTHeader hdr=getHeader(msg);
                if(hdr != null)
                    hdr.serialize(time);
            }
        }
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setSerializeTime(MessageBatch batch) {
        long time=System.nanoTime();
        for(Message msg: batch) {
            if(msg != null) {
                RTTHeader hdr=getHeader(msg);
                if(hdr != null)
                    hdr.serialize(time);
            }
        }
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setDeserializeTime(MessageBatch batch) {
        long time=System.nanoTime();
        for(Message msg: batch) {
            if(msg != null) {
                RTTHeader hdr=getHeader(msg);
                if(hdr != null)
                    hdr.deserialize(time);
            }
        }
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setReceiveReqTime(Message msg) {
        RTTHeader hdr=getHeader(msg);
        if(hdr != null)
            hdr.receiveReq(System.nanoTime());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setSendRspTime(Message msg) {
        RTTHeader hdr=getHeader(msg);
        if(hdr != null)
            hdr.sendRsp(System.nanoTime());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void setRspDispatchedTime(Message msg, RpcStats rpc_stats) {
        if(rpc_stats == null)
            return;
        RTTHeader hdr=getHeader(msg);
        if(hdr != null) {
            hdr.rspDispatched(System.nanoTime());
            rpc_stats.addRTTStats(msg.src(), hdr);
        }
    }

    @SuppressWarnings("MethodMayBeStatic")
    public void copyHeader(Message request, Message response) {
        RTTHeader hdr=getHeader(request);
        if(hdr != null)
            response.putHeader(RTTHeader.RTT_ID, hdr);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public String printHeader(Message msg) {
        RTTHeader hdr=getHeader(msg);
        return hdr == null? "n/a" : hdr.toString();
    }

    protected static RTTHeader getHeader(Message msg) {
        return msg.getHeader(RTTHeader.RTT_ID);
    }

}
