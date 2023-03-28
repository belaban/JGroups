package org.jgroups.util;

import org.jgroups.JChannel;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.MessageProcessingPolicy;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.util.Objects;

/**
 * Common functions for all tests
 * @author Bela Ban
 * @since  5.2.15
 */
public class Tests {

    public static boolean mcastRetransmissionAvailable(JChannel... channels) {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            Protocol nak=stack.findProtocol(NAKACK2.class);
            if(nak == null || ((NAKACK2)nak).getXmitInterval() <= 0)
                return false;
        }
        return true;
    }

    public static boolean ucastRetransmissionAvailable(JChannel... channels) {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            Protocol prot=stack.findProtocol(UNICAST3.class);
            if(prot == null || !((UNICAST3)prot).isXmitsEnabled())
                return false;
        }
        return true;
    }


    public static boolean hasThreadPool(JChannel ... channels) {
        for(JChannel ch: channels) {
            TP tp=ch.getProtocolStack().getTransport();
            if(!tp.getThreadPool().isEnabled())
                return false;
        }
        return true;
    }

    public static boolean processingPolicyIs(Class<? extends MessageProcessingPolicy> policy_class, JChannel ... channels) {
        for(JChannel ch: channels) {
            TP tp=ch.getProtocolStack().getTransport();
            MessageProcessingPolicy pol=tp.getMessageProcessingPolicy();
            if(!Objects.equals(policy_class, pol.getClass()))
                return false;
        }
        return true;
    }
}
