package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.util.Buffer;
import org.jgroups.util.FixedBuffer;

/**
 * New multicast protocols based on fixed-size xmit windows and message ACKs<rb/>
 * Details: https://issues.redhat.com/browse/JGRP-2780
 * @author Bela Ban
 * @since  5.4
 */
public class NAKACK4 extends ReliableMulticast {
    protected static final Buffer.Options SEND_OPTIONS=new Buffer.Options().block(true);

    @Override
    protected Buffer<Message> createXmitWindow(long initial_seqno) {
        return new FixedBuffer<>(initial_seqno);
    }

    @Override
    protected Buffer.Options sendOptions() {return SEND_OPTIONS;}
}
