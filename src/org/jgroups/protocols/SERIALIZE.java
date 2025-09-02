package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Message;
import org.jgroups.MessageFactory;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ByteArray;
import org.jgroups.util.FastArray;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

/**
 * Serializes the entire message (including payload, headers, flags and destination and src) into the payload of
 * another message that's then sent. Deserializes the payload of an incoming message into a new message that's sent
 * up the stack.<p>
 * To be used with {@link ASYM_ENCRYPT} or {@link SYM_ENCRYPT} when the entire message (including the headers) needs to
 * be encrypted. Can be used as a replacement for the deprecated attribute encrypt_entire_message in the above encryption
 * protocols.<p>
 * See https://issues.redhat.com/browse/JGRP-2273 for details.
 * @author Bela Ban
 * @since  4.0.12
 */
@MBean(description="Serializes entire message into the payload of another message")
public class SERIALIZE extends Protocol {
    protected static final short GMS_ID=ClassConfigurator.getProtocolId(GMS.class);
    protected MessageFactory     mf;

    public void init() throws Exception {
        super.init();
        mf=getTransport().getMessageFactory();
    }

    public Object down(Message msg) {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        ByteArray serialized_msg=null;
        try {
            serialized_msg=Util.messageToBuffer(msg);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        // exclude existing headers, they will be seen again when we unmarshal the message at the receiver
        Message tmp=new BytesMessage(msg.dest(), serialized_msg).setFlag(msg.getFlags(), false);
        if(msg.isFlagSet(Message.TransientFlag.DONT_LOOPBACK))
            tmp.setFlag(Message.TransientFlag.DONT_LOOPBACK);
        GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
        if(hdr != null)
            tmp.putHeader(GMS_ID, hdr);
        return down_prot.down(tmp);
    }

    public Object up(Message msg) {
        try {
            Message ret=deserialize(msg.src(), msg.getArray(), msg.getOffset(), msg.getLength());
            return up_prot.up(ret);
        }
        catch(Exception e) {
            throw new RuntimeException(String.format("failed deserialize message from %s", msg.getSrc()), e);
        }
    }

    public void up(MessageBatch batch) {
        FastArray<Message>.FastIterator it=(FastArray<Message>.FastIterator)batch.iterator();
        while(it.hasNext()) {
            Message msg=it.next();
            try {
                Message deserialized_msg=deserialize(msg.src(), msg.getArray(), msg.getOffset(), msg.getLength());
                it.replace(deserialized_msg);
            }
            catch(Exception e) {
                log.error("failed deserializing message", e);
                it.remove();
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected Message deserialize(Address sender, byte[] buf, int offset, int length) throws Exception {
        try {
            Message msg=Util.messageFromBuffer(buf, offset, length, mf);
            if(msg.getDest() == null)
                msg.setDest(msg.getDest());
            if(msg.getSrc() == null)
                msg.setSrc(msg.getSrc());
            return msg;
        }
        catch(Exception e) {
            throw new Exception(String.format("failed deserialize message from %s", sender), e);
        }
    }
}
