package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ByteArray;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.MessageIterator;
import org.jgroups.util.Util;

/**
 * Serializes the entire message (including payload, headers, flags and destination and src) into the payload of
 * another message that's then sent. Deserializes the payload of an incoming message into a new message that's sent
 * up the stack.<br/>
 * To be used with {@link ASYM_ENCRYPT} or {@link SYM_ENCRYPT} when the entire message (including the headers) needs to
 * be encrypted. Can be used as a replacement for the deprecated attribute encrypt_entire_message in the above encryption
 * protocols.<br/>
 * See https://issues.jboss.org/browse/JGRP-2273 for details.
 * @author Bela Ban
 * @since  4.0.12
 */
@MBean(description="Serializes entire message into the payload of another message")
public class SERIALIZE extends Protocol {

    protected static final short GMS_ID=ClassConfigurator.getProtocolId(GMS.class);
    //@Property(description="If true, messages with no payload will not be serialized")
    //protected boolean exclude_empty_msgs=true;
    protected Address        local_addr;
    protected MessageFactory mf;

    public void init() throws Exception {
        super.init();
        mf=getTransport().getMessageFactory();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
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
        GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
        if(hdr != null)
            tmp.putHeader(GMS_ID, hdr);
        return down_prot.down(tmp);
    }

    public Object up(Message msg) {
        try {
            Message ret=deserialize(msg);
            return up_prot.up(ret);
        }
        catch(Exception e) {
            throw new RuntimeException(String.format("failed deserialize message from %s", msg.getSrc()), e);
        }
    }

    public void up(MessageBatch batch) {
        MessageIterator it=batch.iterator();
        while(it.hasNext()) {
            Message msg=it.next();
            try {
                Message deserialized_msg=deserialize(msg);
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


    protected Message deserialize(Message msg) throws Exception {
        try {
            Message ret=Util.messageFromBuffer(msg.getArray(), msg.getOffset(), msg.getLength(), mf);
            if(ret.getDest() == null)
                ret.setDest(msg.getDest());
            if(ret.getSrc() == null)
                ret.setSrc(msg.getSrc());
            return ret;
        }
        catch(Exception e) {
            throw new Exception(String.format("failed deserialize message from %s", msg.getSrc()), e);
        }
    }
}
