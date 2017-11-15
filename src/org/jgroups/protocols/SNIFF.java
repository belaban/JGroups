package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/**
 * Protocol trying to print message payloads as strings
 * @author Bela Ban
 * @since  4.0
 */
@MBean(description="Protocol trying to print payloads as strings")
public class SNIFF extends Protocol {
    @Property(description="Print received messages")
    protected boolean up=true;

    @Property(description="Print sent messages")
    protected boolean down;


    public Object down(Message msg) {
        if(down)
            dump("down msg", msg);
        return down_prot.down(msg);
    }

    public Object up(Message msg) {
        if(up)
            dump("up msg", msg);
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        int count=1;
        for(Message msg: batch)
            dump("batch msg#" + count++, msg);

        up_prot.up(batch);
    }



    protected static void dump(String type, Message msg) {
        StringBuilder sb=new StringBuilder();
        sb.append(String.format("\n%s from %s (%d bytes):\nhdrs: %s\n", type, msg.getSrc(), msg.getLength(), msg.printHeaders()));
        if(msg.getLength() > 0) {
            sb.append("payload: ");
            printPayload(msg, sb);
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }

    protected static String printPayload(Message msg, final StringBuilder sb) {
        byte[] payload=msg.getArray();
        int print_max=Math.min(msg.getLength(), 50);
        for(int i=msg.getOffset(); i < print_max; i++) {
            byte ch=payload[i];
            sb.append((char)ch);
        }
        return null;
    }
}
