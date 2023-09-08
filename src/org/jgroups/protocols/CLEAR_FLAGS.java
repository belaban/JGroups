package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * Protocol which clears a set of flags in the down or up direction for all messages
 * @author Bela Ban
 * @since  4.0.4
 */
@MBean(description="Clears a given set of flags from up or down messages")
public class CLEAR_FLAGS extends Protocol {
    protected final Set<Message.Flag> down_flags=new ConcurrentSkipListSet<>();
    protected final Set<Message.Flag> up_flags=new ConcurrentSkipListSet<>();

    @ManagedAttribute(description="Shows up and down flags")
    public String flags() {
        return String.format("down flags: %s\nup_flags: %s", downFlags(), upFlags());
    }

    @ManagedOperation(description="Adds a flag to the up or down set")
    public CLEAR_FLAGS addFlag(boolean down, String flag) {
        Message.Flag fl=Message.Flag.valueOf(flag);
        return fl != null? addFlag(down, fl) : this;
    }

    public CLEAR_FLAGS addFlag(boolean down, Message.Flag flag) {
        Set<Message.Flag> s=down? down_flags : up_flags;
        s.add(flag);
        return this;
    }

    @ManagedOperation(description="Removes a flag from the up or down set")
    public CLEAR_FLAGS removeFlag(boolean down, String flag) {
        Message.Flag fl=Message.Flag.valueOf(flag);
        return fl != null? removeFlag(down, fl) : this;
    }

    public CLEAR_FLAGS removeFlag(boolean down, Message.Flag flag) {
        Set<Message.Flag> s=down? down_flags : up_flags;
        s.remove(flag);
        return this;
    }

    @ManagedOperation(description="Removes all up or down flags")
    public CLEAR_FLAGS removeAllFlags() {
        down_flags.clear();
        up_flags.clear();
        return this;
    }

    @Override public Object down(Message msg) {
        if(!down_flags.isEmpty()) {
            for(Message.Flag flag: down_flags)
                msg.clearFlag(flag);
        }
        return down_prot.down(msg);
    }

    @Override public Object up(Message msg) {
        if(!up_flags.isEmpty()) {
            for(Message.Flag fl: up_flags)
                msg.clearFlag(fl);
        }
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        if(!up_flags.isEmpty()) {
            for(Message.Flag fl: up_flags)
                for(Message msg: batch)
                    msg.clearFlag(fl);
        }
        up_prot.up(batch);
    }

    public String downFlags() {
        return down_flags.stream().map(Enum::toString).collect(Collectors.joining(", "));
    }

    public String upFlags() {
        return up_flags.stream().map(Enum::toString).collect(Collectors.joining(", "));
    }
}
