package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Detects unicast loopbacks: messages where dest == local address
 * @author Bela Ban
 * @since  5.1.6
 */
@MBean(description="Detects unicast loopback messages")
public class DETECT_LOOPBACKS extends Protocol {

    @Property(description="Prints to stdout")
    protected boolean             print_to_stdout=true;
    protected Address             local_addr;
    protected final AtomicInteger count=new AtomicInteger();


    public Object down(Event evt) {
        if(evt.getType() == Event.SET_LOCAL_ADDRESS)
            local_addr=evt.getArg();
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        if(Objects.equals(local_addr, msg.getDest())) {
            String s=String.format("[%d] %s, headers: %s", count.getAndIncrement(), msg, msg.getHeaders());
            if(print_to_stdout)
                System.out.printf("%s\n", s);
            else if(log.isTraceEnabled())
                log.trace(s);
        }
        return down_prot.down(msg);
    }
}
