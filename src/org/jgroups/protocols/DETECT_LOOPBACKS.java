package org.jgroups.protocols;

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
    protected final AtomicInteger count=new AtomicInteger();

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
