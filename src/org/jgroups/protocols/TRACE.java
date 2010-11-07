// $Id: TRACE.java,v 1.8 2009/09/06 13:51:07 belaban Exp $

package org.jgroups.protocols;
import org.jgroups.Event;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;


@Unsupported
public class TRACE extends Protocol {

    public TRACE() {}

    public Object up(Event evt) {
        System.out.println("---------------- TRACE (received) ----------------------");
        System.out.println(evt);
        System.out.println("--------------------------------------------------------");
        return up_prot.up(evt);
    }


    public Object down(Event evt) {
        System.out.println("------------------- TRACE (sent) -----------------------");
        System.out.println(evt);
        System.out.println("--------------------------------------------------------");
        return down_prot.down(evt);
    }


    public String toString() {
        return "Protocol TRACE";
    }


}
