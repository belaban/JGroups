// $Id: TRACE.java,v 1.6 2007/07/30 12:56:38 belaban Exp $

package org.jgroups.protocols;
import org.jgroups.Event;
import org.jgroups.stack.Protocol;



public class TRACE extends Protocol {

    public TRACE() {}

    public String        getName()             {return "TRACE";}

    

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
