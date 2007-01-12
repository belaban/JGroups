// $Id: HDRS.java,v 1.5 2007/01/12 14:20:18 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;


/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */
public class HDRS extends Protocol {
    public String  getName() {return "HDRS";}


    private void printMessage(Message msg, String label) {
        System.out.println("------------------------- " + label + " ----------------------");
        System.out.println(msg);
        msg.printObjectHeaders();
        System.out.println("--------------------------------------------------------------");
    }


    public Object up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            printMessage(msg, "up");
        }
        return up_prot.up(evt); // Pass up to the layer above us
    }



    public Object down(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            printMessage(msg, "down");
        }

        return down_prot.down(evt);  // Pass on to the layer below us
    }


}
