// $Id: DELAY.java,v 1.14 2009/09/06 13:51:07 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;


/**
 * Delays incoming/outgoing messages by a random number of milliseconds (range between 0 and n
 * where n is determined by the user). Incoming messages can be delayed independently from
 * outgoing messages (or not delayed at all).<p>
 * This protocol should be inserted directly above the bottommost protocol (e.g. UDP).
 */

@Unsupported
public class DELAY extends Protocol {
    @Property
    int in_delay=0;
    @Property
    int out_delay=0;


    public int getInDelay() {
    	return in_delay ;
    }
    
    public void setInDelay(int in_delay) {
    	this.in_delay=in_delay ;
    }

    public int getOutDelay() {
    	return out_delay ;
    }
    
    public void setOutDelay(int out_delay) {
    	this.out_delay=out_delay ;
    }
    
    public Object up(Event evt) {
        int delay=in_delay > 0 ? computeDelay(in_delay) : 0;


        switch(evt.getType()) {
            case Event.MSG:         // Only delay messages, not other events !

                if(log.isInfoEnabled()) log.info("delaying incoming message for " + delay + " milliseconds");
                Util.sleep(delay);
                break;
        }

        return up_prot.up(evt);            // Pass up to the layer above us
    }


    public Object down(Event evt) {
        int delay=out_delay > 0 ? computeDelay(out_delay) : 0;

        switch(evt.getType()) {

            case Event.MSG:         // Only delay messages, not other events !

                if(log.isInfoEnabled()) log.info("delaying outgoing message for " + delay + " milliseconds");
                Util.sleep(delay);
                break;
        }

        return down_prot.down(evt);          // Pass on to the layer below us
    }


    /**
     * Compute a random number between 0 and n
     */
    static int computeDelay(int n) {
        return (int)((Math.random() * 1000000) % n);
    }


}



