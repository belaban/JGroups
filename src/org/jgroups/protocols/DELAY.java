// $Id: DELAY.java,v 1.6 2005/05/30 14:31:07 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Properties;


/**
 * Delays incoming/outgoing messages by a random number of milliseconds (range between 0 and n
 * where n is determined by the user). Incoming messages can be delayed independently from
 * outgoing messages (or not delayed at all).<p>
 * This protocol should be inserted directly above the bottommost protocol (e.g. UDP).
 */

public class DELAY extends Protocol {
    int in_delay=0, out_delay=0;

    /**
     * All protocol names have to be unique !
     */
    public String getName() {
        return "DELAY";
    }


    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);

        str=props.getProperty("in_delay");
        if(str != null) {
            in_delay=Integer.parseInt(str);
            props.remove("in_delay");
        }

        str=props.getProperty("out_delay");
        if(str != null) {
            out_delay=Integer.parseInt(str);
            props.remove("out_delay");
        }

        if(props.size() > 0) {
            log.error("DELAY.setProperties(): these properties are not recognized: " + props);

            return false;
        }
        return true;
    }


    public void up(Event evt) {
        int delay=in_delay > 0 ? computeDelay(in_delay) : 0;


        switch(evt.getType()) {
            case Event.MSG:         // Only delay messages, not other events !

                if(log.isInfoEnabled()) log.info("delaying incoming message for " + delay + " milliseconds");
                Util.sleep(delay);
                break;
        }

        passUp(evt);            // Pass up to the layer above us
    }


    public void down(Event evt) {
        int delay=out_delay > 0 ? computeDelay(out_delay) : 0;

        switch(evt.getType()) {

            case Event.MSG:         // Only delay messages, not other events !

                if(log.isInfoEnabled()) log.info("delaying outgoing message for " + delay + " milliseconds");
                Util.sleep(delay);
                break;
        }

        passDown(evt);          // Pass on to the layer below us
    }


    /**
     * Compute a random number between 0 and n
     */
    int computeDelay(int n) {
        return (int)((Math.random() * 1000000) % n);
    }


}
