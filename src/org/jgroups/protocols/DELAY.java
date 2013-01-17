
package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;


/**
 * Delays incoming/outgoing messages by a random number of milliseconds (range between 0 and n
 * where n is determined by the user). Incoming messages can be delayed independently from
 * outgoing messages (or not delayed at all).<p>
 * This protocol should be inserted directly above the transport protocol (e.g. UDP).
 */
public class DELAY extends Protocol {
    @Property(description="Number of millisconds to delay passing a message up the stack")
    protected int in_delay;
    @Property(description="Number of millisconds to delay passing a message down the stack")
    protected int out_delay;

    public int  getInDelay()               {return in_delay;}
    public void setInDelay(int in_delay)   {this.in_delay=in_delay;}
    public int  getOutDelay()              {return out_delay;}
    public void setOutDelay(int out_delay) {this.out_delay=out_delay;}



    public Object down(Event evt) {
        if(out_delay > 0 && evt.getType() == Event.MSG)
            Util.sleep(computeDelay(out_delay));
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        if(in_delay > 0 && evt.getType() == Event.MSG)
            Util.sleep(computeDelay(in_delay));
        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        if(in_delay > 0)
            for(Message msg: batch)
                if(msg != null)
                    Util.sleep(computeDelay(in_delay));
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /**
     * Compute a random number between 0 and n
     */
    static int computeDelay(int n) {
        return (int)((Math.random() * 1000000) % n);
    }


}



