package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Bundler implementation which sends message batches (or single messages) as soon as the target destination changes
 * (or max_bundler_size is exceeded).<p>
 * Messages are removed from the main queue one by one and processed as follows:<p>
 * A B B C C A causes the following sends: A -> {CC} -> {BB} -> A<p>
 * Note that <em>null</em> is also a valid destination (send-to-all).<p>
 * JIRA: https://issues.redhat.com/browse/JGRP-2171
 * @author Bela Ban
 * @since  4.0.4
 */
@Experimental
public class AlternatingBundler extends TransferQueueBundler {
    protected         Address       target_dest; // the current destination
    protected final   List<Message> target_list=new ArrayList<>(); // msgs for the current dest; flushed on dest change
    protected final   AverageMinMax avg_batch_size=new AverageMinMax();

    @ManagedAttribute(description="Average batch size")
    public String getAverageBatchSize() {return avg_batch_size.toString();}

    public void resetStats() {
        super.resetStats();
        avg_batch_size.clear();
    }

    public void run() {
        while(running) {
            Message msg=null;
            try {
                if((msg=queue.take()) == null) // block until first message is available
                    continue;
                int size=msg.size();
                if(count + size >= max_size) {
                    num_sends_because_full_queue.increment();
                    avg_fill_count.add(count);
                    _sendBundledMessages();
                }

                for(;;) {
                    Address dest=msg.getDest();
                    if(!Util.match(dest, target_dest) || count + size >= max_size)
                        _sendBundledMessages();
                    addMessage(msg, size);
                    msg=queue.poll();
                    if(msg == null)
                        break;
                    size=msg.size();
                }
                _sendBundledMessages();
            }
            catch(Throwable t) {
            }
        }
    }

    protected void _sendBundledMessages() {
        try {
            if(target_list.isEmpty())
                return;
            output.position(0);
            if(target_list.size() == 1)
                sendSingle(target_list.get(0).dest(), target_list.get(0), output);
            else {
                avg_batch_size.add(target_list.size());
                sendMultiple(target_dest, target_list.get(0).getSrc(), target_list, output);
            }
        }
        finally {
            target_list.clear();
            count=0;
        }
    }

    @Override
    protected void addMessage(Message msg, int size) {
        target_dest=msg.getDest();
        target_list.add(msg);
        count+=size;
    }


}
