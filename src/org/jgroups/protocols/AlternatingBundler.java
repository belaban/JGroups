package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bundler implementation which sends message batches (or single messages) as soon as the target destination changes
 * (or max_bundler_size would be exceeded).<br/>
 * Messages are removed from the main queue one by one and processed as follows:<br/>
 * A B B C C A causes the following sends: A -> {CC} -> {BB} -> A<br/>
 * Note that <em>null</em> is also a valid destination (send-to-all).<br/>
 * JIRA: https://issues.jboss.org/browse/JGRP-2171
 * @author Bela Ban
 * @since  4.0.4
 */
@Experimental
public class AlternatingBundler extends TransferQueueBundler implements DiagnosticsHandler.ProbeHandler {
    protected         Address       target_dest; // the current destination
    protected final   List<Message> target_list=new ArrayList<>(); // msgs for the current dest; flushed on dest change
    protected final   AverageMinMax avg_batch_size=new AverageMinMax();


    public synchronized void start() {
        super.start();
        transport.registerProbeHandler(this);
    }

    public synchronized void stop() {
        transport.unregisterProbeHandler(this);
        super.stop();
    }

    public void run() {
        while(running) {
            Message msg=null;
            try {
                if((msg=queue.take()) == null) // block until first message is available
                    continue;
                int size=msg.size();
                if(count + size >= transport.getMaxBundleSize()) {
                    num_sends_because_full_queue++;
                    fill_count.add(count);
                    _sendBundledMessages();
                }

                for(;;) {
                    Address dest=msg.getDest();
                    if(!Util.match(dest, target_dest) || count + size >= transport.getMaxBundleSize())
                        _sendBundledMessages();
                    _addMessage(msg, size);
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

    public Map<String,String> handleProbe(String... keys) {
        Map<String,String> map=new HashMap<>();
        for(String key: keys) {
            switch(key) {
                case "ab.avg_batch_size":
                    map.put(key, avg_batch_size.toString());
                    break;
                case "ab.avg_batch_size.reset":
                    avg_batch_size.clear();
                    break;
            }
        }
        return map;
    }

    public String[] supportedKeys() {
        return new String[]{"ab.avg_batch_size", "ab.avg_batch_size.reset"};
    }

    protected void _sendBundledMessages() {
        try {
            if(target_list.isEmpty())
                return;
            output.position(0);
            if(target_list.size() == 1) {
                sendSingleMessage(target_list.get(0));
                // avg_batch_size.add(1);
            }
            else {
                avg_batch_size.add(target_list.size());
                sendMessageList(target_dest, target_list.get(0).getSrc(), target_list);
                if(transport.statsEnabled())
                    transport.incrBatchesSent(1);
            }
        }
        finally {
            target_list.clear();
            count=0;
        }
    }

    protected void _addMessage(Message msg, long size) {
        target_dest=msg.getDest();
        target_list.add(msg);
        count+=size;
    }


}
