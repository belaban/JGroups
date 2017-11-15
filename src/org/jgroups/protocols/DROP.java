package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Protocol which drops up or down messages according to user-defined filters
 * @author Bela Ban
 * @since  4.0.1
 */
@MBean(description="Drops up or down messages according to user-defined filters")
public class DROP extends Protocol {
    protected final List<Predicate<Message>> down_filters=new ArrayList<>(), up_filters=new ArrayList<>();

    public DROP addDownFilter(Predicate<Message> filter) {
        down_filters.add(filter);
        return this;
    }

    public DROP addUpFilter(Predicate<Message> filter) {
        up_filters.add(filter);
        return this;
    }

    public DROP removeDownFilter(Predicate<Message> filter) {
        down_filters.remove(filter);
        return this;
    }

    public DROP removeUpFilter(Predicate<Message> filter) {
        up_filters.remove(filter);
        return this;
    }

    public DROP clearUpFilters()   {up_filters.clear(); return this;}
    public DROP clearDownFilters() {down_filters.clear(); return this;}


    public Object down(Message msg) {
        for(Predicate<Message> pred: down_filters)
            if(pred.test(msg)) {
                dropped(msg, true);
                return null;
            }
        return down_prot.down(msg);
    }

    public Object up(Message msg) {
        for(Predicate<Message> pred: up_filters)
            if(pred.test(msg)) {
                dropped(msg, false);
                return null;
            }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            for(Predicate<Message> pred: up_filters) {
                if(pred.test(msg)) {
                    dropped(msg, false);
                    batch.remove(msg);
                    break;
                }
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void dropped(Message msg, boolean down) {

        log.trace("dropped msg %s hdrs: %s\n", down? "to " + msg.getDest() : "from " + msg.getSrc(), msg.printHeaders());
    }
}
