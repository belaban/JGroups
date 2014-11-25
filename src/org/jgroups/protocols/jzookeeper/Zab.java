package org.jgroups.protocols.jzookeeper;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.stack.Protocol;

/**
 * Implementation of total order protocol using a sequencer.
 * Consult <a href="https://github.com/belaban/JGroups/blob/master/doc/design/SEQUENCER.txt">SEQUENCER.txt</a> for details
 * @author Ibrahim EL-Sanosi 
 */
@MBean(description="Implementation of Zppkeeper Atomic broadcast protocol using a JGroups")
public class Zab extends Protocol{

    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    protected volatile boolean                  is_coord=false;
    protected final AtomicLong                  seqno=new AtomicLong(0);
    
    /** Maintains messages forwarded to the coord which which no ack has been received yet.
     *  Needs to be sorted so we resend them in the right order
     */
    protected final NavigableMap<Long,Message>  forward_table=new ConcurrentSkipListMap<Long,Message>();

    protected final Lock                        send_lock=new ReentrantLock();

    protected final Condition                   send_cond=send_lock.newCondition();

    /** When ack_mode is set, we need to wait for an ack for each forwarded message until we can send the next one */
    protected volatile boolean                  ack_mode=true;
    
    /** Set when we block all sending threads to resend all messages from forward_table */
    protected volatile boolean                  flushing=false;

    protected volatile boolean                  running=true;
    
    /** Keeps track of the threads sending messages */
    protected final AtomicInteger               in_flight_sends=new AtomicInteger(0);




	

}
