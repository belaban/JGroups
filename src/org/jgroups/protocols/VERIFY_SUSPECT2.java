
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;


/**
 * Double-checks that a suspected member is really dead.<br/>
 * Details: https://issues.redhat.com/browse/JGRP-2558<br/>
 * Design: https://github.com/belaban/JGroups/blob/master/doc/design/VERIFY_SUSPECT2.txt
 * @author Bela Ban
 */
@MBean(description="Double-checks suspicions reports")
public class VERIFY_SUSPECT2 extends Protocol implements Callable<Void> {

    @Property(description="Number of millis to wait for verification that a suspect is really dead (approximation)",
      type=AttributeType.TIME)
    protected long                    timeout=1000;

    @Property(description="Number of verify heartbeats sent to a suspected member")
    protected int                     num_msgs=1;

    @Property(description="Send the I_AM_NOT_DEAD message back as a multicast rather than as multiple unicasts " +
      "(default is false)")
    protected boolean                 use_mcast_rsps;

    protected final Set<Entry>        suspects=new HashSet<>(); // for suspects (no duplicates)

    @ManagedAttribute(description="Is the verifying task is running?")
    protected boolean                 running;

    protected ExecutorService         thread_pool; // single-threaded+queue, runs the task when suspects are added



    @ManagedAttribute(description = "List of currently suspected members")
    public synchronized String getSuspects() {return suspects.toString();}

    private Future<Void> suspectsThread;




    public VERIFY_SUSPECT2() {
    }

    /* ------------------------------------------ Builder-like methods  ------------------------------------------ */

    public VERIFY_SUSPECT2 setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public long getTimeout() {
        return timeout;
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View v=evt.getArg();
                synchronized(this) {
                    suspects.removeIf(e -> !v.containsMember(e.suspect));
                }
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.SUSPECT:  // it all starts here ...
                if(evt.arg() == null)
                    return null;
                // need to copy if we get an unmodifiable collection (we do modify the collection)
                Collection<Address> s=new ArrayList<>(evt.arg());
                s.remove(local_addr); // ignoring suspect of self
                verifySuspect(s);
                return null;  // don't pass up; we will decide later (after verification) whether to pass it up
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        VerifyHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);
        return handle(hdr);
    }

    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            VerifyHeader hdr=msg.getHeader(id);
            if(hdr != null) {
                it.remove();
                handle(hdr);
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /**
     * Started when a suspected member is added to suspects. Clears the bucket and sends the contents up the stack as
     * SUSPECT events, then moves suspects from the queue to the bucket. Runs until the queue and bucket are both empty.
     */
    public Void call() {
        List<Entry> suspected_mbrs=new ArrayList<>();
        while(true) {
            long target_timeout=System.currentTimeMillis() + timeout;
            log.debug("%s: VERIFY_SUSPECT2 verification polling thread has started with timeout %d, next timeout in %d", local_addr, timeout, target_timeout);
            while(target_timeout - System.currentTimeMillis() > 0 && !Thread.currentThread().isInterrupted()) {
                LockSupport.parkUntil(target_timeout);
            } 

            if (Thread.currentThread().isInterrupted()) {
                log.debug("%s: VERIFY_SUSPECT2 verification thread has beeing stoped due to shutdown", local_addr);
                synchronized(VERIFY_SUSPECT2.this) {
                    suspectsThread = null;
                    break;
                }
            }

            suspected_mbrs.clear();
            synchronized(this) {
                long currentMillis = System.currentTimeMillis();
                suspects.stream().filter(e -> e.target_time <= currentMillis).forEach(suspected_mbrs::add);
                suspects.removeAll(suspected_mbrs);
            }

            // send event up for suspect being found.
            if(!suspected_mbrs.isEmpty()) {
                List<Address> suspectedAddresses = suspected_mbrs.stream().map(Entry::suspect).toList();
                log.debug("%s: sending up SUSPECT(%s)", local_addr, suspected_mbrs);
                up_prot.up(new Event(Event.SUSPECT, suspectedAddresses));
            }

        }
        return null;
    }


    /* --------------------------------- Private Methods ----------------------------------- */

    protected Object handle(VerifyHeader hdr) {
        switch(hdr.type) {
            case VerifyHeader.ARE_YOU_DEAD:
                if(hdr.from == null) {
                    log.error(Util.getMessage("AREYOUDEADHdrFromIsNull"));
                    return null;
                }
                Address target=use_mcast_rsps? null : hdr.from;
                for(int i=0; i < num_msgs; i++) {
                    Message rsp=new EmptyMessage(target).setFlag(Message.TransientFlag.DONT_BLOCK)
                      .putHeader(this.id, new VerifyHeader(VerifyHeader.I_AM_NOT_DEAD, local_addr));
                    down_prot.down(rsp);
                }
                return null;
            case VerifyHeader.I_AM_NOT_DEAD:
                if(hdr.from == null) {
                    log.error(Util.getMessage("IAMNOTDEADHdrFromIsNull"));
                    return null;
                }
                unsuspect(hdr.from);
                return null;
        }
        return null;
    }


    /**
     * Sends ARE_YOU_DEAD message to suspected_mbr, wait for return or timeout
     */
    protected void verifySuspect(Collection<Address> mbrs) {
        if(mbrs == null || mbrs.isEmpty())
            return;
        addSuspects(mbrs);

        for(Address mbr: mbrs) {
            for(int i=0; i < num_msgs; i++) {
                Message msg=new EmptyMessage(mbr).setFlag(Message.TransientFlag.DONT_BLOCK)
                  .putHeader(this.id, new VerifyHeader(VerifyHeader.ARE_YOU_DEAD, local_addr));
                down_prot.down(msg);
            }
        }
    }


    /**
     * Adds suspected members to the suspect list. Returns true if a member is not present and the timer is not running.
     * @param list The list of suspected members
     * @return true if the timer needs to be started, or false otherwise
     */
    protected synchronized void addSuspects(Collection<Address> list) {
        if(list == null || list.isEmpty())
            return;

        long target_time=System.currentTimeMillis() + timeout;
        List<Entry> tmp=list.stream().map(a -> new Entry(a, target_time)).toList();
        suspects.addAll(tmp);
        log.debug("verifying that %s %s dead", suspects, suspects.size() == 1? "is" : "are");
    }

    protected synchronized boolean removeSuspect(Address suspect) {
        return suspects.removeIf(e -> Objects.equals(e.suspect, suspect));
    }


    public void unsuspect(Address mbr) {
        boolean removed=mbr != null && removeSuspect(mbr);
        if(removed) {
            log.trace("member %s was unsuspected", mbr);
            down_prot.down(new Event(Event.UNSUSPECT, mbr));
            up_prot.up(new Event(Event.UNSUSPECT, mbr));
        }
    }

    @GuardedBy("lock")
    protected void stopThreadPool() {
        if(thread_pool != null)
            thread_pool.shutdown();
    }

    public void init() throws Exception {
        super.init();
        ThreadFactory f=new DefaultThreadFactory(this.getClass().getSimpleName() + ".Runner", true, true);
        thread_pool=new ThreadPoolExecutor(0, 1, // max 1 thread, started each time a suspect is added
                                           0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), f);
    }

    @Override public synchronized void start() throws Exception {
        log.debug("%s: VERIFY_SUSPECT2 is being started", local_addr);
        running=true;
        if(suspectsThread == null) {
            suspectsThread = thread_pool.submit(this);
        }
        super.start();
    }

    @Override public synchronized void stop() {
        log.debug("%s: VERIFY_SUSPECT2 is being shutdown", local_addr);
        running = false;
        if (suspectsThread!=null) {
            suspectsThread.cancel(true);
            suspectsThread=null;
        }
        suspects.clear();
        super.stop();
    }

    public void destroy() {
        stopThreadPool();
        super.destroy();
    }

    /* ----------------------------- End of Private Methods -------------------------------- */

    protected record Entry(Address suspect, long target_time) implements Comparable<Entry> {

        public boolean equals(Object obj) {
            if(!(obj instanceof Entry other))
                return false;
            return Objects.equals(suspect, other.suspect);
        }

        public int hashCode() {
            return suspect.hashCode();
        }

        public String toString() {
            return String.format("%s (expires in %d ms)", suspect, expiry());
        }

        protected long expiry() {
            return target_time - System.currentTimeMillis();
        }

        public int compareTo(Entry o) {
            return suspect.compareTo(o.suspect);
        }
    }


    public static class VerifyHeader extends Header {
        static final short ARE_YOU_DEAD=1;  // 'from' is sender of verify msg
        static final short I_AM_NOT_DEAD=2;  // 'from' is suspected member

        short type=ARE_YOU_DEAD;
        Address from;     // member who wants to verify that suspected_mbr is dead


        public VerifyHeader() {
        } // used for externalization

        VerifyHeader(short type) {
            this.type=type;
        }

        VerifyHeader(short type, Address from) {
            this(type);
            this.from=from;
        }

        public short getMagicId() {return 94;}

        public Supplier<? extends Header> create() {return VerifyHeader::new;}

        public String toString() {
            return switch(type) {
                case ARE_YOU_DEAD  -> "[ARE_YOU_DEAD]";
                case I_AM_NOT_DEAD -> "[I_AM_NOT_DEAD]";
                default            -> "[unknown type (" + type + ")]";
            };
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeShort(type);
            Util.writeAddress(from, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            type=in.readShort();
            from=Util.readAddress(in);
        }

        @Override
        public int serializedSize() {
            return Global.SHORT_SIZE + Util.size(from);
        }
    }


}

