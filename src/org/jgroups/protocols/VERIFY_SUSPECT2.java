
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
import java.util.stream.Collectors;


/**
 * Double-checks that a suspected member is really dead.<br/>
 * Details: https://issues.redhat.com/browse/JGRP-2558<br/>
 * Design: https://github.com/belaban/JGroups/blob/master/doc/design/VERIFY_SUSPECT2.txt
 * @author Bela Ban
 */
@MBean(description="Double-checks suspicions reports")
public class VERIFY_SUSPECT2 extends Protocol implements Runnable {

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
    public void run() {
        List<Address> suspected_mbrs=new ArrayList<>();
        while(running) {
            long target_time=System.currentTimeMillis() + timeout;
            do {
                LockSupport.parkUntil(target_time);
            }
            while(System.currentTimeMillis() < target_time);

            suspected_mbrs.clear();
            synchronized(this) {
                suspects.forEach(e -> {
                    if(e.target_time <= target_time)
                        suspected_mbrs.add(e.suspect);
                });
                suspects.removeIf(e -> e.target_time <= target_time);
            }

            if(!suspected_mbrs.isEmpty()) {
                log.debug("%s: sending up SUSPECT(%s)", local_addr, suspected_mbrs);
                up_prot.up(new Event(Event.SUSPECT, suspected_mbrs));
            }
            else {
                synchronized(this) {
                    if(suspects.isEmpty()) {
                        running=false;
                        break;
                    }
                }
            }
        }
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
                    Message rsp=new EmptyMessage(target)
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
        if(addSuspects(mbrs)) {
            startTask(); // start timer before we send out are you dead messages
            log.trace("verifying that %s %s dead", mbrs, mbrs.size() == 1? "is" : "are");
        }
        for(Address mbr: mbrs) {
            for(int i=0; i < num_msgs; i++) {
                Message msg=new EmptyMessage(mbr)
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
    protected synchronized boolean addSuspects(Collection<Address> list) {
        if(list == null || list.isEmpty())
            return false;

        long target_time=getCurrentTimeMillis();
        List<Entry> tmp=list.stream().map(a -> new Entry(a, target_time)).collect(Collectors.toList());
        return suspects.addAll(tmp);
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


    protected synchronized void startTask() {
        if(!running) {
            running=true;
            thread_pool.execute(this);
        }
    }

    @GuardedBy("lock")
    protected void stopThreadPool() {
        if(thread_pool != null)
            thread_pool.shutdown();
    }

    public void init() throws Exception {
        super.init();
        ThreadFactory f=new DefaultThreadFactory("VERIFY_SUSPECT.Runner", true, true);
        thread_pool=new ThreadPoolExecutor(0, 1, // max 1 thread, started each time a suspect is added
                                           0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), f);
    }


    public synchronized void stop() {
        suspects.clear();
    }

    public void destroy() {
        stopThreadPool();
        synchronized(this) {
            running=false;
        }
        super.destroy();
    }

    private static long getCurrentTimeMillis() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }
    /* ----------------------------- End of Private Methods -------------------------------- */

    protected static class Entry implements Comparable<Entry> {
        protected final Address suspect;
        protected final long    target_time; // millis

        public Entry(Address suspect, long target_time) {
            this.suspect=suspect;
            this.target_time=target_time;
        }

        public boolean equals(Object obj) {
            if(!(obj instanceof Entry))
                return false;
            Entry other=(Entry)obj;
            return Objects.equals(suspect, other.suspect);
        }

        public int hashCode() {
            return suspect.hashCode();
        }

        public String toString() {
            return String.format("%s (expires in %d ms)", suspect, expiry());
        }

        protected long expiry() {
            return target_time - getCurrentTimeMillis();
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
            switch(type) {
                case ARE_YOU_DEAD:
                    return "[ARE_YOU_DEAD]";
                case I_AM_NOT_DEAD:
                    return "[I_AM_NOT_DEAD]";
                default:
                    return "[unknown type (" + type + ")]";
            }
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

