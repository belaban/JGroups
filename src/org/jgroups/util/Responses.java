package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.protocols.PingData;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages responses for the discovery protocol. Moved from {@link org.jgroups.protocols.Discovery}
 * into this standalone class. Responses are only added but never removed.
 * @author Bela Ban
 * @since  3.5
 */
public class Responses implements Iterable<PingData> {
    protected PingData[]        ping_rsps;
    protected int               index;
    protected final Lock        lock=new ReentrantLock();
    protected final CondVar     cond=new CondVar(lock);
    protected final int         num_expected_rsps;
    protected final boolean     break_on_coord_rsp;
    @GuardedBy("lock")
    protected boolean           done=false; // successfully completed, or cancelled


    public Responses(boolean break_on_coord_rsp) {
        this(0, break_on_coord_rsp);
    }

    public Responses(int num_expected_rsps, boolean break_on_coord_rsp) {
        this(num_expected_rsps, break_on_coord_rsp, 16);
    }

    public Responses(int num_expected_rsps, boolean break_on_coord_rsp, int initial_capacity) {
        this.num_expected_rsps=num_expected_rsps;
        this.break_on_coord_rsp=break_on_coord_rsp;
        ping_rsps=new PingData[Math.max(5, initial_capacity)];
    }

    public boolean isDone() {
        lock.lock();
        try {return done;} finally {lock.unlock();}
    }

    public Responses done() {
        lock.lock();
        try {return _done();} finally {lock.unlock();}
    }

    public Responses clear() {
        lock.lock();
        try {
            index=0;
            return _done();
        }
        finally {
            lock.unlock();
        }
    }

    public void addResponse(PingData rsp, boolean overwrite) {
        if(rsp == null)
            return;

        boolean is_coord_rsp=rsp.isCoord(), changed=false;
        lock.lock();
        try {
            // https://jira.jboss.org/jira/browse/JGRP-1179
            int ind=find(rsp);
            if(ind == -1) { // new addition
                add(rsp);
                changed=true;
            }
            else {
                PingData existing=ping_rsps[ind]; // cannot be null
                if(overwrite || (is_coord_rsp && !existing.isCoord())) {
                    ping_rsps[ind]=rsp;
                    changed=true;
                }
            }
            if(changed && ((num_expected_rsps > 0 && index >= num_expected_rsps) || break_on_coord_rsp && is_coord_rsp))
                _done();
        }
        finally {
            lock.unlock();
        }
    }

    public boolean containsResponseFrom(Address mbr) {
        if(mbr == null) return false;
        for(int i=0; i < index; i++) {
            if(ping_rsps[i] != null && mbr.equals(ping_rsps[i].getAddress()))
                return true;
        }
        return false;
    }

    public boolean isCoord(Address addr) {
        PingData rsp=findResponseFrom(addr);
        return rsp != null && rsp.isCoord() && Objects.equals(rsp.getAddress(), addr);
    }

    public PingData findResponseFrom(Address mbr) {
        if(mbr == null) return null;
        for(int i=0; i < index; i++) {
            if(ping_rsps[i] != null && mbr.equals(ping_rsps[i].getAddress()))
                return ping_rsps[i];
        }
        return null;
    }


    public boolean waitFor(long timeout) {
        return cond.waitFor(this::isDone, timeout, TimeUnit.MILLISECONDS);
    }

    public Iterator<PingData> iterator() {
        lock.lock();
        try {
            return new PingDataIterator(ping_rsps, index);
        }
        finally {
            lock.unlock();
        }
    }

    public int     size()    {return index;}
    public boolean isEmpty() {return size() == 0;}

    public String toString() {
        int[] num=numResponses();
        return String.format("%d rsps (%d coords) [%s]", num[0], num[1], (isDone()? "done" : "pending"));
    }

    public String print() {
        StringBuilder sb=new StringBuilder();
        for(PingData data: this)
            sb.append(data).append("\n");
        return sb.toString();
    }

    @GuardedBy("lock") protected Responses _done() {
        if(!done) {
            done=true;
            cond.signal(true);
        }
        return this;
    }

    protected int[] numResponses() {
        lock.lock();
        try {
            int[] num={0,0};
            for(int i=0; i < index; i++) {
                PingData data=ping_rsps[i];
                num[0]++;
                if(data.isCoord())
                    num[1]++;
            }
            return num;
        }
        finally {
            lock.unlock();
        }
    }

    @GuardedBy("lock") protected List<PingData> toList() {
        return new ArrayList<>(Arrays.asList(ping_rsps).subList(0, index));
    }

    protected void resize(int new_size) {
        lock.lock();
        try {
            ping_rsps=Arrays.copyOf(ping_rsps, new_size);
        }
        finally {
            lock.unlock();
        }
    }

    @GuardedBy("lock") protected void add(PingData data) {
        if(index >= ping_rsps.length)
            resize(newLength(ping_rsps.length));
        ping_rsps[index++]=data;
    }


    @GuardedBy("lock") protected int find(PingData data) {
        if(data == null) return -1;
        for(int i=0; i < index; i++) {
            if(data.equals(ping_rsps[i]))
                return i;
        }
        return -1;
    }


    protected static int newLength(int length) {
        return length > 1000? (int)(length * 1.1) : Math.max(5, length * 2);
    }



    protected static class PingDataIterator implements Iterator<PingData> {
        protected final PingData[] data;
        protected final int        end_index;
        protected int              index;

        public PingDataIterator(PingData[] data, int end_index) {
            this.data=data;
            this.end_index=end_index;
            if(data == null)
                throw new IllegalArgumentException("data cannot be null");
            if(end_index > data.length)
                throw new IndexOutOfBoundsException("index is " + end_index + ", but arrays's length is only " + data.length);
        }

        public boolean hasNext() {
            return index < end_index && data[index] != null;
        }

        public PingData next() {
            if(index >= end_index || index >= data.length)
                throw new NoSuchElementException("index=" + index);
            return data[index++];
        }

        public void remove() {}
    }
}
