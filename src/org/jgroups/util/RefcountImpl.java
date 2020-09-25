package org.jgroups.util;

import org.jgroups.annotations.Experimental;

import java.util.function.Consumer;

/**
 * Ref-counted implementation; can be used by message implementations.<br/>
 * Note that this class is experimental and may get removed without notice. The point of it is to get experience with
 * ref counted messages and see if they're needed or not.<br/>
 * See https://issues.redhat.com/browse/JGRP-2417 for details
 * @author Bela Ban
 * @since  5.1.0
 */
@Experimental
public class RefcountImpl<T> {
    protected byte        refcount;
    protected Consumer<T> release_code;

    public RefcountImpl() {
    }

    public RefcountImpl(Consumer<T> c) {
        this.release_code=c;
    }

    public synchronized byte getRefcount() {
        return refcount;
    }

    public synchronized RefcountImpl<T> incr() {
        refcount++;
        return this;
    }

    public synchronized RefcountImpl<T> decr(T t) {
        byte tmp=--refcount;
        if(tmp == 0)
            release(t);
        else if(tmp < 0)
            refcount=0;
        return this;
    }

    public RefcountImpl<T> onRelease(Consumer<T> rc) {
        release_code=rc;
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s (refcnt=%d)", super.toString(), refcount);
    }


    protected void release(T t) {
        if(release_code != null)
            release_code.accept(t);
    }
}
