package org.jgroups;

import org.jgroups.annotations.Experimental;
import org.jgroups.util.RefcountImpl;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Ref-counted message implementation.<br/>
 * Note that this class is experimental and may get removed without notice. The point of it is to get experience with
 * ref counted messages and see if they're needed or not.<br/>
 * See https://issues.redhat.com/browse/JGRP-2942 for details
 * @author Bela Ban
 * @since  5.5.1
 */
@Experimental
public class RefcountedNioMessage extends NioMessage implements Refcountable<Message> {
    protected final RefcountImpl<Message> impl=new RefcountImpl<>();

    public RefcountedNioMessage() {
    }

    public RefcountedNioMessage(Address dest) {
        super(dest);
    }

    public RefcountedNioMessage(Address dest, ByteBuffer buf) {
        super(dest, buf);
    }

    @Override
    public int refCount() {
        return impl.refCount();
    }

    @Override public int incr() {
        return impl.incr();
    }

    @Override public int decr() {
        return impl.decr(this);
    }

    public RefcountedNioMessage onRelease(Consumer<Message> c) {
        impl.onRelease(c);
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s (refcnt=%d)", super.toString(), impl.refCount());
    }

}
