package org.jgroups;

import org.jgroups.annotations.Experimental;
import org.jgroups.util.ByteArray;
import org.jgroups.util.RefcountImpl;

import java.util.function.Consumer;

/**
 * Ref-counted message implementation.<br/>
 * Note that this class is experimental and may get removed without notice. The point of it is to get experience with
 * ref counted messages and see if they're needed or not.<br/>
 * See https://issues.redhat.com/browse/JGRP-2417 for details
 * @author Bela Ban
 * @since  5.1.0
 */
@Experimental
public class RefcountedBytesMessage extends BytesMessage implements Refcountable<Message> {
    protected final RefcountImpl<Message> impl=new RefcountImpl<>();

    public RefcountedBytesMessage() {
    }

    public RefcountedBytesMessage(Address dest) {
        super(dest);
    }

    public RefcountedBytesMessage(Address dest, byte[] array) {
        super(dest, array);
    }

    public RefcountedBytesMessage(Address dest, byte[] array, int offset, int length) {
        super(dest, array, offset, length);
    }

    public RefcountedBytesMessage(Address dest, ByteArray array) {
        super(dest, array);
    }

    public RefcountedBytesMessage(Address dest, Object obj) {
        super(dest, obj);
    }

    public synchronized byte getRefcount() {
        return impl.getRefcount();
    }

    @Override public synchronized RefcountedBytesMessage incr() {
        impl.incr();
        return this;
    }

    @Override public synchronized RefcountedBytesMessage decr() {
        impl.decr(this);
        return this;
    }

    public RefcountedBytesMessage onRelease(Consumer<Message> rc) {
        impl.onRelease(rc);
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s (refcnt=%d)", super.toString(), impl.getRefcount());
    }

}
