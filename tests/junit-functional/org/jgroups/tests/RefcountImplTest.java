package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.RefcountImpl;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link org.jgroups.util.RefcountImpl}
 * @author Bela Ban
 * @since  5.1.0
 */
@Test(groups=Global.FUNCTIONAL)
public class RefcountImplTest {

    public void testCreation() {
        RefcountImpl<Integer> ref=new RefcountImpl<>();
        assert ref.refCount() == 0;
    }

    public void testIncrAndDecr() {
        RefcountImpl<Void> ref=new RefcountImpl<>();
        ref.incr();
        ref.incr();
        assert ref.refCount() == 2;
        ref.decr(null);
        assert ref.refCount() == 1;
        ref.decr(null);
        ref.decr(null);
        assert ref.refCount() == 0;
    }

    public void testRelease() {
        AtomicInteger ai=new AtomicInteger(100);
        RefcountImpl<AtomicInteger> ref=new RefcountImpl<>(r -> r.set(0));
        ref.incr();
        ref.incr();
        assert ai.get() == 100;
        ref.decr(ai);
        assert ai.get() == 100;
        ref.decr(ai);
        assert ai.get() == 0;
        ai.set(50);
        ref.decr(ai);
        assert ai.get() == 50;
        assert ref.refCount() == 0;
    }
}
