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
        assert ref.getRefcount() == 0;
    }

    public void testIncrAndDecr() {
        RefcountImpl<Integer> ref=new RefcountImpl<>();
        ref.incr().incr();
        assert ref.getRefcount() == 2;
        ref.decr(22);
        assert ref.getRefcount() == 1;
        ref.decr(11).decr(11);
        assert ref.getRefcount() == 0;
    }

    public void testRelease() {
        AtomicInteger ai=new AtomicInteger(100);
        RefcountImpl<AtomicInteger> ref=new RefcountImpl<>(r -> r.set(0));
        ref.incr().incr();
        assert ai.get() == 100;
        ref.decr(ai);
        assert ai.get() == 100;
        ref.decr(ai);
        assert ai.get() == 0;
        ai.set(50);
        ref.decr(ai);
        assert ai.get() == 50;
        assert ref.getRefcount() == 0;
    }
}
