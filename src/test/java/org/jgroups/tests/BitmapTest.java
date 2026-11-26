package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.protocols.FD_ALL3;
import org.testng.annotations.Test;

/**
 * Tests {@link org.jgroups.protocols.FD_ALL3.Bitmap}
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class BitmapTest {

    public void testIndex() {
        FD_ALL3.Bitmap bm=new FD_ALL3.Bitmap(5);
        assert bm.getIndex() == 0;
        bm.advance();
        assert bm.getIndex() == 1;
    }

    public void testNeedsToSuspect() {
        FD_ALL3.Bitmap bm=new FD_ALL3.Bitmap(5);
        assert !bm.needsToSuspect();
        for(int i=0; i < 4; i++) {
            bm.advance();
            assert !bm.needsToSuspect();
        }
        assert !bm.needsToSuspect();
        bm.advance();
        assert bm.needsToSuspect();
        bm.set();
        assert !bm.needsToSuspect();
    }
}
