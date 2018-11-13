package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.MatchingPromise;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  4.0.16
 */
@Test(groups=Global.FUNCTIONAL)
public class MatchingPromiseTest {

    public void testSetResult() {
        MatchingPromise<Integer> mp=new MatchingPromise<>(22);

        mp.setResult(25);
        System.out.println("mp: " + mp);
        assert !mp.hasResult();

        mp.setResult(22);
        System.out.println("mp: " + mp);
        assert mp.hasResult();
        assert mp.getResult() == 22;
    }

    public void testSetResultWithNullExpectedValue() {
        MatchingPromise<Object> mp=new MatchingPromise<>(null);

        mp.setResult(25);
        System.out.println("mp: " + mp);
        assert !mp.hasResult();

        mp.setResult(null);
        System.out.println("mp: " + mp);
        assert mp.hasResult();
        assert mp.getResult() == null;
    }

    public void testReset() {
        MatchingPromise<Object> mp=new MatchingPromise<>("Bela");
        mp.setResult("Michi");
        assert !mp.hasResult();
        mp.setResult("Bela");
        assert mp.hasResult();
        assert "Bela".equals(mp.getResult());

        mp.reset("Michi");
        assert !mp.hasResult();
        mp.setResult("Bela");
        assert !mp.hasResult();
        assert null == mp.getResult(100);
        mp.setResult("Michi");
        assert mp.hasResult() && "Michi".equals(mp.getResult(300));
    }

    public void testResetToNull() {
        MatchingPromise<Integer> mp=new MatchingPromise<>(5);
        assert mp.getExpectedResult() == 5;
        mp.reset(10);
        assert mp.getExpectedResult() == 10;

        mp.reset();
        assert mp.getExpectedResult() == 10;
        assert mp.getResult(1) == null;

        mp.reset(true);
        assert mp.getExpectedResult() == 10;
        assert mp.getResult(1) == null;
    }

    public void testNullExpectedValue() {
        MatchingPromise mp=new MatchingPromise(null);
        mp.setResult("hello");
        assert !mp.hasResult();
        assert mp.getResult(100) == null;
        mp.setResult(null);
        assert mp.hasResult() && null == mp.getResult(1);
    }
}
