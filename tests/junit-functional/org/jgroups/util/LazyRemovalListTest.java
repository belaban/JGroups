package org.jgroups.util;

import org.jgroups.Global;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests {@link LazyRemovalList}
 * @author Bela Ban
 * @since  5.4.4
 */
@Test(groups=Global.FUNCTIONAL)
public class LazyRemovalListTest {

    public void testConstructor() {
        LazyRemovalList<Integer> l=new LazyRemovalList<>(10, 20);
        assert l.size() == 0;
    }

    public void testAddAndRemove() {
        LazyRemovalList<Integer> l=new LazyRemovalList<>(10, 20);
        for(int i=1; i <= 5; i++)
            l.add(i);
        assert l.size() == 5;
        for(int i=6; i <= 15; i++)
            l.add(i);
        Util.sleep(100);
        assert l.size() == 15;
        for(int i=1; i <= 5; i++)
            l.remove(i);
        assert l.size() == 15;
        Util.sleep(100);
        l.add(16);
        assert l.size() == 11;
    }

    public void testGet() {
        LazyRemovalList<Integer> l=new LazyRemovalList<>(10, 20);
        for(int i=1; i <= 15; i++)
            l.add(i);
        assert l.size() == 15;
        Integer v=l.get(5);
        assert v == 5;
        v=l.get(20);
        assert v == null;
    }

    public void testClear() {
        LazyRemovalList<Integer> l=new LazyRemovalList<>(10, 20);
        for(int i=1; i <= 15; i++)
            l.add(i);
        l.clear(false);
        assert l.size() == 15;
        Util.sleep(100);
        l.add(1);
        assert l.size() == 1;
        l.clear(true);
        assert l.size() == 0;
    }

    public void testNonRemovedValues() {
        LazyRemovalList<Integer> l=new LazyRemovalList<>(10, 20);
        for(int i=1; i <= 15; i++)
            l.add(i);
        List<Integer> nrv=l.nonRemovedValues();
        assert nrv.size() == 15;
        for (int i=11; i <= 15; i++)
            l.remove(i);
        nrv=l.nonRemovedValues();
        assert nrv.size() == 10;
    }

    public void testForEach() {
        LazyRemovalList<Integer> l=new LazyRemovalList<>(10, 20);
        for(int i=1; i <= 15; i++)
            l.add(i);
        List<Integer> list=new ArrayList<>(15);
        l.forEach(list::add);
        assert list.size() == 15;
        List<Integer> expected=IntStream.rangeClosed(1, 15).boxed().collect(Collectors.toList());
        assert list.equals(expected);

        for(int i: List.of(1,2,3,4,5))
            l.remove(i);
        list.clear();
        l.forEach(list::add);
        assert list.size() == 10;
        assert list.equals(IntStream.rangeClosed(6,15).boxed().collect(Collectors.toList()));
    }
}
