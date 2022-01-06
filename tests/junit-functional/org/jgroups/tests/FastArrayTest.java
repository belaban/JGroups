package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.FastArray;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests {@link org.jgroups.util.FastArray}
 * @author Bela Ban
 * @since  5.2
 */
@Test(groups=Global.FUNCTIONAL)
public class FastArrayTest {

    public void testCreation() {
        FastArray<Integer> fa=new FastArray<>(1);
        assert fa.isEmpty();
        assert fa.capacity() == 1;
    }

    public void testAdd() {
        FastArray<Integer> fa=new FastArray<>(5);
        int added=fa.add(1);
        assert added == 1;
        assert fa.capacity() == 5;
        assert fa.size() == 1;
        assert !fa.isEmpty();

        added=fa.add(2, 3);
        assert added == 2;
        assert fa.capacity() == 5;
        assert fa.size() == 3;
        assert !fa.isEmpty();
    }

    public void testAddNoResize() {
        FastArray<Integer> fa=new FastArray<>(5);
        List<Integer> list=Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        for(int i: list)
            fa.add(i, false);
        assert fa.size() == 5;
        assert fa.capacity() == 5;
    }

    public void testAddList() {
        FastArray<Integer> fa=create(3);
        List<Integer> list=Arrays.asList(3, 4, 5, 6, 7, 8, 9);
        assert fa.size() == 3;
        int added=fa.add(list);
        assert added == list.size();
        assert fa.size() == 10;
        assert fa.capacity() == 10 + fa.increment();

        fa=new FastArray<>(10);
        added=fa.add(Arrays.asList(0,1,2));
        assert added == 3;
        added=fa.add(list);
        assert added == list.size();
        assert fa.size() == 10;
        assert fa.capacity() == 10;

        added=fa.add(11);
        assert added == 1;
        assert fa.size() == 11;
        assert fa.capacity() == 10 + fa.increment();

        list=new ArrayList<>();
        added=fa.add(list);
        assert added == 0;
        assert fa.size() == 11;
        assert fa.capacity() == 10 + fa.increment();
    }

    public void testAddFastArray() {
        FastArray<Integer> fa=create(10);
        FastArray<Integer> fa2=new FastArray<>(5);
        int added=fa2.add(Arrays.asList(10,11,12,13,14));
        assert added == 5;
        added=fa.add(fa2);
        assert added == fa2.size();
        assert fa.size() == 15;
        assert fa.capacity() == fa.size() + fa.increment();

        fa=new FastArray<>(15);
        added=fa.add(Arrays.asList(0,1,2,3,4,5,6,7,8,9));
        assert added == 10;
        fa2=new FastArray<>(10);
        added=fa2.add(Arrays.asList(10,11,12,13,14,15,16,17,18,19));
        assert added == 10;
        added=fa.add(fa2, false);
        assert added == 5;
        assert fa.size() == 15;
        assert fa.capacity() == fa.size();

        fa=new FastArray<>(20);
        added=fa.add(Arrays.asList(0,1,2,3,4,5,6,7,8,9));
        assert added == 10;
        fa2=new FastArray<>(10);
        added=fa2.add(Arrays.asList(10,11,12,13,14,15,16,17,18,19));
        assert added == 10;
        added=fa.add(fa2, false);
        assert added == 10;
        assert fa.size() == 20;
        assert fa.capacity() == fa.size();
    }

    public void testAddArray() {
        FastArray<Integer> fa=new FastArray<>(5);
        Integer[] arr=createArray(5);
        int added=fa.add(arr, 10);
        assert added == arr.length;
        assert fa.size() == 5;
        assert fa.index() == 5;

        fa=new FastArray<>(5);
        arr=createArray(10);
        added=fa.add(arr, 8);
        assert added == 8;
        assert fa.size() == 8;
        assert fa.index() == 8;

        // add sparse array
        fa=new FastArray<>(5);
        arr[0]=arr[3]=arr[9]=null;
        added=fa.add(arr, arr.length);
        assert added == 7;
        assert fa.size() == 7;
        assert fa.index() == 10;
    }

    public void testTransfer() {
        FastArray<Integer> other=new FastArray<>(3);
        List<Integer> l=Arrays.asList(0,1,2,3,4,5,6,7,8,9);
        other.add(l);
        int other_size=other.size();

        FastArray<Integer> fa=new FastArray<>(5);
        int num=fa.transferFrom(other, true);
        assert num == other_size;
        assert fa.size() == other_size;
        assert other.isEmpty();
    }

    public void testTransfer2() {
        FastArray<Integer> other=new FastArray<>(10);
        List<Integer> l=Arrays.asList(0,1,2,3,4,5,6,7,8,9);
        other.add(l);
        other.remove(0).remove(3).remove(4).remove(9);
        int other_size=other.size();

        FastArray<Integer> fa=new FastArray<>(5);
        int num=fa.transferFrom(other, false);
        assert num == other_size;
        assert fa.size() == other_size;
        assert !other.isEmpty();
        assertSameElements(fa, other);

        fa=new FastArray<>(15);
        num=fa.transferFrom(other, false);
        assert num == other_size;
        assert fa.size() == other_size;
        assert !other.isEmpty();
        assertSameElements(fa, other);

        fa=create(15);
        num=fa.transferFrom(other, false);
        assert num == other_size;
        assert fa.size() == other_size;
        assert !other.isEmpty();
        assertSameElements(fa, other);
    }

    public void testTransfer3() {
        FastArray<Integer> other=new FastArray<>(3);
        List<Integer> l=Arrays.asList(0,1,2,3,4,5,6,7,8,9);
        other.add(l);
           int other_size=other.size();

           FastArray<Integer> fa=new FastArray<>(5);
           l.forEach(fa::add);
           l.forEach(fa::add);
           System.out.println("fa = " + fa);

           int num=fa.transferFrom(other, true);
           assert num == other_size;
           assert fa.size() == other_size;
           assert other.isEmpty();
       }

       public void testTransfer4() {
           FastArray<Integer> other=new FastArray<>(30);
           FastArray<Integer> fa=new FastArray<>(10);
           int num=fa.transferFrom(other, true);
           assert num == 0;
           assert fa.capacity() == 10;
       }

    public void testSet() {
        FastArray<Integer> fa=create(10);
        Integer[] arr={1,3,5,7,9,10};
        fa.set(arr);
        assert fa.size() == 6;
        fa.add(11);
        assert fa.size() == 7;

        Integer el=fa.get(2);
        assert el == 5;
        assert fa.size() == 7;
        fa.set(2, null);
        assert fa.size() == 6;
        assert fa.get(2) == null;
        fa.set(2, null);
        assert fa.size() == 6;
        assert fa.get(2) == null;
        fa.set(2, 5);
        assert fa.size() == 7;
        assert fa.get(2) == 5;
        fa.set(2, 5);
        assert fa.size() == 7;
        assert fa.get(2) == 5;
    }

    public void testSet2() {
        FastArray<Integer> fa=create(10);
        fa.set(9, 90).set(8,80);
        assert fa.size() == 10;
        assert fa.get(9) == 90;
        assert fa.get(8) == 80;
    }

    public void testSet3() {
        FastArray<Integer> fa=create(10);
        for(int i=0; i <= 8; i++) {
            if(i != 5)
                fa.remove(i);
        }
        assert fa.size() == 2;
        fa.set(9, null);
        assert fa.size() == 1;
        fa.set(9, 90);
        assert fa.size() == 2;
    }

    public void testRemove() {
        FastArray<Integer> fa=create(10);
        fa.remove(5).remove(0).remove(9);
        assert fa.size() == 7;
        assert fa.count() == 7;
    }

    public void testRemove2() {
        FastArray<Integer> fa=new FastArray<>(10);
        fa.remove(9);
        assert fa.isEmpty();
    }

    public void testReplaceIf() {
        FastArray<Integer> fa=create(10);
        fa.replaceIf(null, 1, true);
        assert fa.size() == 10;
        int i=0;
        for(Integer n: fa)
            assert n == i++;
    }

    /** Replaces all elements */
    public void testReplaceIfReplaceAll() {
        FastArray<Integer> fa=create(10);
        fa.replaceIf(el -> true, 1, true);
        assert fa.size() == 10;
        for(Integer n: fa)
            assert n == 1;
    }

    /** Replaces all elements */
    public void testReplaceIfReplaceNone() {
        FastArray<Integer> fa=create(10);
        fa.replaceIf(el -> false, 1, true);
        assert fa.size() == 10;
        int i=0;
        for(Integer n: fa)
            assert n == i++;
    }

    public void testReplaceIfRemoveEvenNumbers() {
        FastArray<Integer> fa=create(10);
        fa.replaceIf(el -> el %2 == 0, null, true);
        assert fa.size() == 5;
        for(Integer n: fa)
            assert n % 2 != 0;
    }

    public void testReplaceIfRemoveEvenNumbers2() {
        FastArray<Integer> fa=create(10).remove(0).remove(1).remove(5).remove(6).remove(7).remove(8).remove(9);
        assert fa.size() == 3;
        fa.replaceIf(el -> el %2 == 0, null, true);
        assert fa.size() == 1;
        for(Integer n: fa)
            assert n % 2 != 0;
    }

    public void testGet() {
        FastArray<Integer> fa=create(10);
        fa.remove(5).remove(0).remove(9);
        Integer num=fa.get(3);
        assert num == 3;

        num=fa.get(9);
        assert num == null;
    }

    public void testResize() {
        FastArray<Integer> fa=create(2);
        int old_cap=fa.capacity();
        assert fa.capacity() == old_cap;
        fa.add(3);
        assert fa.capacity() == old_cap + fa.increment();
    }

    public void testSimpleIteration() {
        FastArray<Integer> fa=create(10);
        List<Integer> l=new ArrayList<>(10);
        for(Iterator<Integer> it=fa.iterator(); it.hasNext();) {
            Integer el=it.next();
            l.add(el);
        }
        assert l.size() == fa.size();
        List<Integer> l2=fa.stream().collect(Collectors.toList());
        assert l.equals(l2);
    }

    public void testIteration() {
        FastArray<Integer> fa=create(10);
        int i=0;
        for(Iterator<Integer> it=fa.iterator(); it.hasNext();) {
            Integer el=it.next();
            assert el == i++;
        }
    }

    public void testIteration2() {
        FastArray<Integer> fa=create(10);
        for(Iterator<Integer> it=fa.iterator(); it.hasNext();) {
            Integer el=it.next();
            if(el % 2 == 0)
                it.remove();
        }
        assert fa.size() == 5;
    }

    public void testIteration3() {
        FastArray<Integer> fa=create(10);
        for(Iterator<Integer> it=fa.iterator(); it.hasNext();) {
            it.next();
            it.remove();
        }
        assert fa.isEmpty();
    }

    public void testIteration4() {
        FastArray<Integer> fa=create(10);
        for(int i=0; i < 4; i++)
            fa.remove(i);
        assert fa.size() == 6;
        for(int i=6; i < 10; i++)
            fa.remove(i);
        assert fa.size() == 2;

        // iterator should stop after encountering 5
        for(Iterator<Integer> it=fa.iterator(); it.hasNext();) {
            Integer el=it.next();
            assert el == 4;
            el=it.next();
            assert el == 5;
        }
    }

    public void testIterationWithFilter() {
        FastArray<Integer> fa=create(10);
        List<Integer> l=new ArrayList<>(5);
        for(Iterator<Integer> it=fa.iteratorWithFilter(i -> i %2 == 0); it.hasNext();) {
            Integer el=it.next();
            l.add(el);
        }
        assert l.size() == 5;
        for(int i: l)
            assert i % 2 == 0;
    }

    /** Not really supported, but should nevertheless work... */
    public void testIterationWithConcurrentAddition() {
        FastArray<Integer> fa=create(10);
        List<Integer> l=new ArrayList<>();
        for(Iterator<Integer> it=fa.iterator(); it.hasNext();) {
            Integer el=it.next();
            l.add(el);
            if(el == 8)
                fa.add(10,11);
        }
        assert  l.size() == 12;
        List<Integer> l2=IntStream.rangeClosed(0, 11).boxed().collect(Collectors.toList());
        assert l.equals(l2);
    }

    /** Iteration stops when N non-null elements have been reached where N == size  */
    public void testStopIterationWhenSizeReached() {
        FastArray<Integer> fa=create(10);
        IntStream.rangeClosed(0,9).filter(i -> i != 4 && i != 5).forEach(fa::remove);
        assert fa.size() == 2;
        int count=0;
        FastArray<Integer>.FastIterator it=fa.iterator();
        while(it.hasNext()) {
            Integer el=it.next();
            System.out.println("el = " + el);
            count++;
        }
        assert count == 2;
        assert it.currentIndex() == 5;
        assert it.hitCount() == 2;
    }

    public void testStopIterationWhenSizeReachedUsingFilter() {
        FastArray<Integer> fa=create(10);
        IntStream.rangeClosed(0,9).filter(i -> i != 3 && i != 4 && i != 5).forEach(fa::remove);
        assert fa.size() == 3;
        int count=0;
        FastArray<Integer>.FastIterator it=fa.iteratorWithFilter(el -> el < 3 || el > 5);
        while(it.hasNext()) {
            Integer el=it.next();
            System.out.println("el = " + el);
            count++;
        }
        assert count == 0;
        assert it.currentIndex() == 5;
        assert it.hitCount() == 3;

        fa=create(10);
        IntStream.rangeClosed(0,9).filter(i -> i != 3 && i != 4 && i != 5).forEach(fa::remove);
        assert fa.size() == 3;
        count=0;
        it=fa.iteratorWithFilter(el -> el != 4);
        while(it.hasNext()) {
            Integer el=it.next();
            System.out.println("el = " + el);
            count++;
        }
        assert count == 2;
        assert it.currentIndex() == 5;
        assert it.hitCount() == 3;
    }

    public void testStream() {
        FastArray<Integer> fa=create(10);
        testStream(fa);
    }

    public void testStream2() {
        FastArray<Integer> fa=create(10);
        testStream2(fa);
    }

    public void testClear() {
        FastArray<Integer> fa=create(10);
        assert fa.size() == 10;
        fa.remove(0).remove(1).remove(9).remove(10);
        assert fa.size() == 7;
        fa.clear(true);
        assert fa.isEmpty();
    }

    public void testAnyMatch() {
        FastArray<Integer> fa=create(10);
        boolean match=fa.anyMatch(num -> num > 3);
        assert match;
    }

    protected static void assertSameElements(FastArray<Integer> fa, FastArray<Integer> fa2) {
        assert fa.size() == fa2.size();
        List<Integer> l=fa.stream().collect(Collectors.toList()), l2=fa2.stream().collect(Collectors.toList());
        assert l.equals(l2);
    }


    protected static void testStream(FastArray<Integer> fa) {
        List<Integer> list=fa.stream().collect(Collectors.toList());
        System.out.println("list = " + list);
        for(int i=0; i < fa.size(); i++)
            assert i == list.get(i);
    }

    protected static void testStream2(FastArray<Integer> fa) {
        int i=0;
        for(Integer num: fa) {
            assert num == i++;
        }
    }


    protected static FastArray<Integer> create(int num) {
        FastArray<Integer> fa=new FastArray<>(num);
        for(int i=0; i < num; i++)
            fa.add(i);
        return fa;
    }

    protected static Integer[] createArray(int num) {
        return IntStream.range(0, num).boxed().toArray(Integer[]::new);
    }
}
