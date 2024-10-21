package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.FastArray;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests {@link org.jgroups.util.FastArray}
 * @author Bela Ban
 * @since  5.2
 */
@SuppressWarnings({"SimplifyStreamApiCallChains", "SuspiciousListRemoveInLoop", "UseBulkOperation", "Java8CollectionRemoveIf"})
@Test(groups=Global.FUNCTIONAL)
public class FastArrayTest {

    public void testCreation() {
        FastArray<Integer> fa=new FastArray<>(1);
        assert fa.isEmpty();
        assert fa.capacity() == 1;
    }

    public void testCreationWithArray() {
        Integer[] array={1,2,3,4,5};
        FastArray<Integer> fa=new FastArray<>(array, array.length);
        assert fa.size() == 5;
        //noinspection SimplifyStreamApiCallChains
        List<Integer> list=fa.stream().collect(Collectors.toList());
        List<Integer> expected=IntStream.rangeClosed(1,5).boxed().collect(Collectors.toList());
        assert list.equals(expected);
    }

    public void testCreationWithArray2() {
        Integer[] array={1,2,3,null,5};
        FastArray<Integer> fa=new FastArray<>(array, array.length);
        assert fa.size() == 4;
        //noinspection SimplifyStreamApiCallChains
        List<Integer> list=fa.stream().collect(Collectors.toList());
        List<Integer> expected=IntStream.rangeClosed(1,5).boxed().collect(Collectors.toList());
        expected.remove((Integer)4);
        assert list.equals(expected);
    }

    public void testAdd() {
        FastArray<Integer> fa=new FastArray<>(5);
        boolean added=fa.add(1);
        assert added;
        assert fa.capacity() == 5;
        assert fa.size() == 1;
        assert !fa.isEmpty();

        added=fa.addAll(2, 3);
        assert added;
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

    /** Tests {@link FastArray#add(int, Object)} */
    public void testAddWithIndex() {
        List<Integer> fa=new FastArray<>(16);
        try {
            fa.add(5, 5);
            assert false : "add at index 5 should have thrown an exception";
        }
        catch(IndexOutOfBoundsException ex) {
            System.out.printf("caught exception as expected: %s\n", ex);
        }
        fa.add(0, 1);
        assert fa.size() == 1 && fa.get(0) == 1;
        fa.add(0, 2);
        fa.add(0,3);
        assert fa.size() == 3;
        assert Arrays.equals(fa.toArray(new Integer[0]), new Integer[]{3,2,1});
        fa.add(3,4);
        assert fa.size() == 4;
        assert Arrays.equals(fa.toArray(new Integer[0]), new Integer[]{3,2,1,4});
    }

    public void testAddWithIndex2() {
        List<Integer> fa=create(10);
        fa.add(5, fa.get(5) * 2);
        assertSameElements(fa, List.of(0,1,2,3,4,10,5,6,7,8,9));
        fa.add(6, null);
        List<Integer> expected=new ArrayList<>(List.of(0, 1, 2, 3, 4, 10));
        expected.add(null); expected.addAll(List.of(5,6,7,8,9));
        assert fa.size() == 11;
        Integer[] actual_arr=fa.toArray(new Integer[0]), expected_arr=expected.toArray(new Integer[0]);
        assert Arrays.equals(actual_arr, expected_arr)
          : String.format("expected: %s, actual: %s", Arrays.toString(expected_arr), Arrays.toString(actual_arr));
    }

    public void testAddAllWithIndex() {
        List<Integer> fa=new FastArray<>(10);
        List<Integer> list=new ArrayList<>(List.of(0, 1, 2, 3, 4));
        try {
            fa.addAll(5, list);
            assert false : "add at index 5 should have thrown an exception";
        }
        catch(IndexOutOfBoundsException ex) {
            System.out.printf("caught exception as expected: %s\n", ex);
        }
        assert !fa.addAll(0, List.of());
        assert fa.isEmpty();
        assert !fa.addAll(0, null);
        assert fa.isEmpty();


        assert fa.addAll(0, list);
        assert fa.size() == list.size();
        assertSameElements(fa, list);

        int old_size=fa.size();
        list.addAll(List.of(5,6,7,8,9));
        assert fa.addAll(0, list);
        assert fa.size() == list.size()+old_size;

        List<Integer> l=new ArrayList<>(List.of(0,1,2,3,4));
        assert l.addAll(2, List.of(10,11,12));
        fa=create(5);
        assert fa.addAll(2, List.of(10,11,12));
        assert fa.size() == 8;
        assertSameElements(fa, l);
    }

    public void testAddList() {
        FastArray<Integer> fa=create(3).increment(5);
        List<Integer> list=Arrays.asList(3, 4, 5, 6, 7, 8, 9);
        assert fa.size() == 3;
        boolean added=fa.addAll(list);
        assert added;
        assert fa.size() == 10;
        assert fa.capacity() == 10 + fa.increment();

        fa=new FastArray<Integer>(10).increment(2);
        added=fa.addAll(Arrays.asList(0, 1, 2));
        assert added;
        added=fa.addAll(list);
        assert added;
        assert fa.size() == 10;
        assert fa.capacity() == 10;

        added=fa.add(11);
        assert added;
        assert fa.size() == 11;
        assert fa.capacity() == 11 + fa.increment();

        list=new ArrayList<>();
        added=fa.addAll(list);
        assert !added;
        assert fa.size() == 11;
        assert fa.capacity() == 11 + fa.increment();
    }

    public void testAddFastArray() {
        FastArray<Integer> fa=create(10);
        FastArray<Integer> fa2=new FastArray<>(5);
        boolean added=fa2.addAll(Arrays.asList(10, 11, 12, 13, 14));
        assert added && fa2.size() == 5;
        added=fa.addAll(fa2);
        assert added;
        assert fa.size() == 15;
        assert fa.capacity() == fa.size() + fa.increment();

        fa=new FastArray<>(15);
        added=fa.addAll(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        assert added;
        fa2=new FastArray<>(10);
        added=fa2.addAll(Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
        assert added;
        added=fa.addAll(fa2, false);
        assert added;
        assert fa.size() == 15;
        assert fa.capacity() == fa.size();

        fa=new FastArray<>(20);
        added=fa.addAll(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        assert added;
        fa2=new FastArray<>(10);
        added=fa2.addAll(Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
        assert added;
        added=fa.addAll(fa2, false);
        assert added;
        assert fa.size() == 20;
        assert fa.capacity() == fa.size();
    }

    public void testAddArray() {
        FastArray<Integer> fa=new FastArray<>(5);
        Integer[] arr=createArray(5);
        boolean added=fa.addAll(arr, 10);
        assert added && fa.size() == arr.length;
        assert fa.size() == 5;
        assert fa.index() == 5;

        fa=new FastArray<>(5);
        arr=createArray(10);
        added=fa.addAll(arr, 8);
        assert added;
        assert fa.size() == 8;
        assert fa.index() == 8;

        // add sparse array
        fa=new FastArray<>(5);
        arr[0]=arr[3]=arr[9]=null;
        added=fa.addAll(arr, arr.length);
        assert added;
        assert fa.size() == 7;
        assert fa.index() == 10;
    }

    public void testTransfer() {
        FastArray<Integer> other=new FastArray<>(3);
        List<Integer> l=Arrays.asList(0,1,2,3,4,5,6,7,8,9);
        other.addAll(l);
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
        other.addAll(l);
        other.remove(0);
        other.remove(3);
        other.remove(4);
        other.remove(9);
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

    @SuppressWarnings("UseBulkOperation")
    public void testTransfer3() {
        FastArray<Integer> other=new FastArray<>(3);
        List<Integer> l=Arrays.asList(0,1,2,3,4,5,6,7,8,9);
        other.addAll(l);
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

    public void testTransfer5() {
        FastArray<Integer> other=new FastArray<>(30);
        other.addAll(List.of(1,2,3,4,5)); // size if 5 but capacity is 30
        FastArray<Integer> fa=create(10);
        int old_capacity=fa.capacity();
        int num=fa.transferFrom(other, true);
        assert num == 5;
        assert fa.capacity() == old_capacity;
    }

    public void testTransfer6() {
        FastArray<Integer> other=create(3);
        FastArray<Integer> fa=new FastArray<>(3);
        int num=fa.transferFrom(other, false);
        assert num == 3;
        other.add(3);
        num=fa.transferFrom(other, false);
        assert num == 4;
        assert fa.size() == 4 && fa.capacity() == 4;
        other.add(4); other.add(5);
        num=fa.transferFrom(other, false);
        assert num == 6;
        assert fa.size() == 6;
        other.clear();
        other.addAll(0,1,2);
        num=fa.transferFrom(other, false);
        assert num == 3 && fa.size() == 3;
        Integer[] actual=fa.toArray(new Integer[0]), expected=other.toArray(new Integer[0]);
        assert Arrays.equals(actual, expected);
    }

    public void testContains() {
        FastArray<Integer> fa=create(10);
        assert fa.contains(5);
        assert !fa.contains(-1);
        assert !fa.contains(11);
        assert !fa.contains(null);
        fa.set(3, null);
        assert fa.contains(null);
        assert fa.contains(2) && fa.contains(5) && !fa.contains(15);
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    public void testContainsAll() {
        FastArray<Integer> fa=create(10);
        boolean success=fa.containsAll(List.of(1, 2, 3));
        assert success;
        success=fa.containsAll(new ArrayList<>());
        success=fa.containsAll(List.of(1,6,11));
        assert !success;
        fa.set(3, null);
        success=fa.containsAll(List.of(1, 2, 5));
        assert success;
        List<Integer> list=new ArrayList<>();
        list.add(1); list.add(3); list.add(null);
        success=fa.containsAll(list);
    }

    @SuppressWarnings("ListIndexOfReplaceableByContains")
    public void testIndexOf() {
        FastArray<Integer> fa=create(10);
        assert fa.indexOf(5) == 5;
        assert fa.indexOf(-1) == -1;
        assert fa.indexOf(11) == -1;
        assert fa.indexOf(null) == -1;
        fa.set(3, null);
        assert fa.indexOf(null) == 3;
        assert fa.indexOf(2) == 2;
        assert fa.indexOf(5) == 5;
        assert fa.indexOf(15) == -1;
    }

    public void testLastIndexOf() {
        FastArray<Integer> fa=create(10);
        fa.addAll(1,2,3);
        int index=fa.lastIndexOf(15);
        assert index == -1;
        index=fa.lastIndexOf(4);
        assert index == 4;
        index=fa.lastIndexOf(3);
        assert index == 12; // also at index==3
        index=fa.lastIndexOf(1);
        assert index == 10;
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
        fa.set(9, 90);
        fa.set(8, 80);
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
        Integer prev=fa.set(9, null);
        assert prev == 9;
        assert fa.size() == 1;
        prev=fa.set(9, 90);
        assert prev == null;
        assert fa.size() == 2;
    }

    public void testRemove() {
        FastArray<Integer> fa=create(10);
        Integer prev=fa.remove(5);
        assert prev == 5;
        prev=fa.remove(0);
        assert prev == 0;
        prev=fa.remove(9);
        assert prev == 9;
        assert fa.size() == 7;
        assert fa.count() == 7;
        prev=fa.remove(9);
        assert prev == null;
    }

    public void testRemove2() {
        FastArray<Integer> fa=new FastArray<>(10);
        fa.remove(9);
        assert fa.isEmpty();
    }

    public void testRemove3() {
        FastArray<Integer> fa=create(10);
        boolean removed=fa.remove((Integer)5);
        assert removed;
        removed=fa.remove((Integer)5);
        assert !removed;
        removed=fa.remove(null);
        assert removed;
        removed=fa.remove((Object)11);
        assert !removed;
    }

    public void testRemoveAll() {
        List<Integer> fa=create(10);
        boolean removed=fa.removeAll(List.of(11, 12, 13));
        assert !removed && fa.size() == 10;
        removed=fa.removeAll(List.of(1,2,10));
        assert removed && fa.size() == 8;
    }

    public void testRetainAll() {
        List<Integer> fa=create(10);
        boolean changed=fa.retainAll(List.of(11));
        assert changed && fa.isEmpty();
        fa=create(10);
        changed=fa.retainAll(List.of(1,3,5,7,9));
        assert changed && fa.size() == 5;
        for(int i: List.of(1,3,5,7,9))
            assert fa.contains(i);
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
        FastArray<Integer> fa=create(10);
        Stream.of(0, 1, 5, 6, 7, 8, 9).forEach(idx -> fa.remove((int)idx));
        assert fa.size() == 3;
        fa.replaceIf(el -> el %2 == 0, null, true);
        assert fa.size() == 1;
        for(Integer n: fa)
            assert n % 2 != 0;
    }

    public void testGet() {
        FastArray<Integer> fa=create(10);
        fa.remove(5);
        fa.remove(0);
        fa.remove(9);
        Integer num=fa.get(3);
        assert num == 3;

        num=fa.get(9);
        assert num == null;
    }

    public void testResize() {
        FastArray<Integer> fa=create(2).increment(4);
        int old_cap=fa.capacity();
        fa.add(3);
        assert fa.capacity() == old_cap + 1 + fa.increment();
    }

    public void testResize2() {
        FastArray<Integer> fa=create(128);
        assert fa.capacity() == 128;
        fa.add(128);
        int new_capacity=fa.capacity() + fa.capacity() >> 1;
        assert fa.capacity() == new_capacity;
        IntStream.rangeClosed(128,192).forEach(fa::add);
        new_capacity=fa.capacity() + fa.capacity() >> 1;
        assert fa.capacity() == new_capacity;
        IntStream.rangeClosed(129,288).forEach(fa::add);
        new_capacity=fa.capacity() + fa.capacity() >> 1;
        assert fa.capacity() == new_capacity;
    }

    public void testTrimTo() {
        FastArray<Integer> fa=create(16).increment(10);
        fa.trimTo(10); // no-op
        assert fa.capacity() == 16;
        fa.trimTo(16); // no-op
        assert fa.capacity() == 16;
        fa.trimTo(32); // no-op
        assert fa.capacity() == 16;
        fa.add(16);
        assert fa.capacity() >= 26;
        fa.trimTo(20);
        assert fa.capacity() == 20;
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
        for(Iterator<Integer> it=fa.iterator(i -> i %2 == 0); it.hasNext();) {
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
                fa.addAll(10,11);
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
        assert it.cursor() == 6;
        assert it.hitCount() == 2;
    }

    public void testStopIterationWhenSizeReachedUsingFilter() {
        FastArray<Integer> fa=create(10);
        IntStream.rangeClosed(0,9).filter(i -> i != 3 && i != 4 && i != 5).forEach(fa::remove);
        assert fa.size() == 3;
        int count=0;
        FastArray<Integer>.FastIterator it=fa.iterator(el -> el < 3 || el > 5);
        while(it.hasNext()) {
            Integer el=it.next();
            System.out.println("el = " + el);
            count++;
        }
        assert count == 0;
        assert it.cursor() == 6;
        assert it.hitCount() == 3;

        fa=create(10);
        IntStream.rangeClosed(0,9).filter(i -> i != 3 && i != 4 && i != 5).forEach(fa::remove);
        assert fa.size() == 3;
        count=0;
        it=fa.iterator(el -> el != 4);
        while(it.hasNext()) {
            Integer el=it.next();
            System.out.println("el = " + el);
            count++;
        }
        assert count == 2;
        assert it.cursor() == 6;
        assert it.hitCount() == 3;
    }

    public void testListIterator() {
        List<Integer> list=IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
        List<Integer> fa=create(10);
        ListIterator<Integer> it=fa.listIterator();
        ListIterator<Integer> it2=list.listIterator();
        assert !it.hasPrevious();
        assert !it2.hasPrevious();

        assert it.previousIndex() == -1;
        assert it2.previousIndex() == -1;

        assert it.hasNext();
        assert it2.hasNext();

        assert it.nextIndex() == 0;
        assert it2.nextIndex() == 0;

        Integer el=it.next(), el2=it2.next();
        assert el == 0;
        assert el2 == 0;

        el=it.next();
        el2=it2.next();
        assert el == 1;
        assert el2 == 1;

        assert it.hasPrevious();
        assert it2.hasPrevious();

        assert it.previousIndex() == 1;
        assert it2.previousIndex() == 1;

        assert it.hasNext();
        assert it2.hasNext();

        assert it.nextIndex() == 2;
        assert it2.nextIndex() == 2;

        while(it.hasNext() && it2.hasNext()) {
            el=it.next();
            el2=it2.next();
            assert el.equals(el2);
        }
        assert !it.hasNext();
        assert !it2.hasNext();

        assert it.nextIndex() == 10;
        assert it2.nextIndex() == 10;

        assert it.hasPrevious();
        assert it2.hasPrevious();

        try {
            it.next();
        }
        catch(NoSuchElementException ex) {
            System.out.printf("caught exception as expected: %s\n", ex);
        }

        try {
            it2.next();
        }
        catch(NoSuchElementException ex) {
            System.out.printf("caught exception as expected: %s\n", ex);
        }

        assert it.previousIndex() == 9;
        assert it2.previousIndex() == 9;

        el=it.previous();
        el2=it2.previous();
        assert el == 9;
        assert el2 == 9;

        while(it.hasPrevious() && it2.hasPrevious()) {
            el=it.previous();
            el2=it2.previous();
            assert el.equals(el2);
        }
        assert !it.hasPrevious();
        assert !it2.hasPrevious();

        el=it.next();
        el2=it2.next();
        assert el.equals(el2);
    }

    public void testListIteratorSet() {
        List<Integer> list=IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
        List<Integer> fa=create(10);
        ListIterator<Integer> it=fa.listIterator();
        ListIterator<Integer> it2=list.listIterator();
        for(int i=0; i < fa.size(); i++) {
            it.next();
            it2.next();
            if(i % 2 == 0) {
                it.set(0);
                it2.set(0);
            }
        }
        Integer[] arr1=fa.toArray(new Integer[0]), arr2=list.toArray(new Integer[0]);
        assert Arrays.equals(arr1, arr2);
        for(int n: fa)
            assert n == 0 || n % 2 != 0;
        for(int n: list)
            assert n == 0 || n % 2 != 0;
    }

    public void testListIteratorAdd() {
        List<Integer> list=new ArrayList<>();
        List<Integer> fa=new FastArray<>(10);
        ListIterator<Integer> it=fa.listIterator();
        ListIterator<Integer> it2=list.listIterator();
        it.add(1);
        it2.add(1);
        assert !it.hasNext();
        assert !it2.hasNext();
        assert it.hasPrevious();
        assert it2.hasPrevious();
        Integer el=it.previous();
        Integer el2=it2.previous();
        assert el.equals(el2);
        assert el == 1;

        while(it.hasNext() && it2.hasNext()) {
            it.next(); it2.next();
        }
        it.add(2); it.add(3);
        it2.add(2); it2.add(3);
        Integer[] expected=IntStream.rangeClosed(1, 3).boxed().toArray(Integer[]::new);
        Integer[] actual1=fa.toArray(new Integer[0]);
        Integer[] actual2=list.toArray(new Integer[0]);
        assert Arrays.equals(expected, actual1);
        assert Arrays.equals(expected, actual2);
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
        fa.remove(0);
        fa.remove(1);
        fa.remove(9);
        fa.remove(10);
        assert fa.size() == 7;
        fa.clear(true);
        assert fa.isEmpty();
    }

    public void testAnyMatch() {
        FastArray<Integer> fa=create(10);
        boolean match=fa.anyMatch(num -> num > 3);
        assert match;
    }

    public void testToArray() {
        List<Integer> fa=create(10);
        Object[] arr1=fa.toArray();
        assert arr1.length == 10;
        for(int i=0; i < arr1.length; i++)
            assert arr1[i].equals(i);
        Integer[] arr2=fa.toArray(new Integer[0]);
        assert arr2.length == 10;
        for(int i=0; i < arr2.length; i++) {
            assert arr2[i].equals(i);
            assert arr2[i].getClass().equals(Integer.class);
        }
        fa.remove((Object)3);
        fa.remove((Object)7);
        List<Integer> expected=IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
        expected.set(3, null);
        expected.set(7,null);

        arr1=fa.toArray();
        assert arr1.length == expected.size();
        Iterator<Integer> it=expected.iterator();
        for(Object obj: arr1) {
            Integer exp=it.next();
            assert obj == exp || obj.equals(exp);
        }
        arr2=fa.toArray(new Integer[0]);
        assert arr2.length == 10;
        it=expected.iterator();
        for(int i=0; i < arr2.length; i++) {
            Integer exp=it.next();
            assert (arr2[i] == null && exp == null) || arr2[i].equals(exp);
            assert arr2[i] == null || exp == null || arr2[i].getClass().equals(Integer.class);
        }
    }

    public void testToArrayWithNullValues() {
        List<Integer> fa=create(5);
        fa.set(2, null);
        List<Integer> expected=new ArrayList<>(List.of(0,1,2,3,4));
        expected.set(2, null);
        Integer[] actual=fa.toArray(new Integer[0]), expected_arr=expected.toArray(new Integer[0]);
        assert Arrays.equals(actual, expected_arr) :
          String.format("expected: %s, actual: %s", Arrays.toString(expected_arr), Arrays.toString(actual));
    }


    protected static void assertSameElements(List<Integer> actual, List<Integer> expected) {
        assert actual.size() == expected.size() : String.format("expected: %s, actual: %s", expected, actual);
        List<Integer> l=actual.stream().collect(Collectors.toList());
        assert l.equals(expected) : String.format("expected: %s, actual: %s", expected, actual);
    }


    protected static void testStream(FastArray<Integer> fa) {
        //noinspection SimplifyStreamApiCallChains
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
