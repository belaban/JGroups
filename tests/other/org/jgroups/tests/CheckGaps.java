package org.jgroups.tests;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Set;
import java.util.HashSet;

/** Reads a sequence of numbers from stdin and verifies that they are in order
 * @author Bela Ban
 */
public class CheckGaps {


    public static void main(String[] args) {
        SortedSet<Integer> set=new TreeSet<>();

        for(int i=0; i < args.length; i++) {
            set.add(Integer.parseInt(args[i]));
        }

        int low=set.first(), high=set.last();
        System.out.println("input has " + set.size() + " numbers, low=" + low + ", high=" + high);

        Set<Integer> correct_set=new HashSet<>();
        for(int i=low; i < high; i++) {
            correct_set.add(i);
        }

        correct_set.removeAll(set);
        System.out.println("missing seqnos: " + correct_set);
    }
}
