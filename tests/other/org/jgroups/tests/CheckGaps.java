package org.jgroups.tests;

import org.jgroups.util.SeqnoList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/** Reads a sequence of numbers from stdin and verifies that they are in order
 * @author Bela Ban
 */
public class CheckGaps {


    public static void main(String[] args) throws IOException {
        List<Long> list=new LinkedList<>();
        Set<Long>  set=new ConcurrentSkipListSet<>();
        if(args.length > 1 && args[0].equals("-f")) {
            Files.lines(Path.of(args[1])).forEach(line -> {
                line=line.replace(",", "");
                String[] numbers=line.split("\\s+");
                for(String s: numbers) {
                    s=s.trim();
                    if(!s.isEmpty()) {
                        long number=Long.parseLong(s);
                        list.add(number);
                        set.add(number);
                    }
                }
            });
        }
        else {
            for(int i=0; i < args.length; i++) {
                long num=Long.parseLong(args[i]);
                list.add(num);
                set.add(num);
            }
        }

        int size=list.size(), set_size=set.size();
        long low=list.get(0), high=list.get(size-1), duplicates=size - set_size;
        // list.removeAll(set);
        for(long l: set)
            list.remove(l);


        System.out.printf("read %d numbers (%d duplicates), low=%d, high=%d\n",
                          size, duplicates, low, high);
        if(!list.isEmpty()) {
            SortedSet<Long> tmp=new ConcurrentSkipListSet<>(list);
            long first=tmp.iterator().next();
            SeqnoList sl=new SeqnoList(size, first);
            sl.add(tmp);
            System.out.printf("duplicates: %s\n", sl);
        }

        Set<Long> correct_set=new HashSet<>();
        for(long i=low; i < high; i++)
            correct_set.add(i);

        set.forEach(correct_set::remove);
        System.out.printf("missing seqnos: %s\n", correct_set);
    }
}
