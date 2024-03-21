package org.jgroups.tests;

import org.jgroups.util.SeqnoList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/** Reads a sequence of numbers from stdin and verifies that they are in order
 * @author Bela Ban
 */
public class CheckGaps {


    public static void main(String[] args) throws IOException {
        List<Long> list=new LinkedList<>();
        Set<Long>  set=new ConcurrentSkipListSet<>();
        final AtomicLong high=new AtomicLong();
        if(args.length > 1 && args[0].equals("-f")) {
            Files.lines(Path.of(args[1])).forEach(line -> {
                line=line.replace(",", "").replace("[", "").replace("]", "");
                String[] numbers=line.split("\\s+");
                for(String s: numbers) {
                    s=s.trim();
                    if(!s.isEmpty()) {
                        long number=Long.parseLong(s);
                        list.add(number);
                        set.add(number);
                        high.set(Math.max(high.get(), number));
                    }
                }
            });
        }
        else {
            for(int i=0; i < args.length; i++) {
                long num=Long.parseLong(args[i]);
                list.add(num);
                set.add(num);
                high.set(Math.max(high.get(), num));
            }
        }

        int size=list.size(), set_size=set.size();
        long low=set.iterator().next(), duplicates=size - set_size;
        for(long l: set)
            list.remove(l);


        System.out.printf("read %d numbers (%d duplicates), low=%d, high=%d\n",
                          size, duplicates, low, high.get());
        if(!list.isEmpty()) {
            SortedSet<Long> tmp=new ConcurrentSkipListSet<>(list);
            long first=tmp.iterator().next();
            SeqnoList sl=new SeqnoList(size, first);
            sl.add(tmp);
            System.out.printf("duplicates: %s\n", sl);
        }

        Set<Long> correct_set=new HashSet<>();
        for(long i=low; i < high.get(); i++)
            correct_set.add(i);

        set.forEach(correct_set::remove);
        System.out.printf("missing seqnos: %s\n", correct_set);
    }
}
