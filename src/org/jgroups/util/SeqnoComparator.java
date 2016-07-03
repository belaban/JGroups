package org.jgroups.util;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author Bela Ban
 */
public class SeqnoComparator implements Comparator<Seqno>, Serializable {
    private static final long serialVersionUID = 1L;

    public int compare(Seqno o1, Seqno o2) {

        // o1 and o2 are either Seqnos or SeqnoRanges, so we just compare on 'low'
        if(!o1.isDummy() && !o2.isDummy())
            return o1.low == o2.low? 0 : o1.low < o2.low ? -1 : 1;

        // o2 must be a seqno or SeqnoRange; o1 must be a Seqno
        if(o1.isDummy()) {
            if(o2 instanceof SeqnoRange)
                return _compare2(o1, (SeqnoRange)o2);
            return _compare(o1, o2);
        }

        // o2 is dummy
        if(o1 instanceof SeqnoRange)
            return _compare3((SeqnoRange)o1, o2);
        return _compare(o1, o2);
    }

    private static int _compare(Seqno o1, Seqno o2) {
        return o1.low == o2.low? 0 : o1.low < o2.low? -1 : 1;
    }

    private static int _compare2(Seqno o1, SeqnoRange o2) {
        return o1.low >= o2.low && o1.low <= o2.high? 0 : o1.low < o2.low? -1 : 1;
    }

    private static int _compare3(SeqnoRange o1, Seqno o2) {
        return o2.low >= o1.low && o2.low <= o1.high? 0 : o1.low < o2.low ? -1 : 1;
    }
}
