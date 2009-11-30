package org.jgroups.tests;

import org.testng.annotations.Test;
import org.jgroups.Global;
import org.jgroups.util.SeqnoComparator;
import org.jgroups.util.Seqno;
import org.jgroups.util.SeqnoRange;

/**
 * @author Bela Ban
 * @version $Id: SeqnoComparatorTest.java,v 1.1 2009/11/30 11:40:36 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class SeqnoComparatorTest {

    static final SeqnoComparator comp=new SeqnoComparator();


    public static void testTwoSeqnos() {
        Seqno s1=new Seqno(10), s2=new Seqno(10);
        assert comp.compare(s1, s2) == 0;
        s1=new Seqno(9);
        assert comp.compare(s1, s2) == -1;
        s2=new Seqno(5);
        assert comp.compare(s1, s2) == 1;
    }


    public static void compareDummyWithSeqnoRange() {
        Seqno s1=new Seqno(10, true), s2=new SeqnoRange(1, 100);
        assert comp.compare(s1, s2) == 0;
        s1=new Seqno(1, true);
        assert comp.compare(s1, s2) == 0;
        s1=new Seqno(100, true);
        assert comp.compare(s1, s2) == 0;

        s1=new Seqno(0, true);
        assert comp.compare(s1, s2) == -1;

        s1=new Seqno(101, true);
        assert comp.compare(s1, s2) == 1;
    }

    public static void compareDummyWithSeqno() {
        Seqno s1=new Seqno(10, true), s2=new Seqno(10);
        assert comp.compare(s1, s2) == 0;

        s1=new Seqno(9, true);
        assert comp.compare(s1, s2) == -1;
        s1=new Seqno(11, true);
        assert comp.compare(s1, s2) == 1;
    }

    public static void compareSeqnoRangeWithDummy() {
        Seqno s1=new SeqnoRange(1, 100), s2=new Seqno(10, true);
        assert comp.compare(s1, s2) == 0;
        s2=new Seqno(1, true);
        assert comp.compare(s1, s2) == 0;
        s2=new Seqno(100, true);
        assert comp.compare(s1, s2) == 0;

        s2=new Seqno(0, true);
        assert comp.compare(s1, s2) == 1;

        s2=new Seqno(101, true);
        assert comp.compare(s1, s2) == -1;
    }


    public static void compareSeqnoWithDummy() {
        Seqno s1=new Seqno(10), s2=new Seqno(10, true);
        assert comp.compare(s1, s2) == 0;

        s2=new Seqno(9, true);
        assert comp.compare(s1, s2) == 1;
        s2=new Seqno(11, true);
        assert comp.compare(s1, s2) == -1;
    }
}
