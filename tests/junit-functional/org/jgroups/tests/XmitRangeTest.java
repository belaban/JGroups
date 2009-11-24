package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.XmitRange;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @version $Id: XmitRangeTest.java,v 1.1 2009/11/24 11:53:39 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class XmitRangeTest {

    public static void testConstructor() {
        XmitRange range=new XmitRange(10, 10);
        System.out.println(print(range));
        assert range.size() == 1;
        assert range.getLow() == 10;
        assert range.getHigh() == 10;
        assert range.contains(10);
        assert !range.contains(11);

        range=new XmitRange(10, 15);
        System.out.println(print(range));
        assert range.size() == 6;
        assert range.getLow() == 10;
        assert range.getHigh() == 15;
        assert range.contains(10);
        assert range.contains(14);
    }

    public static void testSetAndGet() {
        XmitRange range=new XmitRange(10, 10);
        assert range.getNumberOfMissingMessages() == 1;
        assert range.getNumberOfReceivedMessages() == 0;

    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public static void testSetOfInvalidIndex() {
        XmitRange range=new XmitRange(10, 10);
        range.set(9);
    }


    private static String print(XmitRange range) {
        StringBuilder sb=new StringBuilder();
        sb.append("low=" + range.getLow() + ", high=" + range.getHigh());
        sb.append( ", size= " + range.size());
        sb.append(", received=" + range.printBits(true) + " (" + range.getNumberOfReceivedMessages() + ")");
        sb.append(", missing=" + range.printBits(false) + " (" + range.getNumberOfMissingMessages() + ")");
        return sb.toString();
    }
}
