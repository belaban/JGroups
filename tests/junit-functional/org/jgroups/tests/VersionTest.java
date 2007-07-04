package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.Version;

/**
 * @author Bela Ban April 4 2003
 * @version $Revision: 1.1 $
 */
public class VersionTest extends TestCase {

    public VersionTest(String s) {
        super(s);
    }

    public void testVersionPrint() {
        System.out.println("version is " +Version.printVersion());
        assertTrue(true);
    }

    public void testNullVersion() {
        assertFalse(Version.isSame((short)0));
    }

    public void testDifferentLengthVersion1() {
        short version2=Version.encode(2,0,7);
        assertFalse(Version.isSame(version2));
    }


    public void testDifferentVersion() {
        short version1=Version.encode(2,0,7), version2=Version.encode(2,0,6);
        assertFalse(version1 == version2);
    }

    public void testSameVersion() {
        assertTrue(match(0,0,1, 0,0,1));
        assertTrue(match(1,0,0, 1,0,0));
        assertTrue(match(10,2,60, 10,2,60));
        assertFalse(match(1,2,3, 1,2,0));
        assertFalse(match(0,0,0, 0,0,1));
        assertFalse(match(2,5,0, 2,5,1));
    }


    public void testBinaryCompatibility() {
        assertTrue(isBinaryCompatible(0,0,0, 0,0,0));
        assertTrue(isBinaryCompatible(1,2,0, 1,2,1));
        assertTrue(isBinaryCompatible(1,2,0, 1,2,60));
        assertFalse(isBinaryCompatible(2,5,0, 2,4,1));
        assertFalse(isBinaryCompatible(2,5,0, 2,6,0));
    }


    private boolean match(int major_1, int minor_1, int micro_1, int major_2, int minor_2, int micro_2) {
        short version1=Version.encode(major_1, minor_1, micro_1);
        short version2=Version.encode(major_2, minor_2, micro_2);
        return version1 == version2;
    }

    private boolean isBinaryCompatible(int major_1, int minor_1, int micro_1, int major_2, int minor_2, int micro_2) {
        short version1=Version.encode(major_1, minor_1, micro_1);
        short version2=Version.encode(major_2, minor_2, micro_2);
        boolean retval=Version.isBinaryCompatible(version1, version2);
        System.out.println(Version.print(version1) + " binary compatibel to " + Version.print(version2) + (retval? " OK" : " FAIL"));
        return Version.isBinaryCompatible(version1, version2);
    }


    public static void main(String[] args) {
        String[] testCaseName = {VersionTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
