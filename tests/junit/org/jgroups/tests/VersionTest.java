package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.Version;

/**
 * @author Bela Ban April 4 2003
 * @version $Revision: 1.2 $
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
        assertTrue(Version.compareTo((short)0) == false);
    }

    public void testDifferentLengthVersion1() {
        short version2=207;
        assertTrue(Version.compareTo(version2) == false);
    }

    public void testDifferentLengthVersion2() {
        short version2=2071;
        assertTrue(Version.compareTo(version2) == false);
    }

    public void testDifferentVersion() {
        short version2=206;
        assertTrue(Version.compareTo(version2) == false);
    }


    public static void main(String[] args) {
        String[] testCaseName = {VersionTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
