package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.Version;

/**
 * @author Bela Ban April 4 2003
 * @version $Revision: 1.1 $
 */
public class VersionTest extends TestCase {
    byte[] version1={'0', '2', '0', '7'};

    public VersionTest(String s) {
        super(s);
    }

    protected void setUp() throws Exception {
        Version.setVersion(version1);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testVersionPrint() {
        System.out.println("version is " +Version.printVersionId(Version.version_id));
        assertTrue(true);
    }

    public void testSameVersion() {
        byte[] version2={'0', '2', '0', '7'};
        assertTrue(Version.compareTo(version2));
    }

    public void testNullVersion() {
        assertTrue(Version.compareTo(null) == false);
    }

    public void testDifferentLengthVersion1() {
        byte[] version2={'2', '0', '7'};
        assertTrue(Version.compareTo(version2) == false);
    }

    public void testDifferentLengthVersion2() {
        byte[] version2={'0', '2', '0', '7', '1'};
        assertTrue(Version.compareTo(version2) == true);
    }

    public void testDifferentVersion() {
        byte[] version2={'0', '2', '0', '6'};
        assertTrue(Version.compareTo(version2) == false);
    }


    public static void main(String[] args) {
        String[] testCaseName = {VersionTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
